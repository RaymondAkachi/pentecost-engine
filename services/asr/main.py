import asyncio
import os
import signal
import time
import json
import base64
import numpy as np
import nats
import logging
from faster_whisper import WhisperModel

# ==============================================================================
# CONFIGURATION & LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("ASR_ENGINE")

NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
INPUT_SUBJECT = os.getenv("INPUT_SUBJECT", "livestream.audio.raw")
OUTPUT_SUBJECT = os.getenv("OUTPUT_SUBJECT", "livestream.transcription.raw")

MODEL_SIZE = os.getenv("MODEL_SIZE", "tiny") 
DEVICE = os.getenv("DEVICE", "cuda" if os.environ.get("CUDA_VISIBLE_DEVICES") else "cpu")
COMPUTE_TYPE = os.getenv("COMPUTE_TYPE", "float16" if DEVICE == "cuda" else "int8")

class ASREngine:
    def __init__(self):
        logger.info(f"🧠 Loading Whisper ({MODEL_SIZE}) on {DEVICE} ({COMPUTE_TYPE})...")
        self.model = WhisperModel(
            MODEL_SIZE, 
            device=DEVICE, 
            compute_type=COMPUTE_TYPE,
            cpu_threads=8 
        )
        logger.info("✅ Model Loaded & Ready.")

class ASRService:
    def __init__(self):
        self.engine = ASREngine()
        self.audio_buffer = np.array([], dtype=np.float32)
        self.buffer_start_pts = 0
        
        # Max buffer before forced flush. 
        self.max_buffer_seconds = 28.0 

    def process_packet(self, chunk_16k: np.ndarray, pts: int):
        # 1. Accumulate Audio
        if len(self.audio_buffer) == 0:
            self.buffer_start_pts = pts

        self.audio_buffer = np.concatenate((self.audio_buffer, chunk_16k))
        buffer_duration = len(self.audio_buffer) / 16000.0

        if buffer_duration < 4.0:
            return None

        start_infer = time.time()
        
        # 2. High-Fidelity Transcription
        segments, info = self.engine.model.transcribe(
            self.audio_buffer, 
            beam_size=5, 
            language="en", 
            condition_on_previous_text=True, 
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=400), # Tightened to ignore standard breaths
            word_timestamps=True 
        )
        
        all_words = []
        for segment in segments:
            all_words.extend(segment.words)
        
        if not all_words:
            if buffer_duration > self.max_buffer_seconds:
                self.audio_buffer = np.array([], dtype=np.float32)
            return None

        force_flush = buffer_duration >= self.max_buffer_seconds
        split_idx = -1
        
        # 3. The Acoustic-Semantic Hybrid Slicer
        if force_flush:
            # Emergency release to prevent buffer explosion
            split_idx = len(all_words) - 2 if len(all_words) > 1 else 0
        else:
            # Iterate backwards looking for true sentence boundaries
            for i in range(len(all_words) - 1, -1, -1):
                word_text = all_words[i].word.strip()
                
                # Check for terminal punctuation
                if word_text and word_text[-1] in ".!?":
                    
                    # If it's the very last word generated, we hold it to ensure the thought is actually finished
                    if i == len(all_words) - 1:
                        continue
                        
                    # Calculate the acoustic gap between this word and the next word
                    next_word = all_words[i+1]
                    current_word = all_words[i]
                    acoustic_gap = next_word.start - current_word.end
                    
                    # TRUE BOUNDARY: Punctuation AND a measurable physical pause (> 0.3s)
                    if acoustic_gap > 0.3:
                        split_idx = i
                        break

        # If no true boundary is found, wait for more audio
        if split_idx == -1:
            logger.info(f"⏳ Listening for true acoustic boundary... (Buffer: {buffer_duration:.1f}s)")
            return None

        # 4. Commit the Grammatically Complete Text
        committed_words = all_words[:split_idx + 1]
        commit_text = "".join([w.word for w in committed_words]).strip()
        cut_time = committed_words[-1].end
        out_pts = self.buffer_start_pts
        
        # 5. The Micro-Slice
        cut_samples = int(cut_time * 16000)
        self.audio_buffer = self.audio_buffer[cut_samples:]
        self.buffer_start_pts += int(cut_time * 1000)
        
        infer_time = time.time() - start_infer
        logger.info(f"⚡ Infer: {infer_time:.2f}s | Cut at: {cut_time:.2f}s | Retaining: {len(self.audio_buffer)/16000:.2f}s")
        logger.info(f"📝 COMMITTED: \"{commit_text}\"")
        
        return {"text": commit_text, "source_pts": out_pts}

# ==============================================================================
# NATS MESSAGE BUS INTEGRATION
# ==============================================================================
async def run():
    nc = None
    while nc is None:
        try:
            nc = await nats.connect(NATS_URL)
            logger.info("✅ Connected to NATS")
        except:
            logger.warning("⏳ Waiting for NATS...")
            await asyncio.sleep(2)
            
    js = nc.jetstream()
    
    try: 
        await js.add_stream(name="LIVESTREAM_TRANSCRIPTION", subjects=[OUTPUT_SUBJECT, "livestream.transcription.enriched"])
    except: pass
    
    service = ASRService()
    loop = asyncio.get_running_loop()
    
    async def msg_handler(msg):
        try:
            payload = json.loads(msg.data.decode('utf-8'))
            
            chunk_id = payload.get("chunk_id", "")
            if not chunk_id.startswith("a_"):
                await msg.ack()
                return

            pts = payload.get("pts", 0)
            audio_b64 = payload.get("data", "")
            
            if not audio_b64:
                await msg.ack()
                return

            raw_bytes = base64.b64decode(audio_b64)
            audio_np = np.frombuffer(raw_bytes, dtype=np.float32)

            result = await loop.run_in_executor(None, service.process_packet, audio_np, pts)
            
            if result:
                out_payload = json.dumps({
                    "text": result["text"], 
                    "source_pts": result["source_pts"]
                })
                await js.publish(OUTPUT_SUBJECT, out_payload.encode("utf-8"), headers={"pts": str(result["source_pts"])})

        except Exception as e:
            logger.error(f"❌ Processing Error: {str(e)}")
        finally:
            await msg.ack()
            
    await js.subscribe(INPUT_SUBJECT, cb=msg_handler)
    logger.info(f"🎧 Listening on {INPUT_SUBJECT}")
    
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, lambda: stop.set_result(None))
    await stop
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())