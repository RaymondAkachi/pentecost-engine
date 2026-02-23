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

# Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("ASR")

# Configuration
NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
INPUT_SUBJECT = os.getenv("INPUT_SUBJECT", "livestream.audio.raw")
OUTPUT_SUBJECT = os.getenv("OUTPUT_SUBJECT", "livestream.transcription.raw")

# Model Settings
MODEL_SIZE = os.getenv("MODEL_SIZE", "tiny") 
COMPUTE_TYPE = os.getenv("COMPUTE_TYPE", "int8") 
DEVICE = os.getenv("DEVICE", "cpu") 

class ASREngine:
    def __init__(self):
        logger.info(f"🧠 Loading Whisper ({MODEL_SIZE}) on {DEVICE}...")
        self.model = WhisperModel(
            MODEL_SIZE, 
            device=DEVICE, 
            compute_type=COMPUTE_TYPE,
            cpu_threads=4 
        )
        logger.info("✅ Model Loaded & Ready.")

    def transcribe(self, audio_np):
        start_time = time.time()
        
        # Run Inference
        segments, info = self.model.transcribe(
            audio_np, 
            beam_size=5, 
            language="en", 
            condition_on_previous_text=False,
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=200) 
        )
        
        segment_list = list(segments)
        text = " ".join([s.text for s in segment_list]).strip()
        
        duration = time.time() - start_time
        if text:
            logger.info(f"⚡ Inference took {duration:.2f}s | VAD Audio Retained: {info.duration_after_vad:.2f}s")
        
        return text

class ASRService:
    def __init__(self):
        self.engine = ASREngine()
        self.buffer = np.array([], dtype=np.float32)
        self.buffer_limit = 16000 * 5 # 5 Seconds @ 16kHz
        self.chunk_count = 0

    # FIX: Now accepts a decoded numpy array directly
    def process_packet(self, chunk_16k: np.ndarray, pts: int):
        self.buffer = np.concatenate((self.buffer, chunk_16k))
        self.chunk_count += 1
        
        if self.chunk_count % 100 == 0:
            fill_pct = (len(self.buffer) / self.buffer_limit) * 100
            logger.info(f"🌊 Buffer Filling: {fill_pct:.1f}% ({len(self.buffer)/16000:.1f}s)")

        if len(self.buffer) >= self.buffer_limit:
            text = self.engine.transcribe(self.buffer)
            self.buffer = np.array([], dtype=np.float32)
            
            if text:
                logger.info(f"📝 DETECTED: \"{text}\"")
                return text
                
        return None

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
            # FIX: Parse JSON payload from Go Engine
            payload = json.loads(msg.data.decode('utf-8'))
            
            # Defensive check: Ensure it's an audio chunk
            chunk_id = payload.get("chunk_id", "")
            if not chunk_id.startswith("a_"):
                return

            pts = payload.get("pts", 0)
            audio_b64 = payload.get("data", "")
            
            if not audio_b64:
                return

            # FIX: Decode Base64 to raw bytes, then to numpy float32
            raw_bytes = base64.b64decode(audio_b64)
            audio_np = np.frombuffer(raw_bytes, dtype=np.float32)

            # Run blocking AI task in thread
            text = await loop.run_in_executor(None, service.process_packet, audio_np, pts)
            
            if text:
                # FIX: Safe JSON serialization to prevent quote-breaking
                out_payload = json.dumps({
                    "text": text, 
                    "source_pts": pts
                })
                await js.publish(OUTPUT_SUBJECT, out_payload.encode("utf-8"), headers={"pts": str(pts)})

        except Exception as e:
            logger.error(f"❌ Processing Error: {str(e)}")
        finally:
            # FIX: Acknowledge message to prevent infinite JetStream loops
            await msg.ack()
            
    await js.subscribe(INPUT_SUBJECT, cb=msg_handler)
    logger.info(f"🎧 Listening on {INPUT_SUBJECT}")
    
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, lambda: stop.set_result(None))
    await stop
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())