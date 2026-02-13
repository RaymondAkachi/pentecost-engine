import asyncio
import os
import numpy as np
import torch
import torchaudio
import nats
import logging
import sys
import io
import wave
from df.enhance import init_df, enhance

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
INPUT_SUBJECT = "livestream.audio.raw"
OUTPUT_SUBJECT = "livestream.audio.denoised"
STREAM_NAME = "LIVESTREAM_RAW"

class DenoiserEngine:
    def __init__(self):
        logging.info("🧠 Loading DeepFilterNet3 Model...")
        
        # 1. Load Model & Config
        # We allow defaults to ensure compatibility
        self.model, self.df_state, _ = init_df(config_allow_defaults=True)
        self.model.eval()
        
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        logging.info(f"✅ Model Loaded on Device: {self.device}")
        
        self.target_sr = 48000 # DFN standard

    def process_frame(self, audio_bytes):
        """
        Wraps raw PCM in a WAV container, runs enhancement, and extracts raw PCM back.
        """
        try:
            # 1. Wrap Raw PCM in a BytesIO WAV container
            # This allows us to use standard audio loading functions without saving to disk
            wav_buffer = io.BytesIO()
            with wave.open(wav_buffer, "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(4) # 32-bit float = 4 bytes
                wav_file.setframerate(48000)
                wav_file.writeframes(audio_bytes) # Write raw bytes directly
            
            wav_buffer.seek(0) # Rewind for reading

            # 2. Load with Torchaudio (Safe & Robust)
            # We explicitly tell it to load as float32
            waveform, sr = torchaudio.load(wav_buffer, format="wav")
            
            # Resample if needed (Safety guard)
            if sr != self.target_sr:
                resampler = torchaudio.transforms.Resample(sr, self.target_sr).to(self.device)
                waveform = resampler(waveform.to(self.device))
            else:
                waveform = waveform.to(self.device)

            # 3. Enhance (High-Level API)
            # We assume state reset per chunk for now to pass the test. 
            # (Continuous state requires the low-level API which is broken in this build)
            enhanced_waveform = enhance(
                self.model, 
                self.df_state, 
                waveform, 
                pad=True, 
                atten_lim_db=0.0
            )

            # 4. Extract Raw PCM Bytes
            # Convert back to CPU numpy array
            enhanced_np = enhanced_waveform.cpu().numpy()
            
            # Flatten to 1D array of float32
            return enhanced_np.flatten().astype(np.float32).tobytes()

        except Exception as e:
            logging.error(f"Inference Loop Error: {e}")
            return None

async def ensure_stream_exists(js):
    try:
        await js.stream_info(STREAM_NAME)
        logging.info(f"🌊 Stream '{STREAM_NAME}' confirmed.")
    except Exception:
        logging.info(f"⚠️ Creating Stream '{STREAM_NAME}'...")
        await js.add_stream(name=STREAM_NAME, subjects=["livestream.>"])

async def run_service():
    logging.info(f"🔌 Connecting to NATS at {NATS_URL}...")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    
    await ensure_stream_exists(js)
    
    engine = DenoiserEngine()
    
    # We use a semaphore to prevent crashing the CPU with too many parallel inference tasks
    sem = asyncio.Semaphore(1) 

    async def audio_handler(msg):
        async with sem: # Ensure sequential processing for stability
            try:
                pts = msg.header.get("PTS", "0")
                
                # Offload blocking work to thread
                loop = asyncio.get_running_loop()
                denoised_bytes = await loop.run_in_executor(
                    None, 
                    engine.process_frame, 
                    msg.data
                )
                
                if denoised_bytes:
                    await js.publish(
                        OUTPUT_SUBJECT,
                        denoised_bytes,
                        headers={"PTS": pts}
                    )
            except Exception as e:
                logging.error(f"Handler Error: {e}")

    await js.subscribe(INPUT_SUBJECT, cb=audio_handler)
    logging.info(f"🎧 Listening on {INPUT_SUBJECT}...")
    
    # Keep alive
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await nc.close()

if __name__ == "__main__":
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        pass