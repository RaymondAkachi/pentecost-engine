import asyncio
import os
import torch
import numpy as np
import json
import nats
import logging
import torchaudio
from transformers import WhisperProcessor, WhisperForConditionalGeneration, BitsAndBytesConfig
from peft import PeftModel

# --- CONFIGURATION ---
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
INPUT_SUBJECT = "livestream.audio.denoised"   # From Denoiser (Layer 0)
OUTPUT_SUBJECT = "livestream.transcription.raw" # To RAG (Layer 1.5)
MODEL_NAME = "openai/whisper-large-v3"
ADAPTER_PATH = "/app/adapter" # Path inside Docker container

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PentecostASR")

class PropheticASR:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"🚀 Initializing ASR on {self.device.upper()}...")

        # 1. Load Tokenizer & Processor
        self.processor = WhisperProcessor.from_pretrained(MODEL_NAME)

        # 2. Load Base Model (Dynamic Quantization)
        if self.device == "cuda":
            logger.info("⚡ GPU Detected: Loading in 4-bit (High Efficiency)...")
            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_use_double_quant=True,
            )
            base_model = WhisperForConditionalGeneration.from_pretrained(
                MODEL_NAME, 
                quantization_config=bnb_config, 
                device_map="auto"
            )
        else:
            logger.warning("⚠️ CPU Detected: Loading in Full Precision (Slower)...")
            base_model = WhisperForConditionalGeneration.from_pretrained(MODEL_NAME)
            base_model.to(self.device)

        # 3. Load Your Custom "Pentecost" Adapter
        logger.info(f"🧠 Loading Prophetic Adapter from {ADAPTER_PATH}...")
        try:
            self.model = PeftModel.from_pretrained(base_model, ADAPTER_PATH)
            self.model.eval()
            logger.info("✅ Adapter Loaded Successfully.")
        except Exception as e:
            logger.critical(f"❌ Failed to load adapter: {e}")
            raise e

        # 4. Resampler (48kHz Denoiser -> 16kHz Whisper)
        self.resampler = torchaudio.transforms.Resample(orig_freq=48000, new_freq=16000).to(self.device)

    def transcribe(self, audio_bytes):
        """
        Takes raw 48kHz PCM bytes, resamples, and transcribes.
        """
        try:
            # A. Bytes -> Tensor
            # Denoiser sends float32 chunks at 48kHz
            audio_tensor = torch.from_numpy(
                np.frombuffer(audio_bytes, dtype=np.float32)
            ).to(self.device)

            # B. Resample (48k -> 16k)
            # Whisper requires 16000Hz audio
            audio_16k = self.resampler(audio_tensor)

            # C. Preprocess Input
            input_features = self.processor(
                audio_16k.cpu().numpy(), # Move to CPU for processor
                sampling_rate=16000, 
                return_tensors="pt"
            ).input_features.to(self.device)

            # Cast to fp16 if on GPU
            if self.device == "cuda":
                input_features = input_features.to(torch.float16)

            # D. Generate Text
            with torch.no_grad():
                predicted_ids = self.model.generate(
                    input_features,
                    language="en",
                    max_new_tokens=128 # Limit latency
                )

            # E. Decode
            transcription = self.processor.batch_decode(predicted_ids, skip_special_tokens=True)[0]
            return transcription.strip()

        except Exception as e:
            logger.error(f"Inference Error: {e}")
            return ""

async def run_service():
    logger.info("🔌 Connecting to NATS...")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    
    # Initialize Engine
    asr_engine = PropheticASR()

    # Semaphore to prevent GPU overload (process 1 chunk at a time)
    sem = asyncio.Semaphore(1)

    async def msg_handler(msg):
        async with sem:
            try:
                # 1. Transcribe (Blocking CPU/GPU operation in executor)
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, asr_engine.transcribe, msg.data)
                
                # 2. Publish if we heard something
                if len(text) > 0:
                    logger.info(f"🗣️  ASR: {text}")
                    
                    payload = {
                        "text": text,
                        "source_pts": msg.headers.get("PTS", "0"),
                        "confidence": 0.99, # High trust in fine-tune
                        "model": "Pentecost-v1"
                    }
                    
                    await js.publish(
                        OUTPUT_SUBJECT, 
                        json.dumps(payload).encode(),
                        headers=msg.headers
                    )
            except Exception as e:
                logger.error(f"Handler Error: {e}")

    # Subscribe
    await js.subscribe(INPUT_SUBJECT, cb=msg_handler)
    logger.info(f"👂 Listening on {INPUT_SUBJECT}")
    
    # Keep alive
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(run_service())