import asyncio
import os
import numpy as np
import onnxruntime as ort
import nats
import logging
import signal
import resampy

# Setup Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [DTLN-DENOISER] - %(levelname)s - %(message)s'
)

# Configuration
NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
INPUT_SUBJECT = "livestream.audio.raw"
OUTPUT_SUBJECT = "livestream.audio.denoised"

# DTLN Constants
DTLN_SR = 16000
BLOCK_SIZE = 512
SHIFT_SIZE = 128

class AudioBuffer:
    """Thread-safe Circular Buffer for Audio Streams."""
    def __init__(self, max_size=64000):
        self._buffer = np.array([], dtype=np.float32)
        self.max_size = max_size

    def write(self, chunk):
        # Clip to safe range before buffering
        chunk = np.clip(chunk, -1.0, 1.0)
        self._buffer = np.concatenate((self._buffer, chunk))
        if len(self._buffer) > self.max_size:
            # Drop oldest data if overflow
            self._buffer = self._buffer[-self.max_size:]

    def peek(self, size):
        if len(self._buffer) >= size:
            return self._buffer[:size]
        return None
    
    def skip(self, size):
        if len(self._buffer) >= size:
            self._buffer = self._buffer[size:]

class DTLNStreamer:
    def __init__(self, model_path="model.onnx"):
        logging.info("🧠 Loading DTLN Model...")
        
        # Load ONNX
        self.sess = ort.InferenceSession(model_path)
        
        # 1. Map Inputs/Outputs Dynamically
        # (Fixes the bug where we guessed the order)
        self.input_names = [x.name for x in self.sess.get_inputs()]
        self.output_names = [x.name for x in self.sess.get_outputs()]
        
        logging.info(f"   Inputs: {self.input_names}")
        logging.info(f"   Outputs: {self.output_names}")
        
        # 2. Initialize States
        self.reset_states()
        self.buffer = AudioBuffer()

    def reset_states(self):
        # DTLN standard states: [1, 2, 128, 2]
        self.h1 = np.zeros((1, 2, 128, 2), dtype=np.float32)
        self.c1 = np.zeros((1, 2, 128, 2), dtype=np.float32)
        self.h2 = np.zeros((1, 2, 128, 2), dtype=np.float32)
        self.c2 = np.zeros((1, 2, 128, 2), dtype=np.float32)

    def process_chunk(self, raw_bytes):
        try:
            # 1. Deserialize
            audio_48k = np.frombuffer(raw_bytes, dtype=np.float32)
            if len(audio_48k) == 0: return None

            # 2. Resample (48k -> 16k)
            audio_16k = resampy.resample(audio_48k, 48000, 16000)
            self.buffer.write(audio_16k)
            
            output_chunks_16k = []
            
            # 3. Process Blocks
            while True:
                in_block = self.buffer.peek(BLOCK_SIZE)
                if in_block is None: break
                
                # Construct Inputs Dictionary
                # Note: Keys must match the ONNX graph exactly
                inputs = {
                    'input_1': np.expand_dims(in_block, axis=0).astype(np.float32),
                    'h1_in': self.h1,
                    'c1_in': self.c1,
                    'h2_in': self.h2,
                    'c2_in': self.c2
                }
                
                # Run Inference
                results = self.sess.run(None, inputs)
                
                # Update States (Order is typically: Audio, H1, C1, H2, C2)
                # But we should rely on index if names match, or just standard export order
                # DTLN export order is fixed in the script: [prediction, h1, c1, h2, c2]
                processed_block = results[0][0] # Audio
                self.h1 = results[1]
                self.c1 = results[2]
                self.h2 = results[3]
                self.c2 = results[4]
                
                output_chunks_16k.append(processed_block)
                self.buffer.skip(SHIFT_SIZE)

            if not output_chunks_16k: return None

            # 4. Upsample (16k -> 48k)
            full_16k = np.concatenate(output_chunks_16k)
            full_48k = resampy.resample(full_16k, 16000, 48000)
            
            return full_48k.astype(np.float32).tobytes()

        except Exception as e:
            logging.error(f"DTLN Process Error: {e}")
            return None

async def run():
    # NATS Connection with Retry
    nc = None
    while nc is None:
        try:
            nc = await nats.connect(NATS_URL)
        except:
            logging.warning("Waiting for NATS...")
            await asyncio.sleep(2)
            
    js = nc.jetstream()
    logging.info(f"✅ DTLN Connected to NATS")

    # Ensure Stream
    try: await js.add_stream(name="LIVESTREAM_DENOISED", subjects=[OUTPUT_SUBJECT])
    except: pass

    engine = DTLNStreamer()
    loop = asyncio.get_running_loop()

    async def msg_handler(msg):
        # Thread offload for blocking inference
        denoised = await loop.run_in_executor(None, engine.process_chunk, msg.data)
        
        if denoised:
            pts = msg.header.get("pts", "0")
            await js.publish(OUTPUT_SUBJECT, denoised, headers={"pts": pts})

    await js.subscribe(INPUT_SUBJECT, cb=msg_handler)
    logging.info(f"🎧 Listening on {INPUT_SUBJECT}")

    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, lambda: stop.set_result(None))
    await stop
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())