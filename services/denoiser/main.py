import asyncio
import os
import numpy as np
import nats
import logging
import signal
import ctypes

# Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [RNNOISE-C] - %(levelname)s - %(message)s'
)

# Configuration
NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
INPUT_SUBJECT = "livestream.audio.raw"
OUTPUT_SUBJECT = "livestream.audio.denoised"

# Constants
SAMPLE_RATE = 48000
FRAME_SIZE = 480  # RNNoise standard: 480 samples (10ms)

class RNNoiseWrapper:
    """
    Direct C-Binding to the RNNoise library.
    """
    def __init__(self, lib_path="/usr/lib/librnnoise.so"):
        logging.info(f"📚 Loading RNNoise library from {lib_path}...")
        
        # 1. Load the Shared Library
        self.lib = ctypes.CDLL(lib_path)
        
        # 2. Define C Function Signatures
        self.lib.rnnoise_create.argtypes = [ctypes.c_void_p]
        self.lib.rnnoise_create.restype = ctypes.c_void_p
        self.lib.rnnoise_destroy.argtypes = [ctypes.c_void_p]
        self.lib.rnnoise_process_frame.argtypes = [
            ctypes.c_void_p, 
            ctypes.POINTER(ctypes.c_float), 
            ctypes.POINTER(ctypes.c_float)
        ]
        self.lib.rnnoise_process_frame.restype = ctypes.c_float
        
        # 3. Initialize State
        self.st = self.lib.rnnoise_create(None)
        if not self.st:
            raise RuntimeError("Failed to create RNNoise state")
        logging.info("✅ RNNoise C-State Initialized")

    def process(self, frame_float):
        # Ensure input is standard C-float array
        in_buffer = frame_float.astype(np.float32)
        out_buffer = np.zeros(FRAME_SIZE, dtype=np.float32)
        
        # Get pointers
        in_ptr = in_buffer.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        out_ptr = out_buffer.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        
        # Call C function
        self.lib.rnnoise_process_frame(self.st, out_ptr, in_ptr)
        
        return out_buffer

class DenoiserService:
    def __init__(self):
        self.rnnoise = RNNoiseWrapper()
        self._buffer = np.array([], dtype=np.float32)
        
    def process_chunk(self, raw_bytes):
        try:
            # 1. Deserialize
            audio_f32 = np.frombuffer(raw_bytes, dtype=np.float32)
            
            # 2. Buffer
            self._buffer = np.concatenate((self._buffer, audio_f32))
            
            output_frames = []
            
            # 3. Process 480-sample frames
            while len(self._buffer) >= FRAME_SIZE:
                frame = self._buffer[:FRAME_SIZE]
                self._buffer = self._buffer[FRAME_SIZE:]
                
                # Scale [-1.0, 1.0] -> [-32768, 32768]
                frame_scaled = frame * 32768.0
                
                # Inference
                denoised_scaled = self.rnnoise.process(frame_scaled)
                
                # Descale
                denoised_norm = denoised_scaled / 32768.0
                output_frames.append(denoised_norm)

            if not output_frames:
                return None
            
            return np.concatenate(output_frames).tobytes()

        except Exception as e:
            logging.error(f"Processing Error: {e}")
            return None

async def run():
    nc = None
    while nc is None:
        try:
            nc = await nats.connect(NATS_URL)
        except:
            await asyncio.sleep(1)
    
    js = nc.jetstream()
    try: await js.add_stream(name="LIVESTREAM_DENOISED", subjects=[OUTPUT_SUBJECT])
    except: pass
    
    service = DenoiserService()
    loop = asyncio.get_running_loop()
    
    async def msg_handler(msg):
        denoised = await loop.run_in_executor(None, service.process_chunk, msg.data)
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