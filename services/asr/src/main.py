import asyncio
import signal
import json
import base64
import structlog
import nats
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor
from .config import settings
from .engine import ASREngine
from .models import AudioChunk

# Logging Setup
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

async def run():
    log = logger.bind(service="pentecost-asr")
    log.info("startup", config=settings.model_dump())

    # 1. Initialize Engine (Heavy Load)
    # We use a ThreadPool to keep the main loop free for NATS
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        engine = ASREngine()
    except Exception as e:
        log.critical("engine_init_failed", error=str(e))
        return

    # 2. Connect NATS
    try:
        nc = await nats.connect(settings.NATS_URL)
        js = nc.jetstream()
        log.info("nats_connected")
    except Exception as e:
        log.critical("nats_failed", error=str(e))
        return

    # Ensure Stream Exists
    try:
        await js.add_stream(name="LIVESTREAM_TRANSCRIPTION", subjects=[settings.OUTPUT_SUBJECT])
    except: pass

    # 3. Message Handler
    async def msg_handler(msg):
        try:
            # Decode Payload
            data = json.loads(msg.data.decode())
            
            # Decode Audio (Base64 -> Float32)
            # We assume input is Float32 PCM encoded as Base64
            audio_bytes = base64.b64decode(data['data'])
            audio_np = np.frombuffer(audio_bytes, dtype=np.float32)
            
            pts = data.get('timestamp', 0)
            
            # Offload Inference to Thread
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(
                executor,
                engine.process_audio,
                audio_np,
                pts
            )
            
            # Publish Results (if any)
            for res in results:
                await js.publish(
                    settings.OUTPUT_SUBJECT,
                    res.model_dump_json().encode()
                )
                log.info("published_transcription", text=res.text[:20])
            
            await msg.ack()

        except Exception as e:
            log.error("processing_error", error=str(e))
            # Don't nak immediately to avoid loop storm on bad data
            await msg.ack() 

    # 4. Subscribe
    await js.subscribe(
        settings.INPUT_SUBJECT,
        cb=msg_handler,
        durable="asr_processor_v1"
    )
    log.info("listening", subject=settings.INPUT_SUBJECT)

    # 5. Graceful Shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    
    await stop_event.wait()
    
    # Cleanup
    executor.shutdown(wait=True)
    await nc.drain()
    log.info("shutdown_complete")

if __name__ == "__main__":
    asyncio.run(run())