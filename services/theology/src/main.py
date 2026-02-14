import asyncio
import signal
import json
import structlog
import nats
import time
from concurrent.futures import ThreadPoolExecutor
from nats.errors import ConnectionClosedError, TimeoutError

from .config import settings
from .engine import TheologicalEngine
from .models import TranscriptionPayload, EnrichedPayload

# Structured Logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

async def run():
    log = logger.bind(service="theology-rag")
    log.info("startup", config=settings.model_dump())

    # 1. Initialize Engine & Thread Pool
    try:
        engine = TheologicalEngine()
        # Create a pool for CPU-bound tasks (Embedding/Regex)
        executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)
    except Exception as e:
        log.critical("init_failed", error=str(e))
        return

    # 2. Connect to NATS
    try:
        nc = await nats.connect(
            settings.NATS_URL,
            reconnect_time_wait=2,
            max_reconnect_attempts=10,
            ping_interval=20 # Keep connection alive
        )
        js = nc.jetstream()
        log.info("nats_connected")
    except Exception as e:
        log.critical("nats_failed", error=str(e))
        return

    # 3. Stream Setup
    try:
        await js.add_stream(name="LIVESTREAM_TRANSCRIPTION", subjects=[settings.INPUT_SUBJECT, settings.OUTPUT_SUBJECT])
    except: pass

    # 4. Message Handler
    async def message_handler(msg):
        start_time = time.perf_counter()
        try:
            data = json.loads(msg.data.decode())
            payload = TranscriptionPayload(**data)
            
            # CRITICAL: Run CPU-bound work in thread pool to avoid blocking asyncio loop
            loop = asyncio.get_running_loop()
            
            # The 'engine.process_sync' runs in a separate thread
            fixed_text, context, corrections = await loop.run_in_executor(
                executor, 
                engine.process_sync, 
                payload.text
            )
            
            processing_time = (time.perf_counter() - start_time) * 1000
            
            # Prepare Response
            response = EnrichedPayload(
                text=fixed_text,
                original_text=payload.text,
                theological_context=context,
                corrections=corrections,
                source_pts=payload.source_pts,
                confidence=payload.confidence,
                processing_time_ms=processing_time
            )

            await js.publish(
                settings.OUTPUT_SUBJECT,
                response.model_dump_json().encode(),
                headers=msg.headers
            )
            await msg.ack()
            
            if corrections or context:
                log.info("enriched", original=payload.text, final=fixed_text, latency=f"{processing_time:.2f}ms")

        except Exception as e:
            log.error("processing_error", error=str(e))
            await msg.nak()

    # 5. Subscribe
    sub = await js.subscribe(
        settings.INPUT_SUBJECT, 
        cb=message_handler, 
        durable="rag_processor_prod"
    )
    log.info("listening", subject=settings.INPUT_SUBJECT)

    # 6. Graceful Shutdown
    stop_event = asyncio.Event()
    def signal_handler():
        log.info("shutdown_signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    await stop_event.wait()
    
    # Cleanup
    await sub.unsubscribe()
    executor.shutdown(wait=True)
    await nc.drain()
    log.info("shutdown_complete")

if __name__ == "__main__":
    asyncio.run(run())