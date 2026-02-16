import asyncio
import signal
import json
import uuid
import structlog
import nats
import time
from concurrent.futures import ThreadPoolExecutor
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy # 👈 IMPORT THESE
from .config import settings
from .engine import NatlasEngine
from pydantic import BaseModel

structlog.configure(
    processors=[structlog.processors.JSONRenderer()],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

class TranslationOutput(BaseModel):
    id: str
    original_text: str
    translations: dict
    source_pts: int
    processing_time_ms: float

async def run():
    log = logger.bind(service="n-atlas-translation")
    log.info("startup")

    # 1. Init Engine
    try:
        engine = NatlasEngine()
        executor = ThreadPoolExecutor(max_workers=1)
    except Exception as e:
        log.critical("init_failed", error=str(e))
        return

    # 2. NATS
    nc = await nats.connect(settings.NATS_URL)
    js = nc.jetstream()
    
    # Ensure Output Streams Exist
    try:
        await js.add_stream(name="LIVESTREAM_TRANSLATION", subjects=[f"{settings.OUTPUT_SUBJECT_PREFIX}.>"])
    except: pass

    async def msg_handler(msg):
        # 👇 LOG IMMEDIATELY to prove receipt
        # log.debug("msg_received", subject=msg.subject) 
        
        start_t = time.perf_counter()
        try:
            data = json.loads(msg.data.decode())
            text = data.get("text", "")
            msg_id = data.get("id", str(uuid.uuid4()))
            
            # Offload to CPU Thread
            loop = asyncio.get_running_loop()
            translations = await loop.run_in_executor(
                executor,
                engine.translate_payload,
                text
            )
            
            duration = (time.perf_counter() - start_t) * 1000
            
            payload = TranslationOutput(
                id=msg_id,
                original_text=text,
                translations=translations,
                source_pts=data.get("source_pts", 0),
                processing_time_ms=duration
            )
            
            await js.publish(
                f"{settings.OUTPUT_SUBJECT_PREFIX}.done",
                payload.model_dump_json().encode()
            )
            
            await msg.ack()
            log.info("translated", original=text[:20], latency=f"{duration:.1f}ms")
            
        except Exception as e:
            log.error("processing_error", error=str(e))
            await msg.nak()

    # 👇 THE FIX: EXPLICIT CONSUMER CONFIGURATION
    # We force DeliverPolicy.NEW so we don't get stuck processing old "Ghost" messages
    # We remove 'durable' for the test phase to ensure a fresh start every time, 
    # OR we configure the durable to be robust. Let's use an Ephemeral consumer for safety now.
    
    await js.subscribe(
        settings.INPUT_SUBJECT, 
        cb=msg_handler,
        # durable="natlas_processor",  <-- REMOVED to prevent "Stuck Consumer" issues during dev
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.NEW,
            ack_policy=AckPolicy.EXPLICIT
        )
    )
    log.info("listening", subject=settings.INPUT_SUBJECT, policy="new_messages_only")

    # Keep Alive
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    
    await stop_event.wait()
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())