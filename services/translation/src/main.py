import asyncio
import signal
import json
import uuid
import structlog
import nats
import time
from concurrent.futures import ThreadPoolExecutor
# 👇 We use DeliverPolicy.ALL to ensure we catch the Warmup message even if we boot late
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from nats.errors import TimeoutError
from nats.js.errors import NotFoundError
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

    # 2. NATS Connect
    nc = await nats.connect(settings.NATS_URL)
    js = nc.jetstream()
    
    # Ensure Output Stream Exists
    try:
        await js.add_stream(name="LIVESTREAM_TRANSLATION", subjects=[f"{settings.OUTPUT_SUBJECT_PREFIX}.>"])
    except: pass

    # 3. Message Handler
    async def msg_handler(msg):
        # 👇 DEBUG LOG: Prove we got the message
        log.info("msg_received", subject=msg.subject)
        
        try:
            data = json.loads(msg.data.decode())
            text = data.get("text", "")
            msg_id = data.get("id", str(uuid.uuid4()))
            
            # Offload Translation to CPU Thread
            loop = asyncio.get_running_loop()
            translations = await loop.run_in_executor(
                executor,
                engine.translate_payload,
                text
            )
            
            # Send Reply
            payload = TranslationOutput(
                id=msg_id,
                original_text=text,
                translations=translations,
                source_pts=data.get("source_pts", 0),
                processing_time_ms=100.0
            )
            
            await js.publish(
                f"{settings.OUTPUT_SUBJECT_PREFIX}.done",
                payload.model_dump_json().encode()
            )
            
            await msg.ack()
            log.info("translated", original=text[:20])
            
        except Exception as e:
            log.error("processing_error", error=str(e))
            await msg.nak()

    # 4. ROBUST SUBSCRIPTION LOOP
    while True:
        try:
            # 👇 FIX: Use DeliverPolicy.ALL
            # This ensures if the Tester sent "Warmup" 1 second ago, we still process it.
            await js.subscribe(
                settings.INPUT_SUBJECT, 
                cb=msg_handler,
                config=ConsumerConfig(
                    deliver_policy=DeliverPolicy.ALL, # <--- CHANGED FROM NEW
                    ack_policy=AckPolicy.EXPLICIT
                )
            )
            log.info("listening", subject=settings.INPUT_SUBJECT, policy="DeliverPolicy.ALL")
            break
        except NotFoundError:
            log.warning("waiting_for_stream", subject=settings.INPUT_SUBJECT)
            await asyncio.sleep(2)
        except Exception as e:
            log.error("subscription_error", error=str(e))
            await asyncio.sleep(2)

    # Keep Alive
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    
    await stop_event.wait()
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())