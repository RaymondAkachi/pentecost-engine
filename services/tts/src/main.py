import asyncio
import signal
import json
import base64
import structlog
import nats
import time
from concurrent.futures import ThreadPoolExecutor
from .config import settings
from .engine import FishSpeechEngine
from pydantic import BaseModel

structlog.configure(
    processors=[structlog.processors.JSONRenderer()],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

class AudioPayload(BaseModel):
    id: str
    language: str
    audio_data: str # Base64
    sample_rate: int
    source_pts: int

async def run():
    log = logger.bind(service="pentecost-tts")
    log.info("startup")

    engine = FishSpeechEngine()
    
    # Critical Optimization: One thread per language to ensure parallel generation
    executor = ThreadPoolExecutor(max_workers=5)

    nc = await nats.connect(settings.NATS_URL)
    js = nc.jetstream()
    
    # Ensure streams exist
    try:
        await js.add_stream(name="LIVESTREAM_AUDIO", subjects=[f"{settings.OUTPUT_SUBJECT_PREFIX}.>"])
    except: pass

    async def msg_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            translations = data.get("translations", {})
            msg_id = data.get("id")
            pts = data.get("source_pts", 0)

            # 1. Define the task function
            def generate_task(lang, txt):
                audio_bytes = engine.synthesize(txt, lang)
                return lang, audio_bytes

            # 2. Schedule all languages at once
            loop = asyncio.get_running_loop()
            tasks = []
            for lang, text in translations.items():
                if text:
                    tasks.append(loop.run_in_executor(executor, generate_task, lang, text))

            # 3. Wait for all (Parallel Execution)
            results = await asyncio.gather(*tasks)

            # 4. Publish results
            for lang, audio in results:
                if not audio: continue
                
                payload = AudioPayload(
                    id=msg_id,
                    language=lang,
                    audio_data=base64.b64encode(audio).decode('utf-8'),
                    sample_rate=settings.SAMPLE_RATE,
                    source_pts=pts
                )
                
                await js.publish(
                    f"{settings.OUTPUT_SUBJECT_PREFIX}.{lang}",
                    payload.model_dump_json().encode()
                )
            
            await msg.ack()
            
        except Exception as e:
            log.error("handler_failed", error=str(e))
            await msg.nak()

    await js.subscribe(settings.INPUT_SUBJECT, cb=msg_handler, durable="tts_worker")
    log.info("listening")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await stop_event.wait()
    executor.shutdown()
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run())