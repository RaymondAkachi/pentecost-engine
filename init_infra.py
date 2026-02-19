import asyncio
import nats
import os
from nats.js.api import StreamConfig, RetentionPolicy, StorageType

async def main():
    print("🏗️  INFRASTRUCTURE: Initializing NATS Streams...", flush=True)
    
    # 1. Connect with Retry
    nc = None
    for i in range(10):
        try:
            nc = await nats.connect(os.getenv("NATS_URL", "nats://nats:4222"))
            break
        except Exception:
            print(f"   ... waiting for NATS ({i+1}/10)")
            await asyncio.sleep(1)
            
    if not nc:
        print("❌ CRITICAL: NATS Unreachable.")
        return

    js = nc.jetstream()

    # 2. Define The Nervous System (Streams)
    # We define ALL streams here. No more "lazy creation" in services.
    streams = [
        # Layer 0: Raw Audio/Video
        {
            "name": "LIVESTREAM_RAW",
            "subjects": ["livestream.audio.raw", "livestream.video.raw"],
            "description": "Raw Ingestion Feed (48k/16k Audio + H.264 Video)"
        },
        # Layer 1: Speech to Text (ASR)
        {
            "name": "LIVESTREAM_TRANSCRIPTION",
            "subjects": ["livestream.transcription.raw", "livestream.transcription.enriched"],
            "description": "Text Stream (Raw ASR -> Theological RAG)"
        },
        # Layer 2: Translation
        {
            "name": "LIVESTREAM_TRANSLATION",
            "subjects": ["livestream.translation.>"],
            "description": "Final Translated Output (All Languages)"
        }
    ]

    for s in streams:
        print(f"   🔹 Configuring Stream: {s['name']}...", end=" ")
        try:
            # Idempotent: Creates if missing, updates if exists
            await js.add_stream(
                name=s["name"],
                subjects=s["subjects"],
                description=s["description"],
                retention=RetentionPolicy.LIMITS,
                max_msgs=10000,
                max_age=300, # Keep messages for 5 minutes
                storage=StorageType.MEMORY
            )
            print("✅ OK")
        except Exception as e:
            print(f"⚠️  OK (Existing)")

    print("\n✅ INFRASTRUCTURE READY. AI Services can now launch safely.")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())