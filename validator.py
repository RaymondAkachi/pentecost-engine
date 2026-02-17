import asyncio
import nats
import numpy as np
import time
import sys
import os

async def main():
    print("🔎 Validator Started... Monitoring Video & Audio Flows", flush=True)
    
    # 1. Connect
    nc = None
    for i in range(10):
        try:
            url = os.getenv("NATS_URL", "nats://nats:4222")
            nc = await nats.connect(url)
            break
        except Exception:
            print(f"   Waiting for NATS ({i+1}/10)...")
            await asyncio.sleep(1)
            
    if not nc:
        print("❌ CRITICAL: NATS Unreachable")
        sys.exit(1)

    js = nc.jetstream()
    print("✅ Connected to JetStream.", flush=True)

    # 2. State Tracking
    video_count = 0
    audio_count = 0
    start_time = time.time()

    # 3. Audio Handler (The Denoised Output)
    async def audio_cb(msg):
        nonlocal audio_count
        audio_count += 1
        data = np.frombuffer(msg.data, dtype=np.float32)
        pts = msg.header.get("pts", "N/A")
        # Print every 50th frame to avoid log spam, or if it's the first one
        if audio_count == 1 or audio_count % 50 == 0:
            print(f"🔊 [AUDIO] Denoised Frame #{audio_count} | PTS: {pts} | Samples: {len(data)}", flush=True)

    # 4. Video Handler (The Raw Feed)
    async def video_cb(msg):
        nonlocal video_count
        video_count += 1
        # Video is raw bytes (H.264/MPEG-TS), so we just check size
        pts = msg.header.get("pts", "N/A")
        if video_count == 1 or video_count % 10 == 0:
            print(f"🎬 [VIDEO] Raw Frame #{video_count}      | PTS: {pts} | Size: {len(msg.data)} bytes", flush=True)

    # 5. Subscribe
    print("   -> Subscribing to 'livestream.audio.denoised'...", flush=True)
    await js.subscribe("livestream.audio.denoised", cb=audio_cb)
    
    print("   -> Subscribing to 'livestream.video.raw'...", flush=True)
    await js.subscribe("livestream.video.raw", cb=video_cb)

    print("✅ Validation Active. Waiting for streams...", flush=True)

    # 6. Keep Alive
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)