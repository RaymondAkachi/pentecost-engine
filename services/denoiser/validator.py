# services/denoiser/validator.py
import asyncio
import nats
import numpy as np
import time
import sys

async def main():
    print("🔎 Validator Started: Waiting for NATS...", flush=True)
    
    # 1. Connect with Retry Logic
    nc = None
    for i in range(10):
        try:
            nc = await nats.connect("nats://nats:4222")
            break
        except Exception:
            print(f"   ...retrying connection ({i+1}/10)")
            await asyncio.sleep(1)
    
    if not nc:
        print("❌ CRITICAL: Could not connect to NATS.")
        sys.exit(1)

    js = nc.jetstream()
    print("✅ Connected to NATS.")

    # 2. Subscribe
    received_count = 0
    
    async def validation_handler(msg):
        nonlocal received_count
        data = np.frombuffer(msg.data, dtype=np.float32)
        pts = msg.header.get("PTS", "Unknown")
        
        if len(data) > 0:
            received_count += 1
            print(f"   ✅ Verified Frame #{received_count} (PTS: {pts}) | Size: {len(data)}", flush=True)

    # Subscribe to the OUTPUT of the Denoiser
    await js.subscribe("livestream.audio.denoised", cb=validation_handler)

    # 3. The "Patience" Loop (Wait up to 60s)
    print("⏳ Waiting for pipeline to spin up (Limit: 60s)...", flush=True)
    start_time = time.time()
    
    while time.time() - start_time < 60:
        if received_count >= 10:
            print(f"\n✨ SUCCESS: Validated {received_count} frames passing through Layer 0!")
            sys.exit(0) # Success Exit Code
        await asyncio.sleep(1)

    print(f"\n❌ FAILURE: Timeout. Only received {received_count} frames.")
    sys.exit(1) # Error Exit Code

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)