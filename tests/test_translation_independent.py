import asyncio
import json
import os
import uuid
import nats
from nats.errors import TimeoutError
from nats.js.errors import NotFoundError, BadRequestError

EXPECTED_LANGUAGES = ["spanish", "french"]

async def run_test():
    print("🧪 INITIALIZING TRANSLATION ISOLATION TEST (v6 - Race Condition Fixed)...")
    
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    print("✅ Connected to NATS")

    # 1. Setup Input Stream
    try:
        await js.delete_stream("LIVESTREAM_TRANSCRIPTION")
    except: pass
    await js.add_stream(name="LIVESTREAM_TRANSCRIPTION", subjects=["livestream.transcription.enriched"])

    # 2. Setup Output Stream (THE FIX: Create it here, don't wait for Service)
    try:
        await js.add_stream(name="LIVESTREAM_TRANSLATION", subjects=["livestream.translation.>"])
        print("✅ Output Stream Pre-Created")
    except: 
        print("⚠️ Output Stream already exists")

    # 3. WARMUP
    print(f"⏳ Sending ONE Warmup Message and waiting...")
    sub = await js.subscribe("livestream.translation.done")
    warmup_id = "warmup-check"
    
    # Send Warmup
    await js.publish("livestream.transcription.enriched", json.dumps({"id": warmup_id, "text": "Warmup", "source_pts": 0}).encode())
    
    service_ready = False
    start = asyncio.get_event_loop().time()
    
    # Wait up to 90s for Model Load + First Inference
    while (asyncio.get_event_loop().time() - start) < 90:
        try:
            msg = await sub.next_msg(timeout=1)
            data = json.loads(msg.data.decode())
            if data.get("id") == warmup_id:
                print(f"✅ SERVICE IS AWAKE! (Warmed up)")
                await msg.ack()
                service_ready = True
                break
        except TimeoutError:
            continue
    
    if not service_ready:
        print("❌ FATAL: Service timed out on warmup.")
        return

    # 4. REAL TEST
    test_cases = [
        {"text": "Jesus loves you.", "desc": "Simple Theological"},
        {"text": "The quick brown fox jumps over the lazy dog.", "desc": "Complex Sentence"}
    ]

    print(f"\n🚀 Sending {len(test_cases)} Test Payloads...")

    for i, case in enumerate(test_cases):
        print(f"\n🔹 TEST {i+1}: {case['desc']}")
        print(f"   Input: '{case['text']}'")
        
        msg_id = str(uuid.uuid4())
        payload = {"id": msg_id, "text": case['text'], "source_pts": i * 1000}
        
        await js.publish("livestream.transcription.enriched", json.dumps(payload).encode())

        try:
            # Wait for translation
            msg = await sub.next_msg(timeout=45)
            data = json.loads(msg.data.decode())
            
            # Skip old warmup messages if any
            while data.get("id") == warmup_id:
                 msg = await sub.next_msg(timeout=45)
                 data = json.loads(msg.data.decode())

            translations = data.get("translations", {})
            
            # VISUAL OUTPUT
            print("   ✅ TRANSLATION RECEIVED:")
            print(json.dumps(translations, indent=4, ensure_ascii=False))
            
            await msg.ack()

        except TimeoutError:
            print("   ❌ TIMEOUT - Test Failed.")

    await nc.close()

if __name__ == "__main__":
    asyncio.run(run_test())