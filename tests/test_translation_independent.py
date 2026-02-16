import asyncio
import json
import os
import uuid
import nats
from nats.errors import TimeoutError
from nats.js.errors import NotFoundError, BadRequestError

EXPECTED_LANGUAGES = ["spanish", "french", "swahili", "portuguese", "german"]

async def run_test():
    print("🧪 INITIALIZING TRANSLATION ISOLATION TEST (v3 - Warmup)...")
    
    # 1. Connect
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    print("✅ Connected to NATS")

    # 2. FORCE CLEAN & CREATE STREAMS
    # We delete the stream first to avoid "Configuration Mismatch" errors
    try:
        await js.delete_stream("LIVESTREAM_TRANSCRIPTION")
        print("   - Cleaned old Input Stream")
    except: pass
    
    # Create Input Stream (Where we send data)
    await js.add_stream(name="LIVESTREAM_TRANSCRIPTION", subjects=["livestream.transcription.enriched"])
    
    # Create Output Stream (Where we listen) - Service usually does this, but we do it to be safe
    try:
        await js.add_stream(name="LIVESTREAM_TRANSLATION", subjects=["livestream.translation.>"])
    except: pass # It's okay if it exists
    
    print("✅ Streams Configured")

    # 3. THE WARMUP PROTOCOL (The Fix for the Timeout)
    print(f"⏳ Waiting for Service Model Load (This takes ~30s)...")
    
    sub = await js.subscribe("livestream.translation.done")
    
    service_ready = False
    warmup_id = "warmup-check"
    
    # Try for up to 60 seconds
    for i in range(30):
        # Send a dummy message
        payload = {"id": warmup_id, "text": "Warmup", "source_pts": 0}
        await js.publish("livestream.transcription.enriched", json.dumps(payload).encode())
        
        try:
            # Wait briefly for a reply
            msg = await sub.next_msg(timeout=2)
            data = json.loads(msg.data.decode())
            if data.get("id") == warmup_id:
                print(f"✅ SERVICE IS AWAKE! (Latency: {i*2}s)")
                await msg.ack()
                service_ready = True
                break
        except TimeoutError:
            print(f"   ... still loading model ({i*2}s)")
            continue
    
    if not service_ready:
        print("❌ FATAL: Service never responded to warmup. Check logs.")
        return

    # 4. RUN REAL TESTS
    test_cases = [
        {"text": "Hello, welcome to the livestream.", "desc": "Basic Greeting"},
        {"text": "The power of the Holy Spirit is here.", "desc": "Theological Sentence"},
        {"text": "We are moving into a Kairos moment.", "desc": "Complex Vocabulary"}
    ]

    score = 0
    print(f"\n🚀 Sending {len(test_cases)} Test Payloads...")

    for i, case in enumerate(test_cases):
        print(f"\n🔹 TEST {i+1}: {case['desc']}")
        
        msg_id = str(uuid.uuid4())
        payload = {
            "id": msg_id,
            "text": case['text'],
            "source_pts": i * 1000
        }
        
        await js.publish("livestream.transcription.enriched", json.dumps(payload).encode())

        try:
            # Now that it's warm, it should be fast (5s timeout is plenty)
            msg = await sub.next_msg(timeout=5)
            data = json.loads(msg.data.decode())
            
            # Ensure we aren't reading old warmup messages
            while data.get("id") == warmup_id:
                 msg = await sub.next_msg(timeout=5)
                 data = json.loads(msg.data.decode())

            translations = data.get("translations", {})
            print(f"   Received {len(translations)} languages.")
            
            missing = [lang for lang in EXPECTED_LANGUAGES if lang not in translations]
            
            if not missing:
                print("   ✅ PASS")
                score += 1
            else:
                print(f"   ❌ FAIL: Missing {missing}")
            
            await msg.ack()

        except TimeoutError:
            print("   ❌ TIMEOUT - Logic Error.")

    print(f"\n{'='*30}")
    print(f"RESULTS: {score}/{len(test_cases)} Passed")
    print(f"{'='*30}")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run_test())