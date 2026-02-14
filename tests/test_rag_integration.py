import asyncio
import json
import os
import uuid
import nats
from nats.errors import TimeoutError
from nats.js.errors import NotFoundError
from nats.js.api import ConsumerConfig, DeliverPolicy

async def run_test():
    print("🧪 INITIALIZING MEGA-GLOSSARY TEST (v3 - Strict ID Matching)...")
    
    # 1. Connect
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 2. Wait for RAG Stream
    print(f"⏳ Waiting for RAG Service...")
    for i in range(30):
        try:
            await js.stream_info("LIVESTREAM_TRANSCRIPTION")
            break
        except NotFoundError:
            await asyncio.sleep(1)
    
    # 3. Subscribe (New Messages Only)
    sub = await js.subscribe(
        "livestream.transcription.enriched",
        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW)
    )

    # 4. WARMUP
    print("🔥 Warming up RAG Engine...")
    warmup_id = str(uuid.uuid4())
    warmup_payload = {"text": "Warmup", "source_pts": 0, "confidence": 1.0, "id": warmup_id}
    
    engine_ready = False
    for i in range(20):
        await js.publish("livestream.transcription.raw", json.dumps(warmup_payload).encode())
        try:
            msg = await sub.next_msg(timeout=1)
            data = json.loads(msg.data.decode())
            # Check if this is OUR warmup response
            if data.get('original_text') == "Warmup":
                print("✅ RAG Engine is HOT!")
                engine_ready = True
                break
        except TimeoutError:
            print(f"   ... loading ({i+1}/20)")
            await asyncio.sleep(1)
            
    if not engine_ready:
        print("❌ TIMEOUT: Engine never responded.")
        return

    # 5. MEGA TEST CASES
    test_cases = [
        {"input": "We need the dunamis power.", "check": "Dunamis", "type": "context"},
        {"input": "He spoke with ex oo sia.", "check": "Exousia", "type": "correction"},
        {"input": "The watchers are looking.", "check": "Watchers", "type": "context"},
        {"input": "Break the evil alters.", "check": "Altars", "type": "correction"},
        {"input": "He walks in the order of milk is a deck.", "check": "Melchizedek", "type": "correction"},
        {"input": "The apple stolic anointing.", "check": "Apostolic", "type": "correction"},
        {"input": "We are rising in a tension.", "check": "Ascension", "type": "correction"},
        {"input": "God's heavy car board glory.", "check": "Kabod", "type": "correction"}
    ]

    print(f"\n🚀 Running {len(test_cases)} Deep Theological Checks...")
    score = 0

    for i, case in enumerate(test_cases):
        print(f"\n🔹 TEST {i+1}: {case['check']}")
        
        # Unique ID for this specific test case
        test_id = str(uuid.uuid4())
        payload = {"text": case['input'], "source_pts": i, "confidence": 0.9, "id": test_id}
        
        # Publish
        await js.publish("livestream.transcription.raw", json.dumps(payload).encode())

        # LOOP until we get THE matching message (Ignore delayed/duplicate messages)
        got_valid_response = False
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < 5.0:
            try:
                msg = await sub.next_msg(timeout=1)
                data = json.loads(msg.data.decode())
                
                # Verify this is the response to CURRENT input
                if data['original_text'] == case['input']:
                    text = data['text']
                    context = data.get('theological_context') or ""
                    
                    print(f"   In:  '{case['input']}'")
                    print(f"   Out: '{text}'")
                    if context: print(f"   Ctx: {context[:40]}...")

                    passed = False
                    if case['type'] == 'correction':
                        if case['check'] in text: passed = True
                    elif case['type'] == 'context':
                        if case['check'] in context or case['check'] in text: passed = True
                    
                    if passed:
                        print("   ✅ PASS")
                        score += 1
                    else:
                        print(f"   ❌ FAIL (Expected {case['check']})")
                    
                    got_valid_response = True
                    break # Exit the wait loop, move to next test
                else:
                    # This is a stale message from a previous test/warmup. Ignore it.
                    continue

            except TimeoutError:
                continue
        
        if not got_valid_response:
             print("   ❌ TIMEOUT - No matching response received")

    print(f"\n{'='*30}")
    print(f"FINAL SCORE: {score}/{len(test_cases)}")
    print(f"{'='*30}")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run_test())