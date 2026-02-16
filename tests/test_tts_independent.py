import asyncio
import json
import os
import uuid
import base64
import nats
from nats.errors import TimeoutError
from nats.js.errors import NotFoundError

# We expect these 5 to come back
EXPECTED_LANGUAGES = ["spanish", "french", "swahili", "portuguese", "german"]

async def run_test():
    print("🧪 INITIALIZING TTS ISOLATION TEST...")
    
    # 1. Connect (Increase max_payload on client side too!)
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        nc = await nats.connect(nats_url, max_reconnect_attempts=5)
        js = nc.jetstream()
        print("✅ Connected to NATS")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 2. Wait for Stream
    print(f"⏳ Waiting for TTS Service...")
    stream_ready = False
    for i in range(30):
        try:
            await js.stream_info("LIVESTREAM_AUDIO")
            stream_ready = True
            print("✅ Service Online!")
            break
        except NotFoundError:
            await asyncio.sleep(1)
    
    if not stream_ready:
        print("❌ TIMEOUT: TTS Service failed to boot.")
        return

    # 3. Subscribe to ALL Audio Outputs
    # wildcard '>' means we get "livestream.audio.spanish", "livestream.audio.french", etc.
    sub = await js.subscribe("livestream.audio.>")
    
    # 4. Send Test Payload
    msg_id = str(uuid.uuid4())
    payload = {
        "id": msg_id,
        "source_pts": 12345,
        "translations": {
            "spanish": "Hola mundo",
            "french": "Bonjour le monde",
            "swahili": "Hujambo dunia",
            "portuguese": "Ola mundo",
            "german": "Hallo welt"
        }
    }
    
    print("\n🚀 Sending Translation Payload...")
    # The TTS service listens to 'livestream.translation.done'
    await js.publish("livestream.translation.done", json.dumps(payload).encode())

    # 5. Collect Responses
    received_audio = {}
    print("⏳ Listening for Audio Streams (timeout 10s)...")
    
    start_time = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start_time) < 10.0:
        try:
            msg = await sub.next_msg(timeout=1)
            data = json.loads(msg.data.decode())
            
            lang = data['language']
            audio_b64 = data['audio_data']
            audio_bytes = base64.b64decode(audio_b64)
            size_kb = len(audio_bytes) / 1024
            
            print(f"   Received {lang.upper()}: {size_kb:.1f} KB Audio")
            
            # Basic Validation: Is it actually WAV?
            if audio_bytes[:4] == b'RIFF':
                received_audio[lang] = True
            else:
                print(f"   ❌ ERROR: {lang} is not a valid WAV file header!")

            if len(received_audio) == 5:
                break
                
        except TimeoutError:
            continue

    # 6. Report
    print(f"\n{'='*30}")
    missing = [L for L in EXPECTED_LANGUAGES if L not in received_audio]
    if not missing:
        print("✅ PASS: All 5 Audio Streams Generated & Validated.")
    else:
        print(f"❌ FAIL: Missing Audio for {missing}")
    print(f"{'='*30}")
    
    await nc.close()

if __name__ == "__main__":
    asyncio.run(run_test())