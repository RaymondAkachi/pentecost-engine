import asyncio
import nats
import json
import os
import sys
from datetime import datetime
from nats.errors import ConnectionClosedError, TimeoutError, NoRespondersError

async def main():
    print("🚀 PENTECOST MONITOR: Initializing...", flush=True)

    # --- 1. ROBUST CONNECTION ---
    nc = None
    while True:
        try:
            nc = await nats.connect(os.getenv("NATS_URL", "nats://nats:4222"))
            print("✅ Connected to NATS Nervous System")
            break
        except Exception:
            print("⏳ Waiting for NATS...", flush=True)
            await asyncio.sleep(2)

    js = nc.jetstream()

    # --- 2. FAIL-PROOF SUBSCRIPTION ---
    # We loop until streams actually exist. We don't crash.
    subs = {}
    targets = [
        ("ASR", "livestream.transcription.raw"),
        ("RAG", "livestream.transcription.enriched"),
        ("TRANS", "livestream.translation.done")
    ]

    print("📡 Hunting for Active Streams...", flush=True)
    
    # Keep trying to subscribe until successful
    while len(subs) < len(targets):
        for label, subject in targets:
            if label in subs: continue # Already subscribed
            
            try:
                # DeliverPolicy.NEW ensures we only see LIVE data, not old history
                s = await js.subscribe(subject, deliver_policy=nats.js.api.DeliverPolicy.NEW)
                subs[label] = s
                print(f"   ✅ Locked on: [{label}] -> {subject}")
            except Exception:
                # Stream doesn't exist yet? That's fine. We wait.
                pass
        
        if len(subs) < len(targets):
            await asyncio.sleep(1)

    print("\n🟢 ALL SYSTEMS GO. MONITORING LIVE TRAFFIC.\n")
    print("TIME     | STAGE | CONTENT")
    print("-" * 60)

    # --- 3. MESSAGE PROCESSING LOOP ---
    async def process_msg(label, sub):
        while True:
            try:
                msg = await sub.next_msg(timeout=1000) # Wait forever technically
                data = json.loads(msg.data.decode())
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                if label == "ASR":
                    print(f"{timestamp} | 🎤 ASR | {data.get('text', '')}", flush=True)
                
                elif label == "RAG":
                    # Only show RAG if it CHANGED something, otherwise it's noise
                    if data.get('theological_check'):
                         original = data.get('original_text', '')
                         fixed = data.get('text', '')
                         print(f"{timestamp} | 🛡️ RAG | ⚠️ DOCTRINE FLIP: '{original}' -> '{fixed}'", flush=True)
                
                elif label == "TRANS":
                    trans = data.get('translations', {})
                    # Just show one language to keep UI clean, or count them
                    count = len(trans)
                    # print(f"{timestamp} | 🌎 TRN | Broadcast to {count} languages", flush=True)
                    for lang, text in trans.items():
                         print(f"           |       | └─ {lang.upper()}: {text}", flush=True)

                await msg.ack()
                
            except nats.errors.TimeoutError:
                # No messages recently? Just loop.
                continue
            except Exception as e:
                print(f"⚠️ Error parsing {label}: {e}")

    # Run listeners in parallel
    tasks = [process_msg(label, sub) for label, sub in subs.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Monitor Stopped")