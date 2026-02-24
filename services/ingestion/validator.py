import asyncio
import json
import base64
import os
import subprocess
import nats
from nats.js.errors import NotFoundError

async def main():
    print("🔄 Connecting to NATS JetStream...", flush=True)
    
    # 1. Self-Healing Connection Loop
    while True:
        try:
            nc = await nats.connect(os.getenv("NATS_URL", "nats://nats:4222"))
            break
        except Exception as e:
            print(f"⏳ Waiting for NATS server: {e}", flush=True)
            await asyncio.sleep(2)

    js = nc.jetstream()
    EXPECTED_AUDIO_BYTES = 320000
    EXPECTED_JPEG_COUNT = 15 # 5 seconds * 3 FPS

    async def video_handler(msg):
        try:
            payload = json.loads(msg.data.decode())
            file_path = payload.get("file_path")
            thumbnail_dir = payload.get("thumbnail_dir")
            pts = payload.get("pts")
            
            # --- 1. MP4 VALIDATION ---
            if not os.path.exists(file_path):
                print(f"❌ FAIL | Video PTS: {pts} | File missing at {file_path}", flush=True)
                return

            cmd = [
                "ffprobe", "-v", "error", "-show_entries", "format=duration", 
                "-of", "default=noprint_wrappers=1:nokey=1", file_path
            ]
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            video_pass = True
            
            # --- 2. JPEG THUMBNAIL VALIDATION ---
            thumbs_pass = False
            jpg_count = 0
            if not thumbnail_dir or not os.path.exists(thumbnail_dir):
                print(f"❌ FAIL | Video PTS: {pts} | Thumbnail directory missing: {thumbnail_dir}", flush=True)
            else:
                jpg_files = [f for f in os.listdir(thumbnail_dir) if f.endswith(".jpg")]
                jpg_count = len(jpg_files)
                if jpg_count == EXPECTED_JPEG_COUNT:
                    thumbs_pass = True
                else:
                    print(f"❌ FAIL | Video PTS: {pts} | Thumbnail count mismatch. Expected {EXPECTED_JPEG_COUNT}, got {jpg_count} in {thumbnail_dir}", flush=True)

            # --- 3. FINAL REPORTING ---
            if video_pass and thumbs_pass:
                print(f"✅ PASS | Video PTS: {pts} | MP4 Verified & {jpg_count} JPEGs found at {thumbnail_dir}", flush=True)

        except subprocess.CalledProcessError:
            print(f"❌ FAIL | Video PTS: {pts} | Corrupted MP4 container", flush=True)
        except Exception as e:
            print(f"❌ FAIL | Video | Error: {str(e)}", flush=True)
        finally:
            await msg.ack()

    async def audio_handler(msg):
        try:
            payload = json.loads(msg.data.decode())
            pts = payload.get("pts")
            b64_data = payload.get("data", "")
            
            raw_audio = base64.b64decode(b64_data)
            actual_bytes = len(raw_audio)
            
            if actual_bytes == EXPECTED_AUDIO_BYTES:
                print(f"✅ PASS | Audio PTS: {pts} | Exact 5s duration ({actual_bytes} bytes)", flush=True)
            else:
                calc = actual_bytes / (16000 * 1 * 4)
                print(f"❌ FAIL | Audio PTS: {pts} | Incorrect duration {calc}s", flush=True)
        except Exception as e:
            print(f"❌ FAIL | Audio | Error: {str(e)}", flush=True)
        finally:
            await msg.ack()

    # 2. Prevent Race Condition: Wait for Go to create the stream
    print("🔍 Waiting for Go service to create LIVESTREAM_RAW stream...", flush=True)
    while True:
        try:
            await js.stream_info("LIVESTREAM_RAW")
            break
        except NotFoundError:
            print("⏳ Stream not ready yet, retrying in 2s...", flush=True)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"⚠️ Error checking stream: {e}", flush=True)
            await asyncio.sleep(2)

    print("✅ Stream found! Subscribing...", flush=True)

    # 3. Bind to the streams
    await js.subscribe("livestream.video.raw", stream="LIVESTREAM_RAW", cb=video_handler)
    await js.subscribe("livestream.audio.raw", stream="LIVESTREAM_RAW", cb=audio_handler)
    
    print("🚀 Validator Service Listening! (Waiting 60s for Pentecost Buffer...)", flush=True)
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await nc.drain()

if __name__ == '__main__':
    asyncio.run(main())