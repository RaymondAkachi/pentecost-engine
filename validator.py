import asyncio
import nats
import numpy as np
import scipy.io.wavfile as wav
import os
import subprocess
import sys

# CONFIG: Capture ~10 seconds
DUMP_LIMIT_CHUNKS = 500  

async def main():
    print("🔎 VALIDATOR STARTED: Diagnostics Mode", flush=True)
    
    # --- 1. PATIENT CONNECTION LOOP ---
    nc = None
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    
    print(f"⏳ Attempting to resolve NATS at: {nats_url}...", flush=True)
    for i in range(60):
        try:
            nc = await nats.connect(nats_url, connect_timeout=2)
            print("✅ CONNECTED to NATS!", flush=True)
            break
        except Exception as e:
            if i % 5 == 0: print(f"   ... waiting for NATS ({e})", flush=True)
            await asyncio.sleep(1)
            
    if not nc:
        print("❌ CRITICAL: Could not connect to NATS. Exiting.", flush=True)
        sys.exit(1)

    js = nc.jetstream()

    # --- 2. DATA CAPTURE ---
    audio_buffer = []
    video_buffer = bytearray()
    
    audio_done = False
    video_done = False

    async def audio_cb(msg):
        nonlocal audio_done
        if audio_done: return
        
        # 1. Receive Raw Float32
        data = np.frombuffer(msg.data, dtype=np.float32)
        audio_buffer.append(data)
        
        if len(audio_buffer) % 100 == 0:
             print(f"🔊 Audio Progress: {len(audio_buffer)}/{DUMP_LIMIT_CHUNKS}", flush=True)

        if len(audio_buffer) >= DUMP_LIMIT_CHUNKS:
            full_audio = np.concatenate(audio_buffer)
            
            # 2. CRITICAL FIX: Convert Float32 -> Int16
            # Float32 is -1.0 to 1.0. Int16 is -32768 to 32767.
            # We multiply by 32767 and cast.
            audio_int16 = (full_audio * 32767).astype(np.int16)
            
            # 3. Save as Standard WAV
            wav.write("/app/temp_audio.wav", 16000, audio_int16)
            
            # 4. Verify File Size
            size = os.path.getsize("/app/temp_audio.wav")
            print(f"✅ Audio Captured: {len(full_audio)} samples -> {size} bytes", flush=True)
            audio_done = True

    async def video_cb(msg):
        nonlocal video_done
        if video_done: return
        
        video_buffer.extend(msg.data)
        
        # Wait for ~5MB video
        if len(video_buffer) > 5 * 1024 * 1024: 
            with open("/app/temp_video.h264", "wb") as f:
                f.write(video_buffer)
            print(f"✅ Video Captured: {len(video_buffer)} bytes", flush=True)
            video_done = True

    print("   -> Subscribing to raw streams...", flush=True)
    await js.subscribe("livestream.audio.raw", cb=audio_cb)
    await js.subscribe("livestream.video.raw", cb=video_cb)

    # --- 3. WAIT & MERGE ---
    print("⏳ Waiting for stream data...", flush=True)
    while not (audio_done and video_done):
        await asyncio.sleep(1)
    
    # Double Check Files Exist
    if not os.path.exists("/app/temp_audio.wav"):
        print("❌ ERROR: Audio file missing!", flush=True)
        return

    print("🔄 Merging Raw Streams -> MP4...", flush=True)
    
    # FFmpeg Command
    cmd = [
        "ffmpeg", "-y", 
        "-r", "25",                     
        "-f", "h264", "-i", "/app/temp_video.h264",
        "-i", "/app/temp_audio.wav",    # Now a standard Int16 WAV
        "-map", "0:v", "-map", "1:a",   
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "128k",
        "-shortest",                    
        "/app/proof.mp4"
    ]
    
    # Capture Full Output for Debugging
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("\n🎉 SUCCESS! 'proof.mp4' created successfully.", flush=True)
    else:
        print(f"\n❌ FFmpeg Merge Failed:\n{result.stderr}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())


# import asyncio
# import nats
# import numpy as np
# import time
# import sys
# import os

# async def main():
#     print("🔎 Validator Started... Monitoring [VIDEO], [AUDIO], and [TEXT]", flush=True)
    
#     # 1. Connect
#     nc = None
#     for i in range(10):
#         try:
#             url = os.getenv("NATS_URL", "nats://nats:4222")
#             nc = await nats.connect(url)
#             break
#         except Exception:
#             print(f"   Waiting for NATS ({i+1}/10)...")
#             await asyncio.sleep(1)
            
#     if not nc:
#         print("❌ CRITICAL: NATS Unreachable")
#         sys.exit(1)

#     js = nc.jetstream()
#     print("✅ Connected. Listening for streams...", flush=True)

#     # 2. Counters
#     counts = {"video": 0, "audio": 0, "text": 0}
#     audio_buffer = []


#     # 3. Handlers
#     async def audio_cb(msg):
#         data = np.frombuffer(msg.data, dtype=np.float32)
#         audio_buffer.append(data)
        
#         # Save first 10 seconds (approx 500 chunks)
#         if len(audio_buffer) == 500:
#             import scipy.io.wavfile as wav
#             full_audio = np.concatenate(audio_buffer)
#             # Save to /app/output which is mounted to your local folder
#             wav.write("/app/test_denoised.wav", 48000, full_audio)
#             print("💾 DUMPED 10s AUDIO to test_denoised.wav - CHECK IT NOW!", flush=True)


#         counts["audio"] += 1
#         # Print every 100th audio frame to reduce noise
#         if counts["audio"] % 100 == 0:
#             pts = msg.header.get("pts", "N/A")
#             print(f"🔊 [AUDIO] Frame #{counts['audio']} | PTS: {pts}", flush=True)

#     async def video_cb(msg):
#         counts["video"] += 1
#         # Print every 50th video frame
#         if counts["video"] % 50 == 0:
#             pts = msg.header.get("pts", "N/A")
#             print(f"🎬 [VIDEO] Frame #{counts['video']} | PTS: {pts}", flush=True)

#     async def text_cb(msg):
#         counts["text"] += 1
#         # ALWAYS print text. This is what we want to see!
#         text = msg.data.decode("utf-8")
#         pts = msg.header.get("pts", "N/A")
#         print(f"📝 [TEXT]  PTS: {pts} | Content: \"{text}\"", flush=True)

#     # 4. Subscribe
#     # We subscribe to all three layers of the pipeline
#     await js.subscribe("livestream.video.raw", cb=video_cb)
#     await js.subscribe("livestream.audio.denoised", cb=audio_cb)
    
#     # The Missing Link: Subscribe to Transcription
#     print("   -> Subscribing to 'livestream.transcription.raw'...", flush=True)
#     await js.subscribe("livestream.transcription.raw", cb=text_cb)

#     # 5. Keep Alive
#     while True:
#         await asyncio.sleep(1)

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         sys.exit(0)