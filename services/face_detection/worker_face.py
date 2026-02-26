import os
import asyncio
import json
import cv2
import numpy as np
import time
from ultralytics import YOLO
import nats
from nats.js.errors import NotFoundError

# ==============================================================================
# GLOBAL CONFIGURATION
# ==============================================================================
DEBUG_MODE = True
DEBUG_DIR = "/shared/debug_faces"
if DEBUG_MODE: os.makedirs(DEBUG_DIR, exist_ok=True)

# State Machine for Temporal Locking (Stage 4)
TRACK_STATE_MACHINE = {}  
TRACK_EXPIRY_SECONDS = 30.0 

print("🧠 Booting Simplified Live Director Engine...", flush=True)

# Stage 1: The Cameraman (YOLO) - Tracks the physical body
global_yolo = YOLO('yolov8n.pt') 

# Stage 2: The Focus Puller (Native OpenCV) - Ensures it's a direct shot of a face
CASCADE_FRONTAL = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

worker_lock = asyncio.Lock()

# ==============================================================================
# THE SPEAKER-LOCK LOGIC
# ==============================================================================
def detect_faces(thumbnail_dir: str) -> dict:
    global TRACK_STATE_MACHINE
    passes, fails, processed_frames = 0, 0, 0
    total_confidence, total_ratio = 0.0, 0.0
    
    current_time = time.time()
    # Garbage collect old tracks
    TRACK_STATE_MACHINE = {k: v for k, v in TRACK_STATE_MACHINE.items() if (current_time - v["last_seen"]) < TRACK_EXPIRY_SECONDS}

    raw_frames = sorted([f for f in os.listdir(thumbnail_dir) if f.endswith(".jpg")])
    frames = [f for f in raw_frames if abs(current_time - os.path.getmtime(os.path.join(thumbnail_dir, f))) < 120]

    if not frames: return {"Confidence": 0.0, "FaceRatio": 0.0, "RequiresFullVideo": False, "Passes": 0, "Fails": 8}

    for frame_file in frames:
        if passes >= 3 or fails >= 12: break

        frame_path = os.path.join(thumbnail_dir, frame_file)
        frame = cv2.imread(frame_path)
        if frame is None: fails += 1; continue

        processed_frames += 1
        img_h, img_w, _ = frame.shape

        # ---------------------------------------------------------
        # STAGE 1: THE CAMERAMAN (YOLO Body Tracker)
        # ---------------------------------------------------------
        results = global_yolo.track(frame, persist=True, tracker="bytetrack.yaml", verbose=False)
        boxes = results[0].boxes
        
        dominant_person, max_area = None, 0
        if boxes is not None and boxes.id is not None:
            for i, box in enumerate(boxes):
                if int(box.cls[0]) == 0: # 0 = Person class in YOLO
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    area = (x2 - x1) * (y2 - y1)
                    if area > max_area:
                        max_area = area
                        dominant_person = {"id": int(boxes.id[i].item()), "box": [x1, y1, x2, y2], "area": area}

        if not dominant_person: fails += 1; continue

        person_ratio = dominant_person["area"] / (img_w * img_h)
        track_id = dominant_person["id"]
        x1, y1, x2, y2 = dominant_person["box"]

        # CROWD KILLER: If the person is tiny (e.g., wide congregation shot), ignore them immediately
        if person_ratio < 0.12: 
            fails += 1; continue

        # ---------------------------------------------------------
        # STAGE 3: THE TEMPORAL LOCK
        # ---------------------------------------------------------
        if track_id not in TRACK_STATE_MACHINE:
            TRACK_STATE_MACHINE[track_id] = {"hits": 0, "last_seen": current_time, "locked": False}
        else:
            TRACK_STATE_MACHINE[track_id]["last_seen"] = current_time

        # If this body was previously verified as a speaker 3 times, bypass AI math completely!
        if TRACK_STATE_MACHINE[track_id]["locked"]:
            passes += 1
            print(f"   ↳ 🟢 {frame_file} | PREACHER LOCK ACTIVE: Body #{track_id} (Ratio: {person_ratio:.2f})", flush=True)
            continue

        # ---------------------------------------------------------
        # STAGE 2: THE FOCUS PULLER (Is this a direct shot?)
        # ---------------------------------------------------------
        # Crop the top 50% of the YOLO body (where the head is)
        head_y2 = int(y1 + ((y2 - y1) * 0.5))
        roi_color = frame[max(0, int(y1)):min(img_h, head_y2), max(0, int(x1)):min(img_w, int(x2))]
        
        if roi_color.size == 0: fails += 1; continue
        roi_gray = cv2.cvtColor(roi_color, cv2.COLOR_BGR2GRAY)

        # Look for a clear, frontal face
        faces = CASCADE_FRONTAL.detectMultiScale(roi_gray, scaleFactor=1.1, minNeighbors=5)
        
        if len(faces) > 0:
            fx, fy, fw, fh = faces[0]
            
            # Blur detection: Reject if the camera is out of focus or panning too fast
            face_crop = roi_gray[fy:fy+fh, fx:fx+fw]
            if cv2.Laplacian(face_crop, cv2.CV_64F).var() >= 10.0:
                
                TRACK_STATE_MACHINE[track_id]["hits"] += 1
                hits = TRACK_STATE_MACHINE[track_id]["hits"]
                
                # Requires 3 separate direct-shot frames to lock the tracker
                if hits >= 3:
                    TRACK_STATE_MACHINE[track_id]["locked"] = True
                    print(f"   ↳ 🔒 {frame_file} | PREACHER VERIFIED! Body #{track_id} locked.", flush=True)
                else:
                    print(f"   ↳ ✅ {frame_file} | DIRECT SHOT FOUND [{hits}/3]: Body #{track_id}.", flush=True)
                    
                passes += 1
                total_confidence += 0.99
                total_ratio += person_ratio
                continue
        
        # If no face is found (turned around, back of head) or it's too blurry
        print(f"   ↳ ❌ {frame_file} | REJECTED: Large body, but no direct face detected.", flush=True)
        fails += 1

    return {
        "Confidence": round(total_confidence / processed_frames if processed_frames else 0, 4),
        "FaceRatio": round(total_ratio / processed_frames if processed_frames else 0, 4),
        "RequiresFullVideo": passes >= 3,
        "Passes": passes,
        "Fails": fails
    }

# ==============================================================================
# NATS ASYNC EVENT LOOP
# ==============================================================================
async def main():
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://nats:4222"))
    js = nc.jetstream()

    async def video_handler(msg):
        try:
            payload = json.loads(msg.data.decode())
            thumbnail_dir = payload.get("thumbnail_dir")
            chunk_id = payload.get("chunk_id")
            
            if not thumbnail_dir or not os.path.exists(thumbnail_dir): return

            print(f"\n🧠 Processing Chunk {chunk_id}...", flush=True)
            async with worker_lock:
                result = await asyncio.to_thread(detect_faces, thumbnail_dir)
            
            decision = "🟢 GPU ROUTE (Preacher Detected)" if result["RequiresFullVideo"] else "🔴 AUDIO ONLY (Crowd/B-Roll)"
            print(f"🎯 {decision} | Passes: {result['Passes']}/8\n", flush=True)

        except Exception as e:
            print(f"❌ Error: {str(e)}", flush=True)
        finally:
            await msg.ack()

    print("✅ Bound to Stream! Listening...", flush=True)
    await js.subscribe("livestream.video.raw", stream="LIVESTREAM_RAW", cb=video_handler)
    
    try:
        while True: await asyncio.sleep(1)
    except asyncio.CancelledError: pass
    finally: await nc.close()

if __name__ == '__main__':
    asyncio.run(main())