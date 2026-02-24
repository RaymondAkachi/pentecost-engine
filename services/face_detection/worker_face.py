import asyncio
import json
import os
import cv2
import numpy as np
import mediapipe as mp
import onnxruntime as ort
import nats
from nats.js.errors import NotFoundError

# ==============================================================================
# GLOBAL INITIALIZATION (AI Models & Gallery)
# ==============================================================================
ONNX_MODEL_PATH = "/app/models/mobilefacenet.onnx"
REFERENCE_DIR = "/app/reference_gallery"
APOSTLE_EMBEDDINGS = []
ort_session = None

# Standard 112x112 MobileFaceNet 5-Point Template
REFERENCE_FACIAL_POINTS = np.array([
    [38.2946, 51.6963], [73.5318, 51.5014], [56.0252, 71.7366],
    [41.5493, 92.3655], [70.7299, 92.2041]
], dtype=np.float32)

MP_LEFT_PUPIL, MP_RIGHT_PUPIL = 468, 473
MP_NOSE, MP_LEFT_MOUTH, MP_RIGHT_MOUTH = 1, 61, 291

def compute_embedding(rgb_crop: np.ndarray) -> np.ndarray:
    resized = cv2.resize(rgb_crop, (112, 112))
    input_tensor = (resized.astype(np.float32) - 127.5) / 128.0
    input_tensor = np.transpose(input_tensor, (2, 0, 1))
    input_tensor = np.expand_dims(input_tensor, axis=0)
    
    outputs = ort_session.run(None, {ort_session.get_inputs()[0].name: input_tensor})
    embedding = outputs[0][0]
    return embedding / np.linalg.norm(embedding)

if os.path.exists(ONNX_MODEL_PATH) and os.path.exists(REFERENCE_DIR):
    sess_options = ort.SessionOptions()
    sess_options.intra_op_num_threads = 1 
    sess_options.log_severity_level = 3 # Silences ONNX warnings
    ort_session = ort.InferenceSession(ONNX_MODEL_PATH, sess_options)
    
    for file in os.listdir(REFERENCE_DIR):
        if file.lower().endswith(('.png', '.jpg', '.jpeg')):
            img_path = os.path.join(REFERENCE_DIR, file)
            ref_image = cv2.cvtColor(cv2.imread(img_path), cv2.COLOR_BGR2RGB)
            APOSTLE_EMBEDDINGS.append(compute_embedding(ref_image))
    print(f"✅ AI SYSTEM READY: Loaded {len(APOSTLE_EMBEDDINGS)} reference profiles.", flush=True)
else:
    print("⚠️ WARNING: ONNX model or Reference Directory missing.", flush=True)
    exit(1)

def cosine_similarity(emb1: np.ndarray, emb2: np.ndarray) -> float:
    return np.dot(emb1, emb2)

def detect_faces(thumbnail_dir: str) -> dict:
    passes, fails, processed_frames = 0, 0, 0
    total_confidence, total_ratio = 0.0, 0.0
    requires_full_video = False

    frames = sorted([f for f in os.listdir(thumbnail_dir) if f.endswith(".jpg")])

    with mp.solutions.face_detection.FaceDetection(model_selection=1, min_detection_confidence=0.5) as face_detector, \
         mp.solutions.face_mesh.FaceMesh(static_image_mode=True, max_num_faces=1, refine_landmarks=True) as face_mesh:

        for frame_file in frames:
            if passes == 8:
                requires_full_video = True
                break
            if fails == 8:
                break

            frame_path = os.path.join(thumbnail_dir, frame_file)
            frame = cv2.imread(frame_path)
            if frame is None:
                fails += 1; continue

            processed_frames += 1
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, _ = rgb_frame.shape

            det_results = face_detector.process(rgb_frame)
            if not det_results.detections:
                fails += 1; continue

            best_det = max(det_results.detections, key=lambda d: d.score[0])
            confidence = best_det.score[0]
            
            bbox = best_det.location_data.relative_bounding_box
            face_ratio = bbox.width * bbox.height
            total_confidence += confidence
            total_ratio += face_ratio

            # FIX 1: Lowered face_ratio to 0.01 (1% of screen) to accommodate medium shots
            if confidence < 0.5 or face_ratio < 0.01:
                fails += 1; continue

            pad_x, pad_y = int(bbox.width * w * 0.5), int(bbox.height * h * 0.5)
            x_min = max(0, int((bbox.xmin * w) - pad_x))
            y_min = max(0, int((bbox.ymin * h) - pad_y))
            x_max = min(w, int((bbox.xmin * w) + (bbox.width * w) + pad_x))
            y_max = min(h, int((bbox.ymin * h) + (bbox.height * h) + pad_y))
            
            crop_rgb = rgb_frame[y_min:y_max, x_min:x_max]
            if crop_rgb.size == 0:
                fails += 1; continue

            mesh_results = face_mesh.process(crop_rgb)
            if not mesh_results.multi_face_landmarks:
                fails += 1; continue
                
            lm = mesh_results.multi_face_landmarks[0].landmark
            ch, cw, _ = crop_rgb.shape
            
            det_pts = np.array([
                [lm[MP_LEFT_PUPIL].x * cw, lm[MP_LEFT_PUPIL].y * ch],
                [lm[MP_RIGHT_PUPIL].x * cw, lm[MP_RIGHT_PUPIL].y * ch],
                [lm[MP_NOSE].x * cw, lm[MP_NOSE].y * ch],
                [lm[MP_LEFT_MOUTH].x * cw, lm[MP_LEFT_MOUTH].y * ch],
                [lm[MP_RIGHT_MOUTH].x * cw, lm[MP_RIGHT_MOUTH].y * ch]
            ], dtype=np.float32)

            t_matrix, _ = cv2.estimateAffinePartial2D(det_pts, REFERENCE_FACIAL_POINTS, method=cv2.LMEDS)
            if t_matrix is None or t_matrix.shape != (2, 3):
                fails += 1; continue

            aligned_face = cv2.warpAffine(crop_rgb, t_matrix, (112, 112), borderMode=cv2.BORDER_CONSTANT)

            live_embedding = compute_embedding(aligned_face)
            max_sim = max(cosine_similarity(ref_emb, live_embedding) for ref_emb in APOSTLE_EMBEDDINGS)
            
            print(f"   ↳ {frame_file} | Similarity: {max_sim:.4f}", flush=True)

            # FIX 2: Corrected threshold to standard SFace baseline (0.363) lowering to (0.360) for more leniency on diverse content.
            if max_sim >= 0.360:
                passes += 1
            else:
                fails += 1

    return {
        "Confidence": round(total_confidence / processed_frames if processed_frames else 0, 4),
        "FaceRatio": round(total_ratio / processed_frames if processed_frames else 0, 4),
        "RequiresFullVideo": requires_full_video,
        "Passes": passes,
        "Fails": fails
    }

async def main():
    print("🔄 Connecting to NATS JetStream...", flush=True)
    while True:
        try:
            nc = await nats.connect(os.getenv("NATS_URL", "nats://nats:4222"))
            break
        except Exception as e:
            print(f"⏳ Waiting for NATS server: {e}", flush=True)
            await asyncio.sleep(2)

    js = nc.jetstream()

    async def video_handler(msg):
        try:
            payload = json.loads(msg.data.decode())
            thumbnail_dir = payload.get("thumbnail_dir")
            chunk_id = payload.get("chunk_id")
            
            if not thumbnail_dir or not os.path.exists(thumbnail_dir):
                print(f"❌ FAIL | Chunk {chunk_id}: Thumbnail directory missing.", flush=True)
                return

            print(f"🧠 Processing Chunk {chunk_id}...", flush=True)
            result = detect_faces(thumbnail_dir)
            
            decision = "🟢 BYPASS ACTIVE (Full GPU)" if result["RequiresFullVideo"] else "🔴 AUDIO ONLY (Pass-through)"
            print(f"🎯 {decision} | Passes: {result['Passes']}/8 | Avg Confidence: {result['Confidence']}\n", flush=True)

        except Exception as e:
            print(f"❌ FAIL | Processing Error: {str(e)}", flush=True)
        finally:
            await msg.ack()

    print("🔍 Waiting for Go service to create LIVESTREAM_RAW stream...", flush=True)
    while True:
        try:
            await js.stream_info("LIVESTREAM_RAW")
            break
        except NotFoundError:
            await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(2)

    print("✅ Bound to Stream! Listening for payloads...", flush=True)
    await js.subscribe("livestream.video.raw", stream="LIVESTREAM_RAW", cb=video_handler)
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await nc.drain()

if __name__ == '__main__':
    asyncio.run(main())