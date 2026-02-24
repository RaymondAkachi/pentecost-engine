import os
import cv2
import shutil
import numpy as np
import mediapipe as mp
import onnxruntime as ort

# ==============================================================================
# GLOBAL INITIALIZATION (Models & Reference Gallery)
# ==============================================================================
ONNX_MODEL_PATH = "/app/models/mobilefacenet.onnx"
REFERENCE_DIR = "/app/reference_gallery"
APOSTLE_EMBEDDINGS = []
ort_session = None

REFERENCE_FACIAL_POINTS = np.array([
    [38.2946, 51.6963], [73.5318, 51.5014], [56.0252, 71.7366],
    [41.5493, 92.3655], [70.7299, 92.2041]
], dtype=np.float32)

MP_LEFT_PUPIL, MP_RIGHT_PUPIL = 468, 473
MP_NOSE, MP_LEFT_MOUTH, MP_RIGHT_MOUTH = 1, 61, 291

def compute_embedding(rgb_crop: np.ndarray) -> np.ndarray:
    input_tensor = (rgb_crop.astype(np.float32) - 127.5) / 128.0
    input_tensor = np.transpose(input_tensor, (2, 0, 1))
    input_tensor = np.expand_dims(input_tensor, axis=0)
    
    outputs = ort_session.run(None, {ort_session.get_inputs()[0].name: input_tensor})
    embedding = outputs[0][0]
    return embedding / np.linalg.norm(embedding)

if os.path.exists(ONNX_MODEL_PATH) and os.path.exists(REFERENCE_DIR):
    sess_options = ort.SessionOptions()
    sess_options.intra_op_num_threads = 1 
    sess_options.log_severity_level = 3 # Silences the ONNX warnings
    ort_session = ort.InferenceSession(ONNX_MODEL_PATH, sess_options)
    
    for file in os.listdir(REFERENCE_DIR):
        if file.lower().endswith(('.png', '.jpg', '.jpeg')):
            img_path = os.path.join(REFERENCE_DIR, file)
            ref_image = cv2.cvtColor(cv2.imread(img_path), cv2.COLOR_BGR2RGB)
            APOSTLE_EMBEDDINGS.append(compute_embedding(ref_image))
    print(f"✅ SYSTEM READY: Loaded {len(APOSTLE_EMBEDDINGS)} reference profiles.")
else:
    print("⚠️ WARNING: ONNX model or Reference Directory missing.")
    exit(1)

def cosine_similarity(emb1: np.ndarray, emb2: np.ndarray) -> float:
    return np.dot(emb1, emb2)

# ==============================================================================
# THE DETECTION LOGIC (No Temporal dependencies)
# ==============================================================================
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

            if confidence < 0.5 or face_ratio < 0.10:
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
            
            print(f"Frame {frame_file} -> Similarity Score: {max_sim:.4f}")

            if max_sim >= 0.55:
                passes += 1
            else:
                fails += 1

    return {
        "Confidence": round(total_confidence / processed_frames if processed_frames else 0, 4),
        "FaceRatio": round(total_ratio / processed_frames if processed_frames else 0, 4),
        "RequiresFullVideo": requires_full_video
    }

if __name__ == "__main__":
    print("\n==================================================")
    print("🚀 STARTING STANDALONE FACE DETECTION TEST")
    print("==================================================\n")

    test_dir = "/app/test_frames"
    os.makedirs(test_dir, exist_ok=True)

    # 1. Clean the test directory
    for f in os.listdir(test_dir):
        os.remove(os.path.join(test_dir, f))

    # 2. Simulate the Go Service dropping 15 frames
    images = [f for f in os.listdir(REFERENCE_DIR) if f.endswith(('.jpg', '.jpeg'))]
    print(f"📂 Simulating Go Ingestion: Copying {min(15, len(images))} frames to {test_dir}...")
    
    for i, img in enumerate(images[:15]):
        shutil.copy(os.path.join(REFERENCE_DIR, img), os.path.join(test_dir, f"frame_{i:02d}.jpg"))

    # 3. Run the Core Logic
    print("\n🧠 RUNNING AI ENGINE...\n")
    result = detect_faces(test_dir)

    print("\n==================================================")
    print(f"🎯 FINAL DECISION RESULT: {result}")
    print("==================================================\n")