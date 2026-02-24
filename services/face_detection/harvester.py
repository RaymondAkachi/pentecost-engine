import cv2
import os
import sys
import math
import uuid
import numpy as np

# ==============================================================================
# ENVIRONMENT VALIDATION
# ==============================================================================
try:
    import mediapipe as mp
    if not hasattr(mp, 'solutions'):
        print("❌ CRITICAL ERROR: 'mediapipe' loaded, but 'solutions' is missing.")
        sys.exit(1)
except ImportError:
    print("❌ CRITICAL ERROR: mediapipe is not installed.")
    sys.exit(1)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
INPUT_VIDEO = os.path.join(DATA_DIR, "raw_sermon.mp4")
OUTPUT_DIR = os.path.join(DATA_DIR, "reference_gallery")
SAMPLE_RATE_SEC = 1.0  

REFERENCE_FACIAL_POINTS = np.array([
    [38.2946, 51.6963],  
    [73.5318, 51.5014],  
    [56.0252, 71.7366],  
    [41.5493, 92.3655],  
    [70.7299, 92.2041]   
], dtype=np.float32)

MP_LEFT_PUPIL = 468    
MP_RIGHT_PUPIL = 473   
MP_NOSE = 1            
MP_LEFT_MOUTH = 61     
MP_RIGHT_MOUTH = 291   

os.makedirs(OUTPUT_DIR, exist_ok=True)
RUN_ID = uuid.uuid4().hex[:6]

def extract_and_align_face(frame: np.ndarray, face_detector, face_mesh):
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    h, w, _ = rgb_frame.shape
    
    # ==========================================
    # STAGE 1: Find the Face in the 1080p Frame
    # ==========================================
    detection_results = face_detector.process(rgb_frame)
    if not detection_results.detections:
        return None, "No face detected by global scanner"
        
    best_detection = max(detection_results.detections, key=lambda d: d.score[0])
    if best_detection.score[0] < 0.5:
        return None, "Face detection confidence too low"

    # Get bounding box and add 50% padding so FaceMesh has context
    bbox = best_detection.location_data.relative_bounding_box
    box_x, box_y = bbox.xmin * w, bbox.ymin * h
    box_w, box_h = bbox.width * w, bbox.height * h
    
    pad_x, pad_y = int(box_w * 0.5), int(box_h * 0.5)
    
    x_min = max(0, int(box_x - pad_x))
    y_min = max(0, int(box_y - pad_y))
    x_max = min(w, int(box_x + box_w + pad_x))
    y_max = min(h, int(box_y + box_h + pad_y))
    
    # Crop the original frame
    face_crop = frame[y_min:y_max, x_min:x_max]
    face_crop_rgb = rgb_frame[y_min:y_max, x_min:x_max]
    crop_h, crop_w, _ = face_crop.shape
    
    if crop_h == 0 or crop_w == 0:
        return None, "Invalid crop dimensions"

    # ==========================================
    # STAGE 2: Extract Landmarks from the Crop
    # ==========================================
    mesh_results = face_mesh.process(face_crop_rgb)
    if not mesh_results.multi_face_landmarks:
        return None, "FaceMesh failed to map landmarks on crop"
        
    landmarks = mesh_results.multi_face_landmarks[0].landmark
    
    detected_points = np.array([
        [landmarks[MP_LEFT_PUPIL].x * crop_w, landmarks[MP_LEFT_PUPIL].y * crop_h],
        [landmarks[MP_RIGHT_PUPIL].x * crop_w, landmarks[MP_RIGHT_PUPIL].y * crop_h],
        [landmarks[MP_NOSE].x * crop_w, landmarks[MP_NOSE].y * crop_h],
        [landmarks[MP_LEFT_MOUTH].x * crop_w, landmarks[MP_LEFT_MOUTH].y * crop_h],
        [landmarks[MP_RIGHT_MOUTH].x * crop_w, landmarks[MP_RIGHT_MOUTH].y * crop_h]
    ], dtype=np.float32)
    
    # Calculate transform on the cropped image
    transform_matrix, _ = cv2.estimateAffinePartial2D(
        detected_points, 
        REFERENCE_FACIAL_POINTS, 
        method=cv2.LMEDS
    )
    
    if transform_matrix is None or transform_matrix.shape != (2, 3):
        return None, "Matrix transformation failed"

    aligned_face = cv2.warpAffine(
        face_crop, 
        transform_matrix, 
        (112, 112), 
        borderMode=cv2.BORDER_CONSTANT, 
        borderValue=(0, 0, 0)
    )
    
    return aligned_face, "Success"

def harvest_dataset():
    if not os.path.exists(INPUT_VIDEO):
        print(f"❌ ERROR: Cannot find video at '{INPUT_VIDEO}'.")
        return

    print(f"🎬 Opening '{INPUT_VIDEO}'...")
    cap = cv2.VideoCapture(INPUT_VIDEO)
    
    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps <= 0 or math.isnan(fps):
        fps = 25.0 
        
    frame_jump = int(fps * SAMPLE_RATE_SEC)
    
    # Initialize both models
    with mp.solutions.face_detection.FaceDetection(
        model_selection=1, min_detection_confidence=0.5
    ) as face_detector, \
    mp.solutions.face_mesh.FaceMesh(
        static_image_mode=True, max_num_faces=1, refine_landmarks=True
    ) as face_mesh:
        
        frame_idx = 0
        saved_count = 0
        
        while cap.isOpened():
            ret = cap.grab()
            if not ret:
                break
            
            if frame_idx % frame_jump == 0:
                ret, frame = cap.retrieve()
                if ret:
                    aligned_face, status = extract_and_align_face(frame, face_detector, face_mesh)
                    
                    if aligned_face is not None:
                        out_path = os.path.join(OUTPUT_DIR, f"orokpo_{RUN_ID}_{saved_count:04d}.jpg")
                        cv2.imwrite(out_path, aligned_face)
                        saved_count += 1
                        print(f"✅ Extracted face {saved_count:03d} at {frame_idx / fps:.1f}s")
                    else:
                        print(f"⚠️ Skipped at {frame_idx / fps:.1f}s: {status}")
            
            frame_idx += 1

    cap.release()
    print(f"\n🎉 Harvesting Complete! Saved {saved_count} faces to '{OUTPUT_DIR}'")

if __name__ == "__main__":
    harvest_dataset()