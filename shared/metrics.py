# shared/metrics.py
from prometheus_client import Histogram, Counter, Gauge, Info
import time
from functools import wraps

# Layer 0: Ingestion
INGESTION_LATENCY = Histogram(
    'pentecost_ingestion_latency_seconds',
    'Time from capture to NATS publish',
    ['stream_type']  # 'audio', 'video'
)

INGESTION_THROUGHPUT = Counter(
    'pentecost_ingestion_frames_total',
    'Total frames ingested',
    ['stream_type']
)

# Layer 1: Denoising
DENOISER_LATENCY = Histogram(
    'pentecost_denoiser_latency_seconds',
    'DeepFilterNet3 processing time',
    buckets=[0.005, 0.01, 0.015, 0.02, 0.025, 0.05]
)

DENOISER_QUALITY = Gauge(
    'pentecost_denoiser_quality_pesq',
    'Estimated PESQ score post-denoising'
)

# Layer 2: ASR
ASR_LATENCY = Histogram(
    'pentecost_asr_latency_seconds',
    'Whisper transcription time',
    buckets=[0.1, 0.2, 0.3, 0.5, 1.0]
)

ASR_CONFIDENCE = Histogram(
    'pentecost_asr_confidence',
    'Transcription confidence score',
    buckets=[0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
)

# Layer 3: TTS
TTS_LATENCY = Histogram(
    'pentecost_tts_latency_seconds',
    'Fish Speech synthesis time',
    ['emotion_class']
)

TTS_SIMILARITY = Gauge(
    'pentecost_tts_voice_similarity',
    'Similarity to reference voice (0-1)'
)

# Layer 4: Video Sync
VSYNC_LATENCY = Histogram(
    'pentecost_vsync_latency_seconds',
    'LatentSync processing time per segment'
)

VSYNC_QUALITY = Gauge(
    'pentecost_vsync_lip_sync_accuracy',
    'Lip-sync quality score (0-1)'
)

# Layer 5: Synchronization
SYNC_DRIFT = Gauge(
    'pentecost_sync_drift_ms',
    'PTS drift between video and audio',
    ['dialect']
)

BUFFER_FILL_RATIO = Gauge(
    'pentecost_buffer_fill_ratio',
    'Pentecost buffer utilization (0-1)',
    ['dialect']
)

DROPPED_SEGMENTS = Counter(
    'pentecost_dropped_segments_total',
    'Segments dropped due to deadline',
    ['dialect', 'reason']
)

def measure_latency(histogram):
    """Decorator to measure function latency"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                histogram.observe(time.time() - start)
        return wrapper
    return decorator