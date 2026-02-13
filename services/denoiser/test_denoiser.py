import pytest
import time
import numpy as np
import torch
import sys
import os


from main import DeepFilterNetService
from shared.types.pentecost_pb2 import AudioFrame
# from google.protobuf.timestamp_pb2 import Timestamp

@pytest.fixture(scope="module")
def denoiser_service():
    """
    Initialize the heavy model ONCE for all tests.
    This simulates the persistent service behavior.
    """
    print("\n[Setup] Loading DeepFilterNet3 Model (this may take a moment)...")
    service = DeepFilterNetService()
    yield service
    # Cleanup (if necessary)
    del service

def create_audio_frame(pcm_data: np.ndarray, stream_id: str = "test_stream") -> AudioFrame:
    """Helper to wrap raw numpy audio into our Pentecost Protocol."""
    frame = AudioFrame()
    frame.pcm_data = pcm_data.tobytes()
    frame.sample_rate = 48000
    frame.source_stream_id = stream_id
    frame.pts = int(time.time() * 1000)
    
    # Add timestamp
    now = time.time()
    frame.captured_at.seconds = int(now)
    frame.captured_at.nanos = int((now - int(now)) * 1_000_000_000)
    
    return frame

def test_inference_latency(denoiser_service):
    """
    Performance Test: Ensures the Neural Network inference is fast enough (<20ms).
    """
    # 1. Generate 20ms of random noise (Worst case for entropy)
    samples = int(0.02 * 48000) # 960 samples
    audio = np.random.randn(samples).astype(np.float32)
    frame = create_audio_frame(audio)

    # 2. Warmup (The first run is always slow due to JIT/Allocation)
    print("\n[Warmup] Running initial inference...")
    for _ in range(5):
        denoiser_service.process_frame(frame)

    # 3. Measure
    latencies = []
    iterations = 50
    print(f"[Measure] Running {iterations} iterations...")
    
    for _ in range(iterations):
        start = time.perf_counter()
        # We test the synchronous processing logic directly
        denoiser_service.process_frame(frame)
        duration_ms = (time.perf_counter() - start) * 1000
        latencies.append(duration_ms)

    # 4. Analysis
    p95 = np.percentile(latencies, 95)
    avg = np.mean(latencies)
    
    print(f"  -> Average Latency: {avg:.2f}ms")
    print(f"  -> p95 Latency:     {p95:.2f}ms")

    # SLA Check
    assert p95 < 25.0, f"Performance Fail: p95 latency {p95:.2f}ms > 25ms SLA"

def test_noise_suppression_quality(denoiser_service):
    """
    Quality Test: Verifies that the model actually removes noise from a signal.
    """
    # 1. Create a clean 440Hz Sine Wave (The "Holy Signal")
    duration = 1.0 # 1 second to give the RNN context
    sr = 48000
    t = np.linspace(0, duration, int(sr * duration), endpoint=False)
    clean_signal = np.sin(2 * np.pi * 440 * t) * 0.5

    # 2. Add Heavy Noise (The "Worldly Distraction")
    noise = np.random.normal(0, 0.2, len(t))
    noisy_signal = (clean_signal + noise).astype(np.float32)

    # 3. Process the audio in chunks (mimicking the stream)
    chunk_size = 960 # 20ms
    enhanced_chunks = []
    
    stream_id = "quality_test_stream"
    
    for i in range(0, len(noisy_signal), chunk_size):
        chunk = noisy_signal[i:i+chunk_size]
        if len(chunk) < chunk_size: break # Skip partial end
        
        frame = create_audio_frame(chunk, stream_id)
        result_frame = denoiser_service.process_frame(frame)
        
        enhanced_chunk = np.frombuffer(result_frame.pcm_data, dtype=np.float32)
        enhanced_chunks.append(enhanced_chunk)

    enhanced_signal = np.concatenate(enhanced_chunks)
    
    # Align lengths for comparison
    min_len = min(len(clean_signal), len(enhanced_signal))
    clean_segment = clean_signal[:min_len]
    enhanced_segment = enhanced_signal[:min_len]

    # 4. Calculate SNR Improvement
    # Measure error before and after
    noise_before = np.std(noisy_signal[:min_len] - clean_segment)
    noise_after = np.std(enhanced_segment - clean_segment)
    
    # Avoid div by zero
    improvement_ratio = noise_before / (noise_after + 1e-9)
    improvement_db = 20 * np.log10(improvement_ratio)

    print(f"\n[Quality] Noise Reduction: {improvement_db:.2f} dB")

    # We expect at least 10dB of improvement
    assert improvement_db > 10.0, f"Quality Fail: Only {improvement_db:.2f}dB reduction (Target: >10dB)"

def test_state_continuity(denoiser_service):
    """
    Logic Test: Ensures the RNN state is maintained across frames.
    """
    # Send Frame 1
    frame1 = create_audio_frame(np.zeros(960, dtype=np.float32), "stream_A")
    denoiser_service.process_frame(frame1)
    
    # Check that state was created
    assert "stream_A" in denoiser_service.states, "State was not initialized for stream_A"
    
    # Send Frame 2
    frame2 = create_audio_frame(np.zeros(960, dtype=np.float32), "stream_A")
    denoiser_service.process_frame(frame2)
    
    # Ensure state persists (identity check)
    initial_state = denoiser_service.states["stream_A"]
    denoiser_service.process_frame(frame2)
    assert denoiser_service.states["stream_A"] is initial_state, "State object was recreated unnecessarily!"