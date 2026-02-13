import pytest
import asyncio
# import tempfile
# import shutil
import numpy as np
from pathlib import Path
from typing import AsyncGenerator, Dict, Any

# Ensure we use the modern asyncio fixture loop scope
@pytest.fixture(scope="session")
def event_loop():
    """Create a persistent event loop for the duration of the test session."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

@pytest.fixture(scope="function")
async def nats_client() -> AsyncGenerator:
    """Robust NATS client with automatic cleanup and error handling."""
    from shared.nats_client import PentecostNATSClient
    import os

    # Fallback to localhost if NATS_URL isn't in env
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    client = PentecostNATSClient([nats_url])
    
    try:
        await client.connect()
        yield client
    finally:
        if client.nc.is_connected:
            await client.nc.close()

@pytest.fixture
def sample_audio_chunk() -> Dict[str, Any]:
    """Generates a high-fidelity 48kHz chunk mimicking a 1s Pentecost slice."""
    sample_rate = 48000
    duration = 1.0
    t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
    
    # 440Hz Sine Wave (A4) + Pink-ish Noise for realistic denoising tests
    signal = np.sin(2 * np.pi * 440 * t) * 0.4
    noise = np.random.normal(0, 0.05, len(t))
    audio_data = (signal + noise).astype(np.float32)
    
    return {
        'data': audio_data.tobytes(),
        'sample_rate': sample_rate,
        'format': 'PCM_32',
        'pts': int(asyncio.get_event_loop().time() * 1000)
    }

@pytest.fixture
def performance_thresholds():
    """Hard-coded SLAs from the Pentecost Blueprint."""
    return {
        'denoiser': {'p95_ms': 25, 'quality_min': 3.5},
        'asr': {'p95_ms': 350, 'accuracy_min': 0.85},
        'tts': {'p95_ms': 150, 'similarity_min': 0.80},
        'vsync': {'p95_ms': 2200, 'sync_offset_ms': 30},
        'buffer_limit_s': 60.0 # Pentecost Buffer Max
    }