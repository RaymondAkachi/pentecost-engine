from pydantic import BaseModel
from typing import Optional, List

class AudioChunk(BaseModel):
    """Incoming raw audio from Denoiser/Ingestion"""
    stream_id: str
    timestamp: float
    data: str  # Base64 encoded PCM float32
    sample_rate: int

class TranscriptionResult(BaseModel):
    """Outgoing text to RAG Layer"""
    text: str
    confidence: float
    start_time: float
    end_time: float
    is_final: bool = True
    source_pts: int