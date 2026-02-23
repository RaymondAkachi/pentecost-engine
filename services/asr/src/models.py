from pydantic import BaseModel

class AudioChunk(BaseModel):
    """Incoming raw audio from Go Ingestion Engine"""
    chunk_id: str
    pts: int       # Changed from timestamp
    duration: float
    data: str      # Base64 encoded PCM float32

class TranscriptionResult(BaseModel):
    """Outgoing text to RAG Layer"""
    text: str
    confidence: float
    start_time: float
    end_time: float
    is_final: bool = True
    source_pts: int