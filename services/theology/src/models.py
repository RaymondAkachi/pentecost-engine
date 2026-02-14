from pydantic import BaseModel
from typing import List, Optional

class TranscriptionPayload(BaseModel):
    text: str
    source_pts: int
    confidence: float = 0.0

class EnrichedPayload(BaseModel):
    text: str
    original_text: str
    theological_context: Optional[str] = None
    corrections: List[str] = []
    source_pts: int
    confidence: float
    is_enriched: bool = True
    processing_time_ms: float = 0.0