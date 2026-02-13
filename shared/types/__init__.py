# shared/types/__init__.py
from dataclasses import dataclass
from typing import Optional, List
import numpy as np

@dataclass
class SegmentMetadata:
    segment_id: int
    source_pts: int
    dialect: str
    created_at: float
    processing_deadline: float
    
@dataclass  
class ProcessingResult:
    success: bool
    output_data: Optional[bytes]
    latency_ms: float
    metadata: dict
    error: Optional[str] = None