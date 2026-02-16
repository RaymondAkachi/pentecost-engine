import torch
import numpy as np
import structlog
import time
from transformers import WhisperProcessor, WhisperForConditionalGeneration
from peft import PeftModel
from typing import List, Tuple, Optional
from .config import settings
from .models import TranscriptionResult

logger = structlog.get_logger()

class ASREngine:
    def __init__(self):
        self.log = logger.bind(component="prophetic_asr_engine")
        self.log.info("initializing_model", base=settings.BASE_MODEL)
        
        # 1. Load Model Components
        try:
            self.processor = WhisperProcessor.from_pretrained(settings.BASE_MODEL)
            
            # Load Base Model (Low CPU Memory mode)
            base_model = WhisperForConditionalGeneration.from_pretrained(
                settings.BASE_MODEL, 
                low_cpu_mem_usage=True
            )
            
            # Load Adapter
            self.model = PeftModel.from_pretrained(base_model, settings.ADAPTER_PATH)
            self.model.eval() # Inference Mode
            
            self.log.info("model_ready", adapter=settings.ADAPTER_PATH)
            
        except Exception as e:
            self.log.critical("model_load_failed", error=str(e))
            raise e

        # 2. Initialize Buffer
        self.audio_buffer = np.array([], dtype=np.float32)
        self.buffer_pts_start = 0 # Track the presentation timestamp of the buffer start
        
    def process_audio(self, pcm_data: np.ndarray, pts: int) -> List[TranscriptionResult]:
        """
        Ingests audio, appends to buffer, and returns stable text if context is sufficient.
        """
        # Initialize PTS on first chunk
        if len(self.audio_buffer) == 0:
            self.buffer_pts_start = pts
            
        # Append new data
        self.audio_buffer = np.concatenate((self.audio_buffer, pcm_data))
        
        results = []
        buffer_sec = len(self.audio_buffer) / settings.SAMPLE_RATE
        
        # Check if we have enough "Future Context" (e.g. 10s total)
        if buffer_sec >= settings.MIN_CONTEXT_SEC:
            
            # Transcribe the WHOLE buffer
            # The model uses the end of the buffer as context for the beginning
            full_text = self._transcribe(self.audio_buffer)
            
            # Extract the "Stable" part (e.g., up to the last full sentence)
            stable_text, cut_idx = self._find_stable_segment(full_text, buffer_sec)
            
            if stable_text:
                # Calculate timing
                duration_processed = cut_idx / settings.SAMPLE_RATE
                
                result = TranscriptionResult(
                    text=stable_text,
                    confidence=0.95, # Placeholder for confidence score
                    start_time=0.0, # Relative
                    end_time=duration_processed,
                    source_pts=self.buffer_pts_start
                )
                results.append(result)
                
                # SLIDE THE WINDOW
                # Remove processed audio. The remainder becomes the "Past" for the next chunk.
                self.audio_buffer = self.audio_buffer[cut_idx:]
                # Update PTS tracking (add duration of cut audio)
                self.buffer_pts_start += int(duration_processed * 1000) # Assuming PTS is ms
                
                self.log.info("committed_text", text=stable_text[:30], buffer_left_sec=len(self.audio_buffer)/16000)

        return results

    def _transcribe(self, audio: np.ndarray) -> str:
        """Run Inference"""
        input_features = self.processor(
            audio, 
            sampling_rate=settings.SAMPLE_RATE, 
            return_tensors="pt"
        ).input_features
        
        # Generate
        predicted_ids = self.model.generate(
            input_features, 
            language="en",
            task="transcribe",
            max_new_tokens=128
        )
        
        # Decode
        text = self.processor.batch_decode(predicted_ids, skip_special_tokens=True)[0]
        return text.strip()

    def _find_stable_segment(self, text: str, total_duration: float) -> Tuple[Optional[str], int]:
        """
        Heuristic: Split at the last sentence-ending punctuation.
        """
        import re
        # Look for . ? ! followed by space or end of string
        matches = list(re.finditer(r'[.?!](?=\s|$)', text))
        
        if not matches:
            # No sentence break found. 
            # If buffer is overflowing, force commit everything.
            if total_duration > settings.MAX_BUFFER_SEC:
                return text, len(self.audio_buffer)
            return None, 0

        # Cut at the last punctuation found
        last_match = matches[-1]
        stable_text = text[:last_match.end()]
        
        # Map text length to audio samples (Approximation)
        # This is the "Prophetic Guess" - in a real v2, we'd use token timestamps.
        # Ratio: processed_len / total_len
        ratio = len(stable_text) / len(text)
        cut_sample = int(len(self.audio_buffer) * ratio)
        
        return stable_text, cut_sample