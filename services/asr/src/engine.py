import torch
import numpy as np
import structlog
import re
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
        
        # FIX: Dynamic Device Mapping (CPU today, GPU tomorrow)
        self.device = torch.device(settings.DEVICE)
        
        try:
            self.processor = WhisperProcessor.from_pretrained(settings.BASE_MODEL)
            
            base_model = WhisperForConditionalGeneration.from_pretrained(
                settings.BASE_MODEL, 
                low_cpu_mem_usage=True
            )
            
            self.model = PeftModel.from_pretrained(base_model, settings.ADAPTER_PATH)
            
            # Map model to active device
            self.model.to(self.device)
            self.model.eval() 
            
            self.log.info("model_ready", adapter=settings.ADAPTER_PATH, device=str(self.device))
            
        except Exception as e:
            self.log.critical("model_load_failed", error=str(e))
            raise e

        self.audio_buffer = np.array([], dtype=np.float32)
        self.buffer_pts_start = 0 
        
    def process_audio(self, pcm_data: np.ndarray, pts: int) -> List[TranscriptionResult]:
        if len(self.audio_buffer) == 0:
            self.buffer_pts_start = pts
            
        self.audio_buffer = np.concatenate((self.audio_buffer, pcm_data))
        
        results = []
        buffer_sec = len(self.audio_buffer) / settings.SAMPLE_RATE
        
        if buffer_sec >= settings.MIN_CONTEXT_SEC:
            full_text = self._transcribe(self.audio_buffer)
            stable_text, cut_idx = self._find_stable_segment(full_text, buffer_sec)
            
            if stable_text:
                duration_processed = cut_idx / settings.SAMPLE_RATE
                
                result = TranscriptionResult(
                    text=stable_text,
                    confidence=0.95, 
                    start_time=0.0, 
                    end_time=duration_processed,
                    source_pts=self.buffer_pts_start
                )
                results.append(result)
                
                self.audio_buffer = self.audio_buffer[cut_idx:]
                self.buffer_pts_start += int(duration_processed * 1000) 
                
                self.log.info("committed_text", text=stable_text[:30], buffer_left_sec=len(self.audio_buffer)/16000)

        return results

    def _transcribe(self, audio: np.ndarray) -> str:
        # FIX: Push input tensors to the active device
        input_features = self.processor(
            audio, 
            sampling_rate=settings.SAMPLE_RATE, 
            return_tensors="pt"
        ).input_features.to(self.device)
        
        with torch.no_grad():
            predicted_ids = self.model.generate(
                input_features, 
                language="en",
                task="transcribe",
                max_new_tokens=128
            )
        
        text = self.processor.batch_decode(predicted_ids, skip_special_tokens=True)[0]
        return text.strip()

    def _find_stable_segment(self, text: str, total_duration: float) -> Tuple[Optional[str], int]:
        matches = list(re.finditer(r'[.?!](?=\s|$)', text))
        
        if not matches:
            if total_duration > settings.MAX_BUFFER_SEC:
                return text, len(self.audio_buffer)
            return None, 0

        last_match = matches[-1]
        stable_text = text[:last_match.end()]
        
        ratio = len(stable_text) / len(text)
        cut_sample = int(len(self.audio_buffer) * ratio)
        
        return stable_text, cut_sample