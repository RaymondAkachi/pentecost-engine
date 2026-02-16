import torch
import numpy as np
import soundfile as sf
import io
import structlog
import os
import time
import librosa
from .config import settings

logger = structlog.get_logger()

class FishSpeechEngine:
    def __init__(self):
        self.log = logger.bind(component="fish_speech_engine")
        self.log.info("initializing")
        
        self.device = "cuda" if settings.USE_GPU and torch.cuda.is_available() else "cpu"
        self.log.info("hardware_check", device=self.device)

        # 1. Load Voice References (The "Anointing" Clones)
        self.voice_cache = {}
        self._preload_voices()
        
        # 2. Load AI Model (Mocked for Dev, Real Structure Prepared)
        # In production, this would load the VQ-GAN and LLM components
        self.model_loaded = False
        if os.path.exists(os.path.join(settings.MODEL_PATH, "config.json")):
            self.log.info("loading_real_model")
            # self.model = FishSpeech.load(settings.MODEL_PATH, device=self.device)
            self.model_loaded = True
        else:
            self.log.warning("model_not_found", msg="Using SYNTHETIC SIGNAL GENERATOR for testing")

    def _preload_voices(self):
        """
        Reads all WAV files at startup to prevent disk I/O lag during broadcast.
        """
        for lang, filename in settings.VOICE_MAP.items():
            path = os.path.join(settings.VOICE_DIR, filename)
            try:
                # Load audio, resample to 44.1kHz, mono
                audio, _ = librosa.load(path, sr=settings.SAMPLE_RATE, mono=True)
                # Convert to tensor for the model
                self.voice_cache[lang] = torch.tensor(audio).to(self.device)
                self.log.info("voice_loaded", lang=lang, samples=len(audio))
            except Exception as e:
                self.log.error("voice_load_failed", lang=lang, error=str(e))
                # Fallback: create silent tensor
                self.voice_cache[lang] = torch.zeros(settings.SAMPLE_RATE).to(self.device)

    def synthesize(self, text: str, language: str) -> bytes:
        """
        Input: Text + Language Code
        Output: Raw WAV Bytes
        """
        start = time.perf_counter()
        
        if not text: return b""
        
        try:
            # --- INFERENCE ---
            if self.model_loaded:
                # Real AI Inference (Placeholder)
                # audio_tensor = self.model.generate(text, ref=self.voice_cache[language])
                pass 
            else:
                # Synthetic Fallback (Sine Wave modulated by text length)
                # This proves the pipeline works without needing the GPU yet
                duration = len(text) * 0.08 + 0.5
                t = np.linspace(0, duration, int(settings.SAMPLE_RATE * duration), False)
                # Different frequencies for different languages to distinguish them audibly
                freqs = {
                    "spanish": 220, "french": 261, "swahili": 330, 
                    "portuguese": 392, "german": 440
                }
                base_freq = freqs.get(language, 440)
                audio_data = 0.5 * np.sin(2 * np.pi * base_freq * t)
                audio_data = audio_data.astype(np.float32)

            # --- ENCODING ---
            buffer = io.BytesIO()
            sf.write(buffer, audio_data, settings.SAMPLE_RATE, format='WAV')
            buffer.seek(0)
            result = buffer.getvalue()
            
            latency = (time.perf_counter() - start) * 1000
            self.log.debug("synthesized", lang=language, latency_ms=f"{latency:.1f}")
            return result

        except Exception as e:
            self.log.error("synthesis_error", error=str(e))
            return b""