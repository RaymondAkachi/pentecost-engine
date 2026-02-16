import ctranslate2
import sentencepiece as spm
import os
import time
import structlog
from typing import Dict, List
from .config import settings

logger = structlog.get_logger()

class NatlasEngine:
    def __init__(self):
        self.log = logger.bind(component="n_atlas_engine")
        self.log.info("initializing_model", path=settings.MODEL_PATH)
        
        # 1. Load CTranslate2 Engine (Auto-detects CPU instruction set: AVX2/AVX512)
        # int8 quantization gives 4x speedup with <1% accuracy loss
        self.translator = ctranslate2.Translator(
            settings.MODEL_PATH, 
            device="cpu", 
            compute_type="int8"
        )
        
        # 2. Load Tokenizer
        sp_model = os.path.join(settings.MODEL_PATH, "sentencepiece.bpe.model")
        self.processor = spm.SentencePieceProcessor()
        self.processor.load(sp_model)
        
        self.log.info("model_ready", languages=list(settings.TARGET_LANGUAGES.keys()))

    def translate_payload(self, text: str) -> Dict[str, str]:
        """
        Translates one English sentence into ALL target languages.
        Returns: {'hausa': '...', 'yoruba': '...'}
        """
        start = time.perf_counter()
        results = {}
        
        if not text or len(text.strip()) < 2:
            return {lang: "" for lang in settings.TARGET_LANGUAGES}

        # 1. Tokenize Source (English)
        # NLLB expects: [eng_Latn] Hello world
        source_tokens = ["eng_Latn"] + self.processor.encode(text, out_type=str)
        
        # 2. Prepare Batches for CTranslate2
        # We can translate to multiple languages in one batch if we replicate the source
        target_codes = list(settings.TARGET_LANGUAGES.values())
        keys = list(settings.TARGET_LANGUAGES.keys())
        
        # Replicate source for each target
        sources = [source_tokens] * len(target_codes)
        # Target prefix (forces the model to translate to that language)
        target_prefixes = [[code] for code in target_codes]
        
        # 3. Inference (The Heavy Lift)
        try:
            translation_results = self.translator.translate_batch(
                sources,
                target_prefix=target_prefixes,
                beam_size=settings.BEAM_SIZE,
                max_batch_size=2024
            )
            
            # 4. Decode
            for i, result in enumerate(translation_results):
                # result.hypotheses[0] is the best prediction
                # Remove the language tag from the output if present
                output_tokens = result.hypotheses[0]
                # Sometimes NLLB repeats the lang tag, skip first token if it matches
                if output_tokens[0] == target_codes[i]:
                    output_tokens = output_tokens[1:]
                    
                decoded_text = self.processor.decode(output_tokens)
                results[keys[i]] = decoded_text

        except Exception as e:
            self.log.error("translation_failed", error=str(e))
            return {lang: "[Error]" for lang in keys}

        duration = (time.perf_counter() - start) * 1000
        self.log.debug("translation_complete", latency_ms=duration, input_len=len(text))
        
        return results