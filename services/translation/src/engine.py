import ctranslate2
import os
import time
import structlog
from typing import Dict, List
from transformers import AutoTokenizer
from .config import settings

logger = structlog.get_logger()

class NatlasEngine:
    def __init__(self):
        self.log = logger.bind(component="n_atlas_engine")
        self.log.info("initializing_model", path=settings.MODEL_PATH)
        
        # 1. Load CTranslate2 Engine
        self.translator = ctranslate2.Translator(
            settings.MODEL_PATH, 
            device="cpu", 
            compute_type="default", 
            inter_threads=1,
            intra_threads=1
        )
        
        # 2. Load Tokenizer
        self.log.info("loading_tokenizer", model="facebook/nllb-200-distilled-600M")
        self.tokenizer = AutoTokenizer.from_pretrained("facebook/nllb-200-distilled-600M")
        
        self.log.info("model_ready", languages=list(settings.TARGET_LANGUAGES.keys()))

    def translate_payload(self, text: str) -> Dict[str, str]:
        start = time.perf_counter()
        results = {}
        
        if not text or len(text.strip()) < 2:
            return {lang: "" for lang in settings.TARGET_LANGUAGES}

        # ---------------------------------------------------------
        # 👇 THE FIX: Force the Source Language Token
        # ---------------------------------------------------------
        # 1. Set source language property (helper for some methods)
        self.tokenizer.src_lang = "eng_Latn"
        
        # 2. Convert text to tokens
        source_tokens = self.tokenizer.convert_ids_to_tokens(self.tokenizer.encode(text))
        
        # 3. CRITICAL: Check if 'eng_Latn' is actually there. If not, add it.
        # HF Tokenizer behavior varies by version. Safer to force it.
        if source_tokens[0] != "eng_Latn":
            source_tokens.insert(0, "eng_Latn")
            
        # ---------------------------------------------------------

        target_codes = list(settings.TARGET_LANGUAGES.values())
        keys = list(settings.TARGET_LANGUAGES.keys())
        
        sources = [source_tokens] * len(target_codes)
        target_prefixes = [[code] for code in target_codes]
        
        try:
            print(f"DEBUG: Entering C++ Inference for '{text[:10]}...'")
            
            translation_results = self.translator.translate_batch(
                sources,
                target_prefix=target_prefixes,
                beam_size=1,            
                repetition_penalty=1.2, 
                no_repeat_ngram_size=3, 
                max_decoding_length=100 
            )
            
            print(f"DEBUG: Exiting C++ Inference")

            for i, result in enumerate(translation_results):
                output_tokens = result.hypotheses[0]
                
                # Cleanup: Remove target language tag if present
                if output_tokens and output_tokens[0] == target_codes[i]:
                    output_tokens = output_tokens[1:]
                
                # Decode
                decoded_text = self.tokenizer.decode(
                    self.tokenizer.convert_tokens_to_ids(output_tokens), 
                    skip_special_tokens=True
                )
                results[keys[i]] = decoded_text

        except Exception as e:
            self.log.error("translation_failed", error=str(e))
            return {lang: "[Error]" for lang in keys}

        duration = (time.perf_counter() - start) * 1000
        
        self.log.info("translation_complete", 
                      latency_ms=f"{duration:.1f}", 
                      input=text[:20], 
                      translations=results)
        
        return results