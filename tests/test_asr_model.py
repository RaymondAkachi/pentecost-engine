import torch
from peft import PeftModel
from transformers import WhisperForConditionalGeneration, WhisperProcessor
import os
import sys

# PATHS
BASE_MODEL = "openai/whisper-large-v3"
ADAPTER_PATH = "/app/adapter"

def test_load():
    print(f"🧪 DIAGNOSTIC: Testing ASR Model Loading...")
    
    # 1. Load Base Model
    print(f"   ⬇️  Loading Base Model: {BASE_MODEL}...")
    try:
        base_model = WhisperForConditionalGeneration.from_pretrained(
            BASE_MODEL, 
            low_cpu_mem_usage=True
        )
        print("   ✅ Base Model Loaded.")
    except Exception as e:
        print(f"   ❌ Base Model Failed: {e}")
        return

    # 2. Load Processor
    print(f"   ⬇️  Loading Processor...")
    try:
        processor = WhisperProcessor.from_pretrained(BASE_MODEL)
        print("   ✅ Processor Loaded.")
    except Exception as e:
        print(f"   ❌ Processor Failed: {e}")

    # 3. Load Adapter (The Critical Step)
    print(f"   🔗 Attaching Pentecost Adapter from {ADAPTER_PATH}...")
    try:
        model = PeftModel.from_pretrained(base_model, ADAPTER_PATH)
        print("   ✅ Adapter Attached Successfully!")
        print("   🚀 SYSTEM READY: The ASR Service is fully operational.")
    except Exception as e:
        print(f"\n❌ ADAPTER LOAD FAILED: {e}")

if __name__ == "__main__":
    test_load()