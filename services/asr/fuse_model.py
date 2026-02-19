# services/asr/fuse_model.py
import torch
from peft import PeftModel
from transformers import WhisperForConditionalGeneration, WhisperTokenizer, WhisperProcessor

# 1. Configuration
BASE_MODEL_ID = "openai/whisper-large-v3"
ADAPTER_PATH = "./adapter" # Where your google colab files are
OUTPUT_PATH = "./fused_model"

print(f"⏳ Loading Base Model: {BASE_MODEL_ID}...")
base_model = WhisperForConditionalGeneration.from_pretrained(
    BASE_MODEL_ID, 
    torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
    low_cpu_mem_usage=True
)

print(f"🔗 Loading Adapter from {ADAPTER_PATH}...")
model = PeftModel.from_pretrained(base_model, ADAPTER_PATH)

print("⚡ Merging LoRA weights into Base Model...")
model = model.merge_and_unload()

print(f"💾 Saving Fused Model to {OUTPUT_PATH}...")
model.save_pretrained(OUTPUT_PATH)

# Also save tokenizer/processor so the new model is complete
processor = WhisperProcessor.from_pretrained(BASE_MODEL_ID)
processor.save_pretrained(OUTPUT_PATH)

print("✅ DONE! You can now use './fused_model' with Faster-Whisper.")