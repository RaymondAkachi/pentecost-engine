from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Dict

class Settings(BaseSettings):
    NATS_URL: str = Field(default="nats://nats:4222")
    INPUT_SUBJECT: str = "livestream.translation.done"
    OUTPUT_SUBJECT_PREFIX: str = "livestream.audio"
    
    MODEL_PATH: str = "/app/model"
    VOICE_DIR: str = "/app/voices"
    
    # 5 Global Languages
    VOICE_MAP: Dict[str, str] = {
        "spanish": "spanish_ref.wav",
        "french": "french_ref.wav",
        "swahili": "swahili_ref.wav",
        "portuguese": "portuguese_ref.wav",
        "german": "german_ref.wav"
    }
    
    SAMPLE_RATE: int = 44100
    USE_GPU: bool = False # Set to True in production

    class Config:
        env_prefix = "TTS_"

settings = Settings()