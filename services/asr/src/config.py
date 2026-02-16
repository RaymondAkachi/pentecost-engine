from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # NATS Configuration
    NATS_URL: str = Field(default="nats://nats:4222")
    INPUT_SUBJECT: str = "livestream.audio.raw"  # From Denoiser
    OUTPUT_SUBJECT: str = "livestream.transcription.raw"
    
    # Model Configuration
    BASE_MODEL: str = "openai/whisper-large-v3"
    ADAPTER_PATH: str = "/app/adapter"
    DEVICE: str = "cpu" # Force CPU for now
    
    # Prophetic Buffer Settings
    SAMPLE_RATE: int = 16000
    MIN_CONTEXT_SEC: float = 10.0  # Look-ahead window
    COMMIT_INTERVAL_SEC: float = 4.0 # How often we output text
    MAX_BUFFER_SEC: float = 30.0   # Max buffer size before forced flush

    class Config:
        env_prefix = "ASR_"

settings = Settings()