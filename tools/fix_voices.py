import os
import numpy as np
import soundfile as sf

VOICE_DIR = "services/tts/voices"
SAMPLE_RATE = 44100
DURATION = 5.0  # 5 seconds of audio

def create_dummy_audio(filepath):
    # Generate a complex tone (mimicking voice frequencies)
    t = np.linspace(0, DURATION, int(SAMPLE_RATE * DURATION), False)
    # Mix of 150Hz (fundamental male voice) and harmonics
    audio = 0.5 * np.sin(2 * np.pi * 150 * t) + 0.3 * np.sin(2 * np.pi * 300 * t)
    
    # Add some noise to make it "realistic" for the encoder
    noise = np.random.normal(0, 0.01, audio.shape)
    final_audio = audio + noise
    
    sf.write(filepath, final_audio, SAMPLE_RATE)
    print(f"✅ Generated dummy voice reference: {filepath}")

def fix():
    if not os.path.exists(VOICE_DIR):
        os.makedirs(VOICE_DIR)
        
    for lang in ["spanish", "french", "swahili", "portuguese", "german"]:
        filename = f"{lang}_ref.wav"
        path = os.path.join(VOICE_DIR, filename)
        
        # Create if missing or empty
        if not os.path.exists(path) or os.path.getsize(path) < 100:
            create_dummy_audio(path)
        else:
            print(f"👍 Valid file exists: {filename}")

if __name__ == "__main__":
    fix()