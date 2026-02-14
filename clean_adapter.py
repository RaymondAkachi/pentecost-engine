import json
import os

# Path to your adapter config
config_path = "services/asr/adapter/adapter_config.json"

print(f"🧹 Cleaning {config_path}...")

try:
    with open(config_path, "r") as f:
        data = json.load(f)

    # Check for the bad key
    if "alora_invocation_tokens" in data:
        print(f"⚠️  Found incompatible key: 'alora_invocation_tokens'")
        print(f"   Value: {data['alora_invocation_tokens']}")
        
        # DELETE IT
        del data["alora_invocation_tokens"]
        
        # Save back
        with open(config_path, "w") as f:
            json.dump(data, f, indent=2)
            
        print("✅ Key removed. Config saved successfully.")
    else:
        print("✅ No incompatible keys found. File is already clean.")

except FileNotFoundError:
    print("❌ Error: Could not find adapter_config.json. Check the path!")
except Exception as e:
    print(f"❌ Error: {e}")