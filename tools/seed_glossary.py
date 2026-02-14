import json
import os

# The "Golden Source" of Theological Truth
MASTER_DEFINITIONS = [
    {"term": "Koinonia", "definition": "Deep spiritual fellowship and sharing with the Holy Spirit.", "misspellings": ["coin on here", "corn on ear", "coinonia"]},
    {"term": "Kairos", "definition": "The opportune, appointed moment of God (Divine Time).", "misspellings": ["cairo", "kairose"]},
    {"term": "Chronos", "definition": "Sequential, chronological time.", "misspellings": ["cronus"]},
    {"term": "Dunamis", "definition": "Inherent miracle-working power.", "misspellings": ["dynamis", "do na miss"]},
    {"term": "Energia", "definition": "Working power; energy in operation.", "misspellings": ["energy a", "inner gia"]},
    {"term": "Exousia", "definition": "Delegated authority or legal right.", "misspellings": ["ex oo sia", "exo see a"]},
    {"term": "Ischus", "definition": "Great might or strength.", "misspellings": ["is cuss", "ish us"]},
    {"term": "Mantle", "definition": "A spiritual covering or authority passed down.", "misspellings": ["mental", "mantel"]},
    {"term": "Zoe", "definition": "The God-kind of life; eternal life.", "misspellings": ["zoey", "zone"]},
    {"term": "Kabod", "definition": "The heavy weight of God's glory.", "misspellings": ["cupboard", "car board"]},
    {"term": "Ascension", "definition": "Rising in spiritual rank or stature.", "misspellings": ["a tension"]},
    {"term": "Legislators", "definition": "Believers who decree God's will into the earth realm.", "misspellings": ["legislatures"]},
    {"term": "Ordinances", "definition": "Divine laws or decrees set in the spirit.", "misspellings": ["audiences"]},
    {"term": "Necromancy", "definition": "Consulting the dead; a forbidden practice.", "misspellings": ["neck romancy"]},
    {"term": "Divination", "definition": "Seeking knowledge of the future by supernatural means other than God.", "misspellings": ["divine nation"]},
    {"term": "Watchers", "definition": "Angelic beings that oversee the affairs of men.", "misspellings": ["watch us"]},
    {"term": "Altars", "definition": "Platforms where the spirit realm interacts with the physical.", "misspellings": ["alters"]},
    {"term": "Alignment", "definition": "Coming into agreement with God's will and timing.", "misspellings": []}
]

def seed():
    path = "services/theology/glossary.json"
    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    with open(path, "w") as f:
        json.dump(MASTER_DEFINITIONS, f, indent=2)
    
    print(f"✅ SUCCESSFULLY SEEDED {len(MASTER_DEFINITIONS)} THEOLOGICAL TERMS.")
    print(f"📍 Location: {path}")

if __name__ == "__main__":
    seed()