import os

API_KEYS = [
    key.strip()
    for key in os.getenv("API_KEYS", "").split(",")
    if key.strip()
]