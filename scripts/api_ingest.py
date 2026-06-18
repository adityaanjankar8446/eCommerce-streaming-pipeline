import requests
import json
import os
from datetime import datetime

BASE_URL = "https://fakestoreapi.com"

ENDPOINTS = {
    "orders":    f"{BASE_URL}/carts",
    "customers": f"{BASE_URL}/users",
    "products":  f"{BASE_URL}/products"
}

OUTPUT_DIRS = {
    "orders":    "/opt/data/raw/orders",
    "customers": "/opt/data/raw/customers",
    "products":  "/opt/data/raw/products"
}

def fetch_and_save():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for entity, url in ENDPOINTS.items():
        print(f"Fetching {entity}...")
        response = requests.get(url)
        data = response.json()
        
        os.makedirs(OUTPUT_DIRS[entity], exist_ok=True)
        
        # Save as NDJSON (one record per line) ← fixes multiLine issue!
        filepath = f"{OUTPUT_DIRS[entity]}/{entity}_{timestamp}.json"
        with open(filepath, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")
        
        print(f"✅ Saved {len(data)} records → {filepath}")

if __name__ == "__main__":
    fetch_and_save()