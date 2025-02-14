import requests
import pandas as pd
import time
from tqdm import tqdm  # For progress bar

# API Endpoints
TAGS_URL = "https://sky.coflnet.com/api/items/bazaar/tags"
HISTORY_URL = "https://sky.coflnet.com/api/bazaar/{}/history?end=2025-01-11T02%3A00%3A06.152"

# Step 1: Fetch all item IDs
print("Fetching Bazaar item list...")
response = requests.get(TAGS_URL)
if response.status_code != 200:
    raise Exception(f"Failed to fetch item tags: {response.text}")

item_ids = response.json()
print(f"Found {len(item_ids)} items to process.\n")

# Step 2: Collect historical data
data = []

print("Fetching historical data...")
for item in tqdm(item_ids, desc="Processing Items", ncols=80):
    url = HISTORY_URL.format(item)
    resp = requests.get(url)

    if resp.status_code == 200:
        history = resp.json()
        
        for entry in history:
            data.append({
                "timestamp": entry.get("timestamp", "N/A"),
                "item_id": item,
                "max_buy": entry.get("maxBuy", 0),
                "max_sell": entry.get("maxSell", 0),
                "min_buy": entry.get("minBuy", 0),
                "min_sell": entry.get("minSell", 0),
                "buy": entry.get("buy", 0),
                "sell": entry.get("sell", 0),
                "buy_volume": entry.get("buyVolume", 0),
                "sell_volume": entry.get("sellVolume", 0)
            })
    
      # To avoid rate limits

# Step 3: Convert to Pandas DataFrame and save as CSV
df = pd.DataFrame(data)
df.to_csv("bazaar_historical.csv", index=False)

print("\nHistorical data saved as **bazaar_historical.csv**")
print(f"Total entries collected: {len(df)}")
print("Process complete!")
