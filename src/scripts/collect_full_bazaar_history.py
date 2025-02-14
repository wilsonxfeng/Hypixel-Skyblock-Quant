import asyncio
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data.collectors import BazaarDataCollector

async def collect_full_bazaar_history():
    collector = BazaarDataCollector()
    
    try:
        # Fetch all item IDs
        item_ids = collector.get_all_item_ids()
        print(f"Found {len(item_ids)} items to process.")
        
        # Collect and store historical data for each item
        for item_id in item_ids:
            try:
                print(f"Processing item: {item_id}")
                historical_data = collector.get_item_history(item_id)
                collector.store_historical_data(historical_data, item_id)
                print(f"Successfully stored history for item: {item_id}")
            except Exception as e:
                print(f"Error processing item {item_id}: {e}")
    except Exception as e:
        print(f"Error in collecting full bazaar history: {e}")

if __name__ == "__main__":
    asyncio.run(collect_full_bazaar_history()) 