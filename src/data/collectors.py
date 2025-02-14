import requests
from datetime import datetime
import time
from typing import Dict, List, Optional
from tqdm import tqdm

from ..utils.logger import get_logger
from ..config.settings import COFLNET_API_BASE_URL, COFLNET_API_KEY
from .models import SessionLocal, BazaarItem

logger = get_logger(__name__)

class BazaarDataCollector:
    def __init__(self):
        self.base_url = COFLNET_API_BASE_URL
        self.api_key = COFLNET_API_KEY
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def get_all_item_ids(self) -> List[str]:
        """Fetch all available bazaar item IDs."""
        try:
            response = requests.get(
                f"{self.base_url}/items/bazaar/tags",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching item IDs: {e}")
            raise

    def get_current_prices(self) -> Dict:
        """Fetch current bazaar prices for all items."""
        try:
            response = requests.get(
                f"{self.base_url}/bazaar/snapshot",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching current prices: {e}")
            raise
    
    def get_item_history(
        self,
        item_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict]:
        """Fetch historical data for a specific item."""
        try:
            params = {}
            if start_time:
                params["from"] = int(start_time.timestamp() * 1000)
            if end_time:
                params["to"] = int(end_time.timestamp() * 1000)
            
            response = requests.get(
                f"{self.base_url}/bazaar/{item_id}/history",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching history for item {item_id}: {e}")
            raise

    def collect_full_history(self, rate_limit_delay: float = 0.5) -> List[Dict]:
        """
        Collect complete historical data for all bazaar items.
        
        Args:
            rate_limit_delay: Time to wait between requests in seconds
        
        Returns:
            List of historical data entries
        """
        try:
            # Get all item IDs
            logger.info("Fetching bazaar item list...")
            item_ids = self.get_all_item_ids()
            logger.info(f"Found {len(item_ids)} items to process")
            
            # Collect historical data
            all_data = []
            
            for item in tqdm(item_ids, desc="Processing Items", ncols=80):
                try:
                    history = self.get_item_history(item)
                    
                    for entry in history:
                        all_data.append({
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
                    
                    # Rate limiting
                    time.sleep(rate_limit_delay)
                    
                except Exception as e:
                    logger.error(f"Error processing item {item}: {e}")
                    continue
            
            logger.info(f"Successfully collected {len(all_data)} historical entries")
            return all_data
            
        except Exception as e:
            logger.error(f"Error in collect_full_history: {e}")
            raise

    def collect_and_store_current_data(self):
        """Collect current bazaar data and store it in the database."""
        try:
            data = self.get_current_prices()
            
            # Create database session
            db = SessionLocal()
            try:
                # Process and store each item
                for item_id, item_data in data.items():
                    bazaar_item = BazaarItem(
                        item_id=item_id,
                        timestamp=datetime.utcnow(),
                        buy_price=item_data.get('buy', 0),
                        sell_price=item_data.get('sell', 0),
                        max_buy=item_data.get('maxBuy', 0),
                        max_sell=item_data.get('maxSell', 0),
                        min_buy=item_data.get('minBuy', 0),
                        min_sell=item_data.get('minSell', 0),
                        buy_volume=item_data.get('buyVolume', 0),
                        sell_volume=item_data.get('sellVolume', 0)
                    )
                    db.add(bazaar_item)
                
                # Commit the transaction
                db.commit()
                logger.info(f"Successfully stored data for {len(data)} items")
            finally:
                db.close()
            
            return data
        except Exception as e:
            logger.error(f"Error in collect_and_store_current_data: {e}")
            raise

    def store_historical_data(self, historical_data: List[Dict]):
        """Store historical data in the database."""
        try:
            db = SessionLocal()
            try:
                for entry in tqdm(historical_data, desc="Storing historical data", ncols=80):
                    bazaar_item = BazaarItem(
                        item_id=entry['item_id'],
                        timestamp=datetime.fromtimestamp(entry['timestamp'] / 1000),  # Convert from milliseconds
                        buy_price=entry['buy'],
                        sell_price=entry['sell'],
                        max_buy=entry['max_buy'],
                        max_sell=entry['max_sell'],
                        min_buy=entry['min_buy'],
                        min_sell=entry['min_sell'],
                        buy_volume=entry['buy_volume'],
                        sell_volume=entry['sell_volume']
                    )
                    db.add(bazaar_item)
                
                db.commit()
                logger.info(f"Successfully stored {len(historical_data)} historical entries")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Error storing historical data: {e}")
            raise 