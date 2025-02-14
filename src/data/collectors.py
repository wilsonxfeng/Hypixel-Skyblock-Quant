import requests
from datetime import datetime
import time
from typing import Dict, List, Optional
from tqdm import tqdm
import aiohttp
from dateutil import parser

from ..utils.logger import get_logger
from ..config.settings import COFLNET_API_BASE_URL
from src.data.models import SessionLocal, BazaarItem

logger = get_logger(__name__)

class BazaarDataCollector:
    def __init__(self):
        self.base_url = COFLNET_API_BASE_URL
    
    def get_all_item_ids(self) -> List[str]:
        """Fetch all available bazaar item IDs."""
        try:
            response = requests.get(
                f"{self.base_url}/items/bazaar/tags"
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching item IDs: {e}")
            raise

    async def get_current_prices(self) -> Dict[str, Dict[str, float]]:
        """
        Fetches current bazaar prices from Hypixel API.
        Returns a dictionary mapping item IDs to their price data.
        """
        url = "https://api.hypixel.net/v2/skyblock/bazaar"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to fetch bazaar data: {response.status}")
                    
                    data = await response.json()
                    
                    if not data.get('success'):
                        raise Exception("API request was not successful")
                    
                    prices = {}
                    for product_id, product_data in data['products'].items():
                        quick_status = product_data.get('quick_status', {})
                        if quick_status:
                            prices[product_id] = {
                                'sell_price': quick_status.get('sellPrice', 0.0),
                                'sell_volume': quick_status.get('sellVolume', 0),
                                'sell_moving_week': quick_status.get('sellMovingWeek', 0),
                                'buy_price': quick_status.get('buyPrice', 0.0),
                                'buy_volume': quick_status.get('buyVolume', 0),
                                'buy_moving_week': quick_status.get('buyMovingWeek', 0)
                            }
                    
                    return prices
        except Exception as e:
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
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Debugging: Print the data structure
            logger.debug(f"Data for {item_id}: {data}")
            
            # Check if 'item_id' is included in each entry
            for entry in data:
                if 'item_id' not in entry:
                    entry['item_id'] = item_id  # Add item_id if missing
            
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching history for item {item_id}: {e}")
            raise

    def collect_full_history(self, rate_limit_delay: float = 0) -> List[Dict]:
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

    async def collect_and_store_current_data(self):
        """Collect current bazaar data and store it in the database."""
        try:
            data = await self.get_current_prices()
            
            # Create database session
            db = SessionLocal()
            try:
                # Process and store each item
                for item_id, item_data in data.items():
                    bazaar_item = BazaarItem(
                        item_id=item_id,
                        timestamp=datetime.utcnow(),
                        buy_price=item_data.get('buy_price', 0),
                        sell_price=item_data.get('sell_price', 0),
                        max_buy=item_data.get('max_buy', 0),
                        max_sell=item_data.get('max_sell', 0),
                        min_buy=item_data.get('min_buy', 0),
                        min_sell=item_data.get('min_sell', 0),
                        buy_volume=item_data.get('buy_volume', 0),
                        sell_volume=item_data.get('sell_volume', 0)
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

    def store_historical_data(self, historical_data: List[Dict], item_id: str):
        """Store historical data in the database."""
        try:
            db = SessionLocal()
            try:
                count = 0
                for entry in tqdm(historical_data, desc="Storing historical data", ncols=80):
                    try:
                        # Parse the ISO 8601 timestamp or use current time if not available
                        timestamp = parser.isoparse(entry.get('timestamp', datetime.utcnow().isoformat()))
                        
                        bazaar_item = BazaarItem(
                            item_id=item_id,
                            timestamp=timestamp,
                            buy_price=float(entry.get('buy', entry.get('buyPrice', 0))),
                            sell_price=float(entry.get('sell', entry.get('sellPrice', 0))),
                            buy_volume=int(entry.get('buyVolume', 0)),
                            sell_volume=int(entry.get('sellVolume', 0)),
                            buy_moving_week=int(entry.get('buyMovingWeek', 0)),
                            sell_moving_week=int(entry.get('sellMovingWeek', 0))
                        )
                        db.add(bazaar_item)
                        count += 1
                    except KeyError as e:
                        logger.error(f"Missing key {e} in entry: {entry}")
                        continue
                    except ValueError as e:
                        logger.error(f"Value error {e} in entry: {entry}")
                        continue
                
                db.commit()
                logger.info(f"Successfully stored {count} historical entries for item {item_id}")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Error storing historical data: {e}")
            raise 