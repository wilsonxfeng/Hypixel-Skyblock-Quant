import sys
import os
import asyncio
from datetime import datetime
import signal

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.data.collectors import BazaarDataCollector
from src.utils.logger import get_logger
from src.config.settings import COLLECTION_INTERVAL

logger = get_logger(__name__)

class DataCollectionService:
    def __init__(self):
        self.collector = BazaarDataCollector()
        self.is_running = False
        
    async def collect_data(self):
        """Collect and store current bazaar data."""
        try:
            await self.collector.collect_and_store_current_data()
            logger.info(f"Data collection completed at {datetime.now()}")
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
    
    async def run(self):
        """Run the continuous data collection loop."""
        self.is_running = True
        
        # Set up signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self.handle_shutdown)
            
        logger.info("Starting bazaar data collection service...")
        
        while self.is_running:
            await self.collect_data()
            
            # Wait for the next collection interval
            if COLLECTION_INTERVAL > 0:
                logger.info(f"Waiting {COLLECTION_INTERVAL} seconds until next collection...")
                await asyncio.sleep(COLLECTION_INTERVAL)
            else:
                self.is_running = False
    
    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Received shutdown signal. Stopping data collection...")
        self.is_running = False

async def main():
    service = DataCollectionService()
    await service.run()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main()) 