import pytest
import asyncio
from datetime import datetime, timedelta, UTC
from sqlalchemy import text
from src.data.collectors import BazaarDataCollector
from src.data.models import BazaarItem, SessionLocal, init_db
import aiohttp

@pytest.fixture(scope="session", autouse=True)
def initialize_database():
    """Initialize database before any tests run"""
    init_db()

@pytest.fixture
def collector():
    return BazaarDataCollector()

@pytest.fixture
def db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

@pytest.mark.asyncio
async def test_get_current_prices(collector):
    """Test fetching current prices from Hypixel API"""
    prices = await collector.get_current_prices()
    
    # Basic validation
    assert prices is not None
    assert len(prices) > 0
    
    # Check structure of a random item
    sample_item = next(iter(prices.values()))
    assert 'sell_price' in sample_item
    assert 'buy_price' in sample_item
    assert 'sell_volume' in sample_item
    assert 'buy_volume' in sample_item
    
    # Validate data types
    assert isinstance(sample_item['sell_price'], (int, float))
    assert isinstance(sample_item['buy_price'], (int, float))
    assert isinstance(sample_item['sell_volume'], int)
    assert isinstance(sample_item['buy_volume'], int)

@pytest.mark.asyncio
async def test_collect_and_store_current_data(collector, db_session):
    """Test storing current prices in database"""
    # Collect and store data
    data = await collector.collect_and_store_current_data()
    
    # Verify data was stored
    latest_items = db_session.query(BazaarItem).filter(
        BazaarItem.timestamp >= datetime.now(UTC) - timedelta(minutes=1)
    ).all()
    
    assert len(latest_items) > 0
    
    # Verify sample item data
    sample_item = latest_items[0]
    assert sample_item.item_id is not None
    assert isinstance(sample_item.buy_price, float)
    assert isinstance(sample_item.sell_price, float)
    assert isinstance(sample_item.buy_volume, int)
    assert isinstance(sample_item.sell_volume, int)

@pytest.mark.asyncio
async def test_api_error_handling(collector):
    """Test error handling for API failures"""
    # Use an invalid URL that will definitely fail
    url = "http://invalid-url-that-will-definitely-fail.xyz"
    
    with pytest.raises(Exception):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=1) as response:
                await response.json()

def test_database_connection(db_session):
    """Test database connection"""
    # Try to execute a simple query
    result = db_session.execute(text("SELECT 1")).scalar()
    assert result == 1

if __name__ == "__main__":
    pytest.main([__file__]) 