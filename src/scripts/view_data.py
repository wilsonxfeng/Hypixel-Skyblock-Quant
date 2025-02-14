import sys
import os

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from src.data.models import BazaarItem, DATABASE_URL

# Create a new database session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
db = SessionLocal()

def view_data():
    try:
        # Query all items in the bazaar_items table
        items = db.query(BazaarItem).all()
        
        # Print each item
        for item in items:
            print(f"Item ID: {item.item_id}")
            print(f"Timestamp: {item.timestamp}")
            print(f"Buy Price: {item.buy_price}")
            print(f"Sell Price: {item.sell_price}")
            print(f"Buy Volume: {item.buy_volume}")
            print(f"Sell Volume: {item.sell_volume}")
            print(f"Max Buy: {item.max_buy}")
            print(f"Max Sell: {item.max_sell}")
            print(f"Min Buy: {item.min_buy}")
            print(f"Min Sell: {item.min_sell}")
            print("-" * 40)
    finally:
        db.close()

if __name__ == "__main__":
    view_data() 