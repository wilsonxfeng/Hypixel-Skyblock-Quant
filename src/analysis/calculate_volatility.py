import sys
import os

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from src.data.models import BazaarItem, DATABASE_URL

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Create a new database session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
db = SessionLocal()

def calculate_buy_volatility(item_id: str, days: int = 7):
    try:
        # Fetch buy prices for the specified item over the last 'days' days
        query = text("""
            SELECT buy_price
            FROM bazaar_items
            WHERE item_id = :item_id AND timestamp >= NOW() - INTERVAL :days
        """)
        result = db.execute(query, {'item_id': item_id, 'days': f'{days} days'}).fetchall()
        
        # Extract buy prices into a list
        buy_prices = [row.buy_price for row in result if row.buy_price is not None]
        
        # Check if there are enough data points to calculate volatility
        if len(buy_prices) < 2:
            print(f"Not enough data to calculate volatility for {item_id} over the last {days} days.")
            return
        
        # Calculate the standard deviation of buy prices
        volatility = np.std(buy_prices)
        print(f"Buy Volatility for {item_id} over the last {days} days: {volatility}")
    except Exception as e:
        print(f"An error occurred while calculating volatility: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    item_id = 'INK_SACK:3'  # Replace with the desired item ID
    calculate_buy_volatility(item_id) 