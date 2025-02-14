from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from ..config.settings import DATABASE_URL

Base = declarative_base()

class BazaarItem(Base):
    __tablename__ = "bazaar_items"
    
    id = Column(Integer, primary_key=True)
    item_id = Column(String, index=True)
    timestamp = Column(DateTime, index=True, default=datetime.utcnow)
    buy_price = Column(Float)  # Regular buy price
    sell_price = Column(Float)  # Regular sell price
    max_buy = Column(Float)    # Maximum buy price
    max_sell = Column(Float)   # Maximum sell price
    min_buy = Column(Float)    # Minimum buy price
    min_sell = Column(Float)   # Minimum sell price
    buy_volume = Column(Integer)
    sell_volume = Column(Integer)
    
    def __repr__(self):
        return f"<BazaarItem(item_id={self.item_id}, timestamp={self.timestamp})>"

# Create database engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """Initialize the database by creating all tables."""
    Base.metadata.create_all(engine) 