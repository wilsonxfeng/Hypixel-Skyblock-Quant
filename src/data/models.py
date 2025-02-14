from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from ..config.settings import DATABASE_URL

Base = declarative_base()

class BazaarItem(Base):
    __tablename__ = "bazaar_items"
    
    id = Column(Integer, primary_key=True)
    item_id = Column(String)
    timestamp = Column(DateTime)
    buy_price = Column(Float)
    sell_price = Column(Float)
    buy_volume = Column(Integer)
    sell_volume = Column(Integer)
    buy_moving_week = Column(Integer)
    sell_moving_week = Column(Integer)
    
    def __repr__(self):
        return f"<BazaarItem(item_id={self.item_id}, timestamp={self.timestamp})>"

# Create database engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """Initialize the database by creating all tables."""
    Base.metadata.create_all(engine) 