import pandas as pd
import numpy as np

import sys
import os

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import plotly.graph_objects as go
from typing import List, Dict

from src.config.settings import DATABASE_URL
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MarketAnalyzer:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        
    def get_high_volume_items(self, min_volume: int = 1000) -> pd.DataFrame:
        """Find items with consistently high trading volume."""
        query = text("""
        SELECT item_id,
               AVG(buy_volume) as avg_buy_volume,
               AVG(sell_volume) as avg_sell_volume,
               AVG(buy_price) as avg_buy_price,
               AVG(sell_price) as avg_sell_price
        FROM bazaar_items
        GROUP BY item_id
        HAVING AVG(buy_volume) > :min_volume OR AVG(sell_volume) > :min_volume
        ORDER BY (AVG(buy_volume) + AVG(sell_volume)) DESC
        LIMIT 20
        """)
        return pd.read_sql(query, self.engine, params={'min_volume': min_volume})

    def calculate_volatility(self, lookback_days: int = 7) -> pd.DataFrame:
        """Calculate price volatility for all items."""
        query = text("""
        WITH price_changes AS (
            SELECT 
                item_id,
                timestamp,
                buy_price,
                sell_price,
                LAG(buy_price) OVER (PARTITION BY item_id ORDER BY timestamp) as prev_buy,
                LAG(sell_price) OVER (PARTITION BY item_id ORDER BY timestamp) as prev_sell
            FROM bazaar_items
            WHERE timestamp >= NOW() - INTERVAL :lookback_days
        )
        SELECT 
            item_id,
            COUNT(*) as data_points,
            STDDEV(NULLIF(((buy_price - prev_buy) / NULLIF(prev_buy, 0)) * 100, 0)) as buy_volatility,
            STDDEV(NULLIF(((sell_price - prev_sell) / NULLIF(prev_sell, 0)) * 100, 0)) as sell_volatility,
            AVG(buy_price) as avg_buy_price,
            AVG(sell_price) as avg_sell_price
        FROM price_changes
        GROUP BY item_id
        HAVING COUNT(*) > 10
        ORDER BY (COALESCE(STDDEV(NULLIF(((buy_price - prev_buy) / NULLIF(prev_buy, 0)) * 100, 0)), 0) + 
                  COALESCE(STDDEV(NULLIF(((sell_price - prev_sell) / NULLIF(prev_sell, 0)) * 100, 0)), 0)) DESC
        LIMIT 20
        """)
        return pd.read_sql(query, self.engine, params={'lookback_days': f'{lookback_days} days'})

    def find_trading_opportunities(self) -> pd.DataFrame:
        """Find items with good trading opportunities based on price patterns."""
        query = text("""
        WITH price_stats AS (
            SELECT 
                item_id,
                AVG(buy_price) as avg_buy,
                AVG(sell_price) as avg_sell,
                MIN(buy_price) as min_buy,
                MAX(sell_price) as max_sell,
                STDDEV(buy_price) as std_buy,
                STDDEV(sell_price) as std_sell,
                AVG(buy_volume) as avg_buy_volume,
                AVG(sell_volume) as avg_sell_volume
            FROM bazaar_items
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY item_id
        )
        SELECT 
            item_id,
            avg_buy,
            avg_sell,
            ((max_sell - min_buy) / NULLIF(min_buy, 0)) * 100 as potential_profit_percent,
            avg_buy_volume,
            avg_sell_volume,
            std_buy / NULLIF(avg_buy, 0) * 100 as buy_cv,
            std_sell / NULLIF(avg_sell, 0) * 100 as sell_cv
        FROM price_stats
        WHERE avg_buy_volume > 1000 AND avg_sell_volume > 1000
        ORDER BY potential_profit_percent DESC
        LIMIT 20
        """)
        return pd.read_sql(query, self.engine)

    def plot_price_history(self, item_id: str, days: int = 7):
        """Plot price and volume history for a specific item."""
        query = text("""
        SELECT timestamp, buy_price, sell_price, buy_volume, sell_volume
        FROM bazaar_items
        WHERE item_id = :item_id
        AND timestamp >= NOW() - INTERVAL ':days days'
        ORDER BY timestamp
        """)
        df = pd.read_sql(query, self.engine, params={'item_id': item_id, 'days': days})
        
        # Create figure with secondary y-axis
        fig = go.Figure()

        # Add price lines
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['buy_price'],
                      name="Buy Price", line=dict(color='blue'))
        )
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['sell_price'],
                      name="Sell Price", line=dict(color='red'))
        )

        # Add volume bars
        fig.add_trace(
            go.Bar(x=df['timestamp'], y=df['buy_volume'],
                  name="Buy Volume", opacity=0.3, marker_color='blue')
        )
        fig.add_trace(
            go.Bar(x=df['timestamp'], y=df['sell_volume'],
                  name="Sell Volume", opacity=0.3, marker_color='red')
        )

        # Update layout
        fig.update_layout(
            title=f'Price and Volume History for {item_id}',
            xaxis_title='Time',
            yaxis_title='Price',
            barmode='group',
            hovermode='x unified'
        )
        
        return fig

    def analyze_market(self):
        """Perform comprehensive market analysis."""
        logger.info("Starting market analysis...")
        
        # Get high volume items
        high_volume = self.get_high_volume_items()
        logger.info("\nTop items by trading volume:")
        print(high_volume)
        
        # Get volatile items
        volatile = self.calculate_volatility()
        logger.info("\nMost volatile items:")
        print(volatile)
        
        # Get trading opportunities
        opportunities = self.find_trading_opportunities()
        logger.info("\nBest trading opportunities:")
        print(opportunities)
        
        # Return combined analysis
        return {
            'high_volume_items': high_volume,
            'volatile_items': volatile,
            'trading_opportunities': opportunities
        }

if __name__ == "__main__":
    analyzer = MarketAnalyzer()
    analysis = analyzer.analyze_market()
    
    # Plot top 5 trading opportunities
    for item_id in analysis['trading_opportunities']['item_id'].head():
        fig = analyzer.plot_price_history(item_id)
        fig.show() 