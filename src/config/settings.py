import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Settings
COFLNET_API_BASE_URL = "https://sky.coflnet.com/api"
COFLNET_API_KEY = os.getenv("COFLNET_API_KEY")

# Database Settings
DATABASE_URL = os.getenv('DATABASE_URL')

# Data Collection Settings
COLLECTION_INTERVAL =  3 # 5 minutes in seconds 