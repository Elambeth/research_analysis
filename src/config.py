# config.py
"""Configuration settings for the supplement analyzer"""
import os
from pathlib import Path
from typing import Optional
import dotenv

# Load environment variables
dotenv.load_dotenv()

# API Configuration
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

NUM_WORKERS = 2                    # Reduced from 5 for testing
TASKS_PER_WORKER = 3              # Reduced from 6 for testing
MAX_CONNECTIONS_PER_WORKER = 3    # Reduced from 6
TOTAL_MAX_CONNECTIONS = 10        # Reduced from 30

# Batch Configuration
FETCH_BATCH_SIZE = 50             # Reduced from 500 - THIS IS THE KEY CHANGE
LOCAL_SAVE_BATCH_SIZE = 25        # Reduced from 1000
UPLOAD_BATCH_SIZE = 10            # Reduced from 100

# Optional: Add a test limit
TEST_MODE = True                  # Add this line
MAX_PAPERS_TEST = 50              # Add this line - total papers to process in test

""" # THESE ARE THE DEFAULT CONFIGURATIONS, YOU CAN CHANGE THEM AS NEEDED
# Processing Configuration
NUM_WORKERS = 5                    # Number of worker processes
TASKS_PER_WORKER = 6              # Async tasks per worker
MAX_CONNECTIONS_PER_WORKER = 6    # DB connections per worker
TOTAL_MAX_CONNECTIONS = 30        # Total DB connections

# Batch Configuration
FETCH_BATCH_SIZE = 50           # Papers to fetch per batch --- CHANGE THIS TO 500 AFTER TESTING
LOCAL_SAVE_BATCH_SIZE = 1000     # Papers per local file
UPLOAD_BATCH_SIZE = 100          # Papers per upload batch
"""
# Retry Configuration
API_MAX_RETRIES = 3
API_RETRY_DELAY = 2
API_RETRY_BACKOFF = 2
DB_MAX_RETRIES = 3
DB_RETRY_DELAY = 1

# Timeouts
API_TIMEOUT = 30  # seconds
DB_TIMEOUT = 10   # seconds

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
COMPLETED_DIR = DATA_DIR / "completed"
FAILED_DIR = DATA_DIR / "failed"
PROGRESS_DIR = DATA_DIR / "progress"
QUEUE_DIR = DATA_DIR / "queue"
LOGS_DIR = BASE_DIR / "logs"

# Create directories
for dir_path in [COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR, QUEUE_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Progress tracking
CHECKPOINT_FILE = PROGRESS_DIR / "checkpoint.json"
PROGRESS_UPDATE_INTERVAL = 10  # Update progress every N papers

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"

# Analysis Configuration
PROMPT_VERSION = "1.1"
TEMPERATURE = 0.1

# Safety thresholds
MAX_MEMORY_PERCENT = 80  # Stop if memory usage exceeds this
MAX_ERROR_RATE = 0.1     # Stop if error rate exceeds this