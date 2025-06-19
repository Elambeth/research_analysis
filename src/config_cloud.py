# src/config_cloud.py - Production Cloud Configuration
import os
from pathlib import Path
import dotenv

dotenv.load_dotenv()

# API Configuration
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") 
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

# CLOUD OPTIMIZED SETTINGS
NUM_WORKERS = 12                   # Optimized for cloud
TASKS_PER_WORKER = 8              # High concurrency
MAX_CONNECTIONS_PER_WORKER = 8
TOTAL_MAX_CONNECTIONS = 96

# Batch sizes optimized for cloud
FETCH_BATCH_SIZE = 300            # Larger batches
LOCAL_SAVE_BATCH_SIZE = 1000      
UPLOAD_BATCH_SIZE = 100

# PROCESS ALL PAPERS
TEST_MODE = False                 
MAX_PAPERS_TEST = None
# Cloud-optimized retry settings
API_MAX_RETRIES = 5
API_RETRY_DELAY = 1
API_RETRY_BACKOFF = 1.5
DB_MAX_RETRIES = 5
DB_RETRY_DELAY = 0.5

# Timeouts
API_TIMEOUT = 60
DB_TIMEOUT = 30

# Paths (same as before)
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
COMPLETED_DIR = DATA_DIR / "completed"
FAILED_DIR = DATA_DIR / "failed"
PROGRESS_DIR = DATA_DIR / "progress"
QUEUE_DIR = DATA_DIR / "queue" 
LOGS_DIR = BASE_DIR / "logs"

for dir_path in [COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR, QUEUE_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = PROGRESS_DIR / "checkpoint.json"
PROGRESS_UPDATE_INTERVAL = 5

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"

# Analysis settings
PROMPT_VERSION = "1.1"
TEMPERATURE = 0.1

# Cloud safety thresholds
MAX_MEMORY_PERCENT = 90
MAX_ERROR_RATE = 0.15