supplement_analyzer/
├── src/
│   ├── __init__.py
│   ├── config.py          # Configuration settings
│   ├── models.py          # Data models
│   ├── database.py        # Database operations
│   ├── storage.py         # Local file storage
│   ├── processor.py       # DeepSeek API processing
│   ├── worker.py          # Worker process logic
│   ├── coordinator.py     # Main coordinator
│   └── uploader.py        # Upload results to Supabase
├── data/
│   ├── completed/         # Completed analyses
│   ├── failed/           # Failed papers
│   ├── progress/         # Progress tracking
│   └── queue/            # Work queue
├── logs/                 # Log files
├── main.py              # Entry point for processing
├── upload.py            # Entry point for uploading
├── requirements.txt     # Dependencies
└── .env                 # Environment variables