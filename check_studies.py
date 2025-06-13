# check_studies.py - Check what studies we're trying to process
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database import DatabaseManager
from src.config import FETCH_BATCH_SIZE

async def check_studies():
    """Check the first batch of studies"""
    db = DatabaseManager()
    
    print("Fetching first batch of studies...")
    studies = await db.fetch_unanalyzed_studies(limit=5)  # Just get 5 for checking
    
    print(f"\nFound {len(studies)} studies:")
    
    for i, study in enumerate(studies):
        print(f"\n--- Study {i+1} ---")
        print(f"ID: {study.id}")
        print(f"Supplement: {study.supplement_name} (ID: {study.supplement_id})")
        print(f"PMID: {study.pmid}")
        print(f"Title: {study.title[:100] if study.title else 'No title'}...")
        print(f"Abstract: {'Yes' if study.abstract else 'NO ABSTRACT!'} "
              f"({len(study.abstract) if study.abstract else 0} chars)")
        
        if not study.abstract:
            print("WARNING: This study has no abstract and will fail!")
    
    db.close()

if __name__ == "__main__":
    asyncio.run(check_studies())