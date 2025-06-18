# uploader.py
"""Upload analyzed results back to Supabase"""
import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import sys

from src.config_cloud import COMPLETED_DIR, UPLOAD_BATCH_SIZE
from src.database import DatabaseManager
from src.models import AnalysisResult
from src.storage import StorageManager

logger = logging.getLogger(__name__)


class Uploader:
    """Handles uploading results to Supabase"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.storage = StorageManager()
        self.total_uploaded = 0
        self.failed_uploads = 0
    
    def _load_results_from_file(self, filepath: Path) -> List[AnalysisResult]:
        """Load results from a JSONL file"""
        results = []
        
        try:
            with open(filepath, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        data = json.loads(line.strip())
                        result = AnalysisResult.from_dict(data)
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error parsing line {line_num} in {filepath}: {e}")
                        self.failed_uploads += 1
        
        except Exception as e:
            logger.error(f"Error reading file {filepath}: {e}")
        
        return results
    
    async def upload_batch(self, results: List[AnalysisResult]) -> bool:
        """Upload a batch of results to Supabase"""
        # Convert to update format
        updates = []
        for result in results:
            update_data = result.to_dict()
            # Remove supplement_id as it's not needed for update
            update_data.pop('supplement_id', None)
            updates.append(update_data)
        
        # Attempt upload
        success = await self.db.batch_update_studies(updates)
        
        if success:
            self.total_uploaded += len(results)
            logger.info(f"Successfully uploaded batch of {len(results)} results")
        else:
            self.failed_uploads += len(results)
            logger.error(f"Failed to upload batch of {len(results)} results")
        
        return success
    
    async def upload_file(self, filepath: Path) -> Dict[str, Any]:
        """Upload all results from a single file"""
        logger.info(f"Processing file: {filepath}")
        
        # Load results
        results = self._load_results_from_file(filepath)
        if not results:
            logger.warning(f"No valid results found in {filepath}")
            return {'file': filepath.name, 'uploaded': 0, 'failed': 0}
        
        logger.info(f"Loaded {len(results)} results from {filepath}")
        
        # Upload in batches
        uploaded = 0
        failed = 0
        
        for i in range(0, len(results), UPLOAD_BATCH_SIZE):
            batch = results[i:i + UPLOAD_BATCH_SIZE]
            
            if await self.upload_batch(batch):
                uploaded += len(batch)
            else:
                failed += len(batch)
                # Optionally save failed batch for retry
                failed_path = filepath.parent / "failed" / f"failed_{filepath.name}"
                failed_path.parent.mkdir(exist_ok=True)
                
                with open(failed_path, 'a') as f:
                    for result in batch:
                        f.write(result.to_json_line() + '\n')
            
            # Small delay between batches
            await asyncio.sleep(0.5)
        
        return {
            'file': filepath.name,
            'uploaded': uploaded,
            'failed': failed
        }
    
    async def run(self, specific_files: Optional[List[str]] = None):
        """Main upload process"""
        logger.info("Starting upload process...")
        start_time = datetime.now()
        
        # Get files to upload
        if specific_files:
            files = [COMPLETED_DIR / f for f in specific_files if (COMPLETED_DIR / f).exists()]
        else:
            files = sorted(self.storage.get_completed_batches())
        
        if not files:
            logger.warning("No files found to upload")
            return
        
        logger.info(f"Found {len(files)} files to upload")
        
        # Process each file
        results = []
        for file_path in files:
            result = await self.upload_file(file_path)
            results.append(result)
            
            # Display progress
            logger.info(
                f"Progress: {len(results)}/{len(files)} files | "
                f"Total uploaded: {self.total_uploaded} | "
                f"Failed: {self.failed_uploads}"
            )
        
        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("\n" + "="*50)
        logger.info("UPLOAD COMPLETE")
        logger.info("="*50)
        logger.info(f"Files processed: {len(files)}")
        logger.info(f"Total uploaded: {self.total_uploaded}")
        logger.info(f"Failed uploads: {self.failed_uploads}")
        logger.info(f"Success rate: {self.total_uploaded/(self.total_uploaded+self.failed_uploads)*100:.1f}%")
        logger.info(f"Time elapsed: {elapsed/60:.1f} minutes")
        logger.info(f"Upload speed: {self.total_uploaded/elapsed:.1f} records/second")
        
        # Close database connection
        self.db.close()


async def uploader_main(specific_files: Optional[List[str]] = None):
    """Main entry point for uploader"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Uploader - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/uploader.log"),
            logging.StreamHandler()
        ]
    )
    
    uploader = Uploader()
    
    try:
        await uploader.run(specific_files)
    except Exception as e:
        logger.error(f"Fatal error in uploader: {e}")
        sys.exit(1)


def run_uploader(specific_files: Optional[List[str]] = None):
    """Entry point for the uploader"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(uploader_main(specific_files))