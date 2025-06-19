# storage.py - MODIFIED FOR CONTINUOUS CLOUD STORAGE UPLOADS
"""Local storage manager with continuous cloud uploads"""
import json
import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import aiofiles
import logging
from threading import Lock
from google.cloud import storage
import os

from src.config_cloud import (
    COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR, CHECKPOINT_FILE,
    LOCAL_SAVE_BATCH_SIZE
)
from src.models import AnalysisResult, Progress, FailedPaper, Study

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages local file storage with continuous cloud uploads"""
    
    def __init__(self):
        self.current_batch = []
        self.current_batch_num = 1
        self.batch_lock = asyncio.Lock()
        self.failed_lock = asyncio.Lock()
        self.cloud_exporter = CloudStorageExporter()
        self.uploaded_batches = set()  # Track what's been uploaded
        self._ensure_directories()
        self._load_uploaded_batches()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        for dir_path in [COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def _load_uploaded_batches(self):
        """Load list of already uploaded batches for recovery"""
        uploaded_file = PROGRESS_DIR / "uploaded_batches.json"
        if uploaded_file.exists():
            try:
                with open(uploaded_file, 'r') as f:
                    data = json.load(f)
                    self.uploaded_batches = set(data.get('uploaded_batches', []))
                    self.current_batch_num = data.get('next_batch_num', 1)
                logger.info(f"Loaded upload checkpoint: {len(self.uploaded_batches)} batches already uploaded")
            except Exception as e:
                logger.error(f"Failed to load upload checkpoint: {e}")
    
    def _save_uploaded_batches(self):
        """Save list of uploaded batches for recovery"""
        uploaded_file = PROGRESS_DIR / "uploaded_batches.json"
        try:
            data = {
                'uploaded_batches': list(self.uploaded_batches),
                'next_batch_num': self.current_batch_num,
                'last_update': datetime.now().isoformat()
            }
            with open(uploaded_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save upload checkpoint: {e}")
    
    def _get_batch_filename(self, batch_num: int) -> Path:
        """Get filename for a batch"""
        return COMPLETED_DIR / f"batch_{batch_num:06d}.jsonl"
    
    async def save_result(self, result: AnalysisResult) -> None:
        """Save a single analysis result"""
        async with self.batch_lock:
            self.current_batch.append(result)
            
            # Check if we need to flush the batch
            if len(self.current_batch) >= LOCAL_SAVE_BATCH_SIZE:
                await self._flush_batch()
    
    async def _flush_batch(self) -> None:
        """Flush current batch to disk AND upload to cloud storage"""
        if not self.current_batch:
            return
        
        filename = self._get_batch_filename(self.current_batch_num)
        
        try:
            # 1. Save locally first
            async with aiofiles.open(filename, 'w') as f:
                for result in self.current_batch:
                    await f.write(result.to_json_line() + '\n')
            
            logger.info(f"✅ Saved batch {self.current_batch_num} locally with {len(self.current_batch)} results")
            
            # 2. IMMEDIATELY upload to Cloud Storage
            success = await self.cloud_exporter.upload_batch_file(filename, self.current_batch_num)
            
            if success:
                # Track successful upload
                self.uploaded_batches.add(self.current_batch_num)
                self._save_uploaded_batches()
                logger.info(f"🚀 Successfully uploaded batch {self.current_batch_num} to Cloud Storage")
                
                # Optional: Remove local file to save space (keep if you want local backup)
                # filename.unlink()  # Uncomment this line to delete local files after upload
            else:
                logger.error(f"❌ Failed to upload batch {self.current_batch_num} to Cloud Storage (kept locally)")
            
            # Reset for next batch
            self.current_batch = []
            self.current_batch_num += 1
            
        except Exception as e:
            logger.error(f"Failed to save/upload batch {self.current_batch_num}: {e}")
            raise
    
    async def save_failed(self, failed_paper: FailedPaper) -> None:
        """Save a failed paper for later retry"""
        filename = FAILED_DIR / f"failed_papers.jsonl"
        
        async with self.failed_lock:
            try:
                async with aiofiles.open(filename, 'a') as f:
                    await f.write(failed_paper.to_json_line() + '\n')
                logger.debug(f"Saved failed paper: {failed_paper.study.id}")
                
                # Also upload failed papers periodically
                await self.cloud_exporter.upload_failed_papers()
                
            except Exception as e:
                logger.error(f"Failed to save failed paper: {e}")
    
    async def finalize(self) -> None:
        """Finalize storage, flush any remaining data"""
        async with self.batch_lock:
            if self.current_batch:
                await self._flush_batch()
        
        # Final upload of any remaining files
        await self.cloud_exporter.upload_final_files()
        
        logger.info(f"🎉 Storage finalized. Total batches uploaded: {len(self.uploaded_batches)}")
    
    def get_completed_batches(self) -> List[Path]:
        """Get list of all completed batch files"""
        return sorted(COMPLETED_DIR.glob("batch_*.jsonl"))
    
    def get_failed_papers(self) -> List[FailedPaper]:
        """Load all failed papers"""
        failed_papers = []
        failed_file = FAILED_DIR / "failed_papers.jsonl"
        
        if not failed_file.exists():
            return failed_papers
        
        with open(failed_file, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                study_data = data['study']
                study = Study(**study_data)
                
                failed_paper = FailedPaper(
                    study=study,
                    error=data['error'],
                    error_type=data['error_type'],
                    attempts=data['attempts'],
                    last_attempt=datetime.fromisoformat(data['last_attempt'])
                )
                failed_papers.append(failed_paper)
        
        return failed_papers


class ProgressTracker:
    """Tracks and persists progress with cloud backup"""
    
    def __init__(self):
        self.progress: Optional[Progress] = None
        self.lock = Lock()
        self.cloud_exporter = CloudStorageExporter()
        self._load_progress()
    
    def _load_progress(self) -> None:
        """Load progress from checkpoint file"""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    data = json.load(f)
                self.progress = Progress.from_dict(data)
                logger.info(f"Loaded progress: {self.progress.processed_papers}/{self.progress.total_papers} papers processed")
            except Exception as e:
                logger.error(f"Failed to load progress: {e}")
                self.progress = None
    
    def initialize(self, total_papers: int) -> None:
        """Initialize progress tracking"""
        with self.lock:
            if self.progress is None:
                self.progress = Progress(
                    total_papers=total_papers,
                    processed_papers=0,
                    failed_papers=0,
                    current_batch_id=0,
                    batches_completed=[],
                    start_time=datetime.now(),
                    last_update=datetime.now(),
                    error_counts={}
                )
                self._save_progress()
    
    def update(self, processed: int = 0, failed: int = 0, error_type: Optional[str] = None) -> None:
        """Update progress"""
        with self.lock:
            if self.progress:
                self.progress.processed_papers += processed
                self.progress.failed_papers += failed
                self.progress.last_update = datetime.now()
                
                if error_type:
                    self.progress.error_counts[error_type] = self.progress.error_counts.get(error_type, 0) + 1
                
                self._save_progress()
                
                # Upload progress to cloud every 100 updates
                if self.progress.processed_papers % 100 == 0:
                    asyncio.create_task(self.cloud_exporter.upload_progress_file())
    
    def complete_batch(self, batch_id: int) -> None:
        """Mark a batch as completed"""
        with self.lock:
            if self.progress:
                self.progress.batches_completed.append(batch_id)
                self.progress.current_batch_id = batch_id + 1
                self._save_progress()
    
    def _save_progress(self) -> None:
        """Save progress to checkpoint file"""
        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(self.progress.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def get_progress(self) -> Optional[Progress]:
        """Get current progress"""
        with self.lock:
            return self.progress
    
    def get_stats(self) -> Dict[str, Any]:
        """Get progress statistics"""
        with self.lock:
            if not self.progress:
                return {}
            
            elapsed = (self.progress.last_update - self.progress.start_time).total_seconds()
            
            return {
                'processed': self.progress.processed_papers,
                'failed': self.progress.failed_papers,
                'total': self.progress.total_papers,
                'success_rate': f"{self.progress.success_rate:.2%}",
                'papers_per_second': f"{self.progress.papers_per_second:.2f}",
                'elapsed_time': f"{elapsed/60:.1f} minutes",
                'estimated_remaining': f"{self.progress.estimated_time_remaining/60:.1f} minutes",
                'current_batch': self.progress.current_batch_id,
                'error_counts': self.progress.error_counts
            }
        

class CloudStorageExporter:
    """Export results to Google Cloud Storage continuously"""
    
    def __init__(self, bucket_name="supplement-analysis-results"):
        self.bucket_name = bucket_name
        self.client = None
        self.bucket = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Cloud Storage client"""
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"✅ Connected to Cloud Storage bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Cloud Storage: {e}")
            self.client = None
    
    async def upload_batch_file(self, batch_file: Path, batch_num: int) -> bool:
        """Upload a single batch file to Cloud Storage"""
        if not self.client:
            logger.error("Cloud Storage not available")
            return False
        
        try:
            # Upload with timestamp for uniqueness
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"completed/batch_{batch_num:06d}_{timestamp}.jsonl"
            blob = self.bucket.blob(blob_name)
            
            # Upload in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._upload_file, blob, batch_file)
            
            logger.info(f"📤 Uploaded {batch_file.name} to gs://{self.bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {batch_file}: {e}")
            return False
    
    def _upload_file(self, blob, file_path):
        """Helper method to upload file (runs in executor)"""
        with open(file_path, 'rb') as f:
            blob.upload_from_file(f)
    
    async def upload_failed_papers(self):
        """Upload failed papers file if it exists"""
        if not self.client:
            return
        
        failed_file = FAILED_DIR / "failed_papers.jsonl"
        if not failed_file.exists():
            return
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"failed/failed_papers_{timestamp}.jsonl"
            blob = self.bucket.blob(blob_name)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._upload_file, blob, failed_file)
            
        except Exception as e:
            logger.error(f"Failed to upload failed papers: {e}")
    
    async def upload_progress_file(self):
        """Upload progress checkpoint to cloud"""
        if not self.client or not CHECKPOINT_FILE.exists():
            return
        
        try:
            blob_name = f"progress/checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            blob = self.bucket.blob(blob_name)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._upload_file, blob, CHECKPOINT_FILE)
            
        except Exception as e:
            logger.error(f"Failed to upload progress: {e}")
    
    async def upload_final_files(self):
        """Upload any remaining files at the end"""
        if not self.client:
            return
        
        try:
            # Upload final progress
            await self.upload_progress_file()
            
            # Upload any remaining failed papers
            await self.upload_failed_papers()
            
            logger.info("✅ Final files uploaded to Cloud Storage")
            
        except Exception as e:
            logger.error(f"Failed to upload final files: {e}")
    
    async def export_completed_batches(self):
        """Export all completed batch files to Cloud Storage (legacy method for compatibility)"""
        # This method is kept for compatibility but now uploads happen continuously
        logger.info("Note: Batches are now uploaded continuously. Running final cleanup...")
        await self.upload_final_files()