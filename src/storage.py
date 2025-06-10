# storage.py
"""Local storage manager for analysis results"""
import json
import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import aiofiles
import logging
from threading import Lock

from src.config import (
    COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR, CHECKPOINT_FILE,
    LOCAL_SAVE_BATCH_SIZE
)
from src.models import AnalysisResult, Progress, FailedPaper, Study

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages local file storage of analysis results"""
    
    def __init__(self):
        self.current_batch = []
        self.current_batch_num = 1
        self.batch_lock = asyncio.Lock()
        self.failed_lock = asyncio.Lock()
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        for dir_path in [COMPLETED_DIR, FAILED_DIR, PROGRESS_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
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
        """Flush current batch to disk"""
        if not self.current_batch:
            return
        
        filename = self._get_batch_filename(self.current_batch_num)
        
        try:
            async with aiofiles.open(filename, 'w') as f:
                for result in self.current_batch:
                    await f.write(result.to_json_line() + '\n')
            
            logger.info(f"Saved batch {self.current_batch_num} with {len(self.current_batch)} results")
            
            # Reset for next batch
            self.current_batch = []
            self.current_batch_num += 1
            
        except Exception as e:
            logger.error(f"Failed to save batch {self.current_batch_num}: {e}")
            raise
    
    async def save_failed(self, failed_paper: FailedPaper) -> None:
        """Save a failed paper for later retry"""
        filename = FAILED_DIR / f"failed_papers.jsonl"
        
        async with self.failed_lock:
            try:
                async with aiofiles.open(filename, 'a') as f:
                    await f.write(failed_paper.to_json_line() + '\n')
                logger.debug(f"Saved failed paper: {failed_paper.study.id}")
            except Exception as e:
                logger.error(f"Failed to save failed paper: {e}")
    
    async def finalize(self) -> None:
        """Finalize storage, flush any remaining data"""
        async with self.batch_lock:
            if self.current_batch:
                await self._flush_batch()
        
        logger.info(f"Storage finalized. Total batches: {self.current_batch_num - 1}")
    
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
    """Tracks and persists progress"""
    
    def __init__(self):
        self.progress: Optional[Progress] = None
        self.lock = Lock()
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