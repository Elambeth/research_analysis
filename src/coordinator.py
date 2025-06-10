# coordinator.py
"""Main coordinator for the supplement analysis system"""
import asyncio
import logging
import multiprocessing as mp
import signal
import sys
import time
from datetime import datetime
from typing import List, Optional
from src.config import TEST_MODE, MAX_PAPERS_TEST
import psutil

from src.config import (
    NUM_WORKERS, FETCH_BATCH_SIZE, PROGRESS_UPDATE_INTERVAL,
    MAX_MEMORY_PERCENT, MAX_ERROR_RATE
)
from src.database import DatabaseManager
from src.storage import ProgressTracker
from src.worker import run_worker
from src.models import WorkBatch, Study

logger = logging.getLogger(__name__)


class Coordinator:
    """Coordinates the entire analysis process"""
    
    def __init__(self):
        self.work_queue = mp.Queue(maxsize=NUM_WORKERS * 2)  # Don't overwhelm with batches
        self.progress_queue = mp.Queue()
        self.workers: List[mp.Process] = []
        self.db = DatabaseManager()
        self.progress_tracker = ProgressTracker()
        self.shutdown_event = mp.Event()
        self.batch_counter = 0
        
        # Statistics
        self.start_time = datetime.now()
        self.total_distributed = 0
    
    def initialize(self):
        """Initialize the coordinator"""
        logger.info("Initializing coordinator...")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start workers
        self._start_workers()
        
        logger.info(f"Coordinator initialized with {NUM_WORKERS} workers")
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Coordinator received signal {sig}, initiating shutdown...")
        self.shutdown_event.set()
    
    def _start_workers(self):
        """Start worker processes"""
        for i in range(NUM_WORKERS):
            worker = mp.Process(
                target=run_worker,
                args=(i, self.work_queue, self.progress_queue),
                name=f"Worker-{i}"
            )
            worker.start()
            self.workers.append(worker)
            logger.info(f"Started worker {i} (PID: {worker.pid})")
    
    def _stop_workers(self):
        """Stop all worker processes"""
        logger.info("Stopping workers...")
        
        # Send poison pills
        for _ in range(NUM_WORKERS):
            self.work_queue.put(None)
        
        # Wait for workers to finish
        for i, worker in enumerate(self.workers):
            worker.join(timeout=30)
            if worker.is_alive():
                logger.warning(f"Worker {i} did not stop gracefully, terminating...")
                worker.terminate()
                worker.join()
            logger.info(f"Worker {i} stopped")
    
    async def _distribute_work(self):
        """Distribute work to workers"""
        try:
            # Get total count
            total_count = await self.db.get_total_unanalyzed_count()
            if total_count == 0:
                logger.info("No unanalyzed studies found")
                return
            
            if TEST_MODE and total_count > MAX_PAPERS_TEST:
                logger.info(f"TEST MODE: Limiting processing to {MAX_PAPERS_TEST} papers")
                total_count = MAX_PAPERS_TEST

            logger.info(f"Found {total_count} unanalyzed studies to process")
            self.progress_tracker.initialize(total_count)
            
            # Process in batches
            offset = 0
            
            while offset < total_count and not self.shutdown_event.is_set():
                # Check system resources
                if not self._check_system_health():
                    logger.error("System health check failed, stopping distribution")
                    break
                
                # Fetch batch of studies
                studies = await self.db.fetch_unanalyzed_studies(
                    offset=offset,
                    limit=FETCH_BATCH_SIZE
                )
                
                if not studies:
                    logger.info("No more studies to process")
                    break
                
                # Create work batch
                batch = WorkBatch(
                    batch_id=self.batch_counter,
                    studies=studies,
                    created_at=datetime.now()
                )
                
                # Put in queue (blocks if queue is full)
                logger.info(f"Distributing batch {self.batch_counter} with {len(studies)} studies")
                await asyncio.get_event_loop().run_in_executor(
                    None, self.work_queue.put, batch.to_dict()
                )
                
                self.batch_counter += 1
                self.total_distributed += len(studies)
                offset += len(studies)
                
                # Small delay to prevent overwhelming
                await asyncio.sleep(0.1)
            
            logger.info(f"Work distribution complete. Distributed {self.total_distributed} studies in {self.batch_counter} batches")
            
        except Exception as e:
            logger.error(f"Error distributing work: {e}")
            raise
    
    def _check_system_health(self) -> bool:
        """Check if system is healthy to continue"""
        try:
            # Check memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > MAX_MEMORY_PERCENT:
                logger.error(f"Memory usage too high: {memory_percent}%")
                return False
            
            # Check error rate from progress
            progress = self.progress_tracker.get_progress()
            if progress and progress.processed_papers > 100:  # Only check after some processing
                error_rate = progress.failed_papers / progress.processed_papers
                if error_rate > MAX_ERROR_RATE:
                    logger.error(f"Error rate too high: {error_rate:.2%}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking system health: {e}")
            return True  # Continue on error
    
    def _process_progress_updates(self):
        """Process progress updates from workers"""
        processed_updates = 0
        
        while True:
            try:
                # Get all available updates
                update = self.progress_queue.get_nowait()
                
                # Update progress tracker
                if 'processed' in update and 'failed' in update:
                    # This is a cumulative update from a worker
                    # We need to track per-worker progress properly
                    # For now, just log it
                    logger.debug(f"Progress update from worker {update['worker_id']}: "
                               f"Processed: {update['processed']}, Failed: {update['failed']}")
                
                processed_updates += 1
                
            except:
                break
        
        if processed_updates > 0:
            # Update overall progress
            self._update_and_display_progress()
    
    def _update_and_display_progress(self):
        """Update and display current progress"""
        stats = self.progress_tracker.get_stats()
        if stats:
            logger.info(
                f"Progress: {stats['processed']}/{stats['total']} "
                f"({stats['processed']/stats['total']*100:.1f}%) | "
                f"Success rate: {stats['success_rate']} | "
                f"Speed: {stats['papers_per_second']} papers/sec | "
                f"Time remaining: {stats['estimated_remaining']}"
            )
    
    async def run(self):
        """Main coordinator loop"""
        logger.info("Starting coordinator...")
        
        try:
            # Start distributing work
            distribution_task = asyncio.create_task(self._distribute_work())
            
            # Monitor progress
            while not self.shutdown_event.is_set():
                # Process progress updates
                self._process_progress_updates()
                
                # Check if distribution is complete and all workers are idle
                if distribution_task.done() and self.work_queue.empty():
                    # Give workers time to finish current work
                    await asyncio.sleep(5)
                    if self.work_queue.empty():
                        logger.info("All work distributed and queue empty")
                        break
                
                # Check worker health
                alive_workers = sum(1 for w in self.workers if w.is_alive())
                if alive_workers < NUM_WORKERS:
                    logger.warning(f"Only {alive_workers}/{NUM_WORKERS} workers alive")
                
                await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)
            
            # Wait for distribution to complete if interrupted
            if not distribution_task.done():
                distribution_task.cancel()
                await asyncio.gather(distribution_task, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Coordinator error: {e}")
            raise
        
        finally:
            logger.info("Coordinator shutting down...")
            self._stop_workers()
            
            # Final progress update
            self._update_and_display_progress()
            
            # Close database
            self.db.close()
            
            elapsed = (datetime.now() - self.start_time).total_seconds() / 60
            logger.info(f"Coordinator shutdown complete. Total runtime: {elapsed:.1f} minutes")


async def coordinator_main():
    """Main entry point for coordinator"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Coordinator - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/coordinator.log"),
            logging.StreamHandler()
        ]
    )
    
    coordinator = Coordinator()
    coordinator.initialize()
    
    try:
        await coordinator.run()
    except Exception as e:
        logger.error(f"Fatal error in coordinator: {e}")
        sys.exit(1)


def run_coordinator():
    """Entry point for the coordinator process"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(coordinator_main())