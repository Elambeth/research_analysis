# coordinator.py - SYNTHESIZED VERSION
"""Main coordinator for the supplement analysis system"""
import asyncio
import logging
import multiprocessing as mp
import signal
import sys
import time
from datetime import datetime
from typing import List, Optional
from src.config_cloud import TEST_MODE, MAX_PAPERS_TEST
import psutil

from src.config_cloud import (
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
    
    # Just replace the _distribute_work method in your coordinator.py with this:

    async def _distribute_work(self):
        """Distribute work to workers - SIMPLIFIED VERSION"""
        try:
            logger.info("Starting work distribution (skipping total count)")
            
            # Initialize progress tracker with a large estimate
            estimated_total = 50000  # Adjust based on your data size
            self.progress_tracker.initialize(estimated_total)
            
            # Process in batches until no more studies
            offset = 0
            consecutive_empty_batches = 0
            papers_distributed = 0
            
            while not self.shutdown_event.is_set():
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
                    consecutive_empty_batches += 1
                    logger.info(f"No studies found at offset {offset} (empty batch #{consecutive_empty_batches})")
                    
                    # If we get 3 consecutive empty batches, we're probably done
                    if consecutive_empty_batches >= 3:
                        logger.info("Multiple consecutive empty batches - processing complete")
                        break
                    
                    # Skip ahead in case there are gaps in the data
                    offset += FETCH_BATCH_SIZE
                    continue
                else:
                    consecutive_empty_batches = 0  # Reset counter
                
                # TEST MODE: Check if we've hit the limit
                if TEST_MODE and MAX_PAPERS_TEST and papers_distributed >= MAX_PAPERS_TEST:
                    logger.info(f"TEST MODE: Reached limit of {MAX_PAPERS_TEST} papers")
                    break
                
                # Trim batch if needed for test mode
                if TEST_MODE and MAX_PAPERS_TEST:
                    remaining_allowed = MAX_PAPERS_TEST - papers_distributed
                    if len(studies) > remaining_allowed:
                        studies = studies[:remaining_allowed]
                        logger.info(f"TEST MODE: Trimmed batch to {len(studies)} papers")
                
                # Create work batch
                batch = WorkBatch(
                    batch_id=self.batch_counter,
                    studies=studies,
                    created_at=datetime.now()
                )
                
                # Put in queue
                logger.info(f"Distributing batch {self.batch_counter} with {len(studies)} studies (offset: {offset})")
                await asyncio.get_event_loop().run_in_executor(
                    None, self.work_queue.put, batch.to_dict()
                )
                
                self.batch_counter += 1
                self.total_distributed += len(studies)
                papers_distributed += len(studies)
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
                
                # Track cumulative progress from all workers
                worker_id = update.get('worker_id')
                processed = update.get('processed', 0)
                failed = update.get('failed', 0)
                
                logger.debug(f"Progress update from worker {worker_id}: "
                        f"Processed: {processed}, Failed: {failed}")
                
                processed_updates += 1
                
            except:
                break
        
        if processed_updates > 0:
            # Update overall progress based on actual results
            # This is a simplified approach - in production you'd track per-worker
            self.progress_tracker.update(processed=processed_updates)
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
            last_progress_check = time.time()
            no_progress_counter = 0
            
            while not self.shutdown_event.is_set():
                # Process progress updates
                self._process_progress_updates()
                
                # Check if distribution is complete and all workers are idle
                if distribution_task.done() and self.work_queue.empty():
                    # Check if we're still making progress
                    current_progress = self.progress_tracker.get_progress()
                    if current_progress:
                        # If we've processed everything, we're done
                        if current_progress.processed_papers >= current_progress.total_papers:
                            logger.info("All papers processed!")
                            break
                        
                        # If no progress for 60 seconds, assume we're done
                        if time.time() - last_progress_check > 60:
                            no_progress_counter += 1
                            if no_progress_counter >= 2:  # No progress for 2 minutes
                                logger.info("No progress detected for 2 minutes, assuming complete")
                                break
                        else:
                            no_progress_counter = 0
                            last_progress_check = time.time()
                
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