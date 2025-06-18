# worker.py
"""Worker process for analyzing studies"""
import asyncio
import logging
import signal
import sys
from typing import Optional, List
from datetime import datetime
import multiprocessing as mp
import traceback
import queue

from src.config_cloud import TASKS_PER_WORKER, PROGRESS_UPDATE_INTERVAL
from src.database import DatabaseManager
from src.processor import ProcessorPool, ProcessorError
from src.storage import StorageManager
from src.models import Study, FailedPaper, WorkBatch

logger = logging.getLogger(__name__)


class Worker:
    """Worker process that processes studies"""
    
    def __init__(self, worker_id: int, work_queue: mp.Queue, progress_queue: mp.Queue):
        self.worker_id = worker_id
        self.work_queue = work_queue
        self.progress_queue = progress_queue
        self.storage = StorageManager()
        self.db = DatabaseManager(worker_id=worker_id)
        self.processor_pool = ProcessorPool(TASKS_PER_WORKER)
        self.shutdown_event = asyncio.Event()
        self.tasks: List[asyncio.Task] = []
        
        # Statistics
        self.processed_count = 0
        self.failed_count = 0
        self.last_update_time = datetime.now()
    
    async def initialize(self):
        """Initialize worker resources"""
        await self.processor_pool.initialize()
        logger.info(f"Worker {self.worker_id} initialized")
    
    async def cleanup(self):
        """Clean up worker resources"""
        # Cancel any remaining tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Clean up resources
        await self.processor_pool.close()
        await self.storage.finalize()
        self.db.close()
        
        logger.info(f"Worker {self.worker_id} cleaned up")
    
    # In worker.py, modify the process_study method to skip studies without abstracts:


    async def process_study(self, study: Study):
        """Process a single study"""
        # Skip if no abstract
        if not study.abstract:
            logger.warning(f"Worker {self.worker_id}: Skipping study {study.id} - no abstract")
            
            # Save as failed with specific reason
            failed_paper = FailedPaper(
                study=study,
                error="No abstract available",
                error_type="NoAbstract",
                attempts=1,
                last_attempt=datetime.now()
            )
            await self.storage.save_failed(failed_paper)
            
            self.failed_count += 1
            
            # Report progress for failed study
            self._report_progress(None)
            return
        
        try:
            # Analyze with DeepSeek
            result = await self.processor_pool.process_study(study)
            
            # Save result locally
            await self.storage.save_result(result)
            
            # Update statistics
            self.processed_count += 1
            
            # Report progress after each successful study
            self._report_progress(None)
            
            # Log progress periodically
            if self.processed_count % 5 == 0:
                logger.info(f"Worker {self.worker_id}: Processed {self.processed_count} studies successfully")
            
        except ProcessorError as e:
            # Handle known processor errors
            logger.warning(f"Worker {self.worker_id}: Failed to process study {study.id}: {e}")
            
            failed_paper = FailedPaper(
                study=study,
                error=str(e),
                error_type="ProcessorError",
                attempts=1,
                last_attempt=datetime.now()
            )
            await self.storage.save_failed(failed_paper)
            
            self.failed_count += 1
            
            # Report progress for failed study
            self._report_progress(None)
            
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Worker {self.worker_id}: Unexpected error processing study {study.id}: {e}")
            logger.debug(traceback.format_exc())
            
            failed_paper = FailedPaper(
                study=study,
                error=str(e),
                error_type=type(e).__name__,
                attempts=1,
                last_attempt=datetime.now()
            )
            await self.storage.save_failed(failed_paper)
            
            self.failed_count += 1
            
            # Report progress for failed study
            self._report_progress(None)
    
    async def process_batch(self, batch: WorkBatch):
        """Process a batch of studies"""
        logger.info(f"Worker {self.worker_id}: Processing batch {batch.batch_id} with {len(batch.studies)} studies")
        
        # Create tasks for concurrent processing
        tasks = []
        for study in batch.studies:
            if self.shutdown_event.is_set():
                break
            
            task = asyncio.create_task(self.process_study(study))
            tasks.append(task)
            
            # Limit concurrent tasks
            if len(tasks) >= TASKS_PER_WORKER:
                # Wait for some to complete
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = list(pending)
        
        # Wait for remaining tasks
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Report progress
        self._report_progress(batch.batch_id)
        
        logger.info(f"Worker {self.worker_id}: Completed batch {batch.batch_id}")
    
    def _report_progress(self, batch_id: int):
        """Report progress to coordinator"""
        try:
            progress_data = {
                'worker_id': self.worker_id,
                'batch_id': batch_id,
                'processed': self.processed_count,
                'failed': self.failed_count,
                'timestamp': datetime.now()
            }
            self.progress_queue.put_nowait(progress_data)
        except:
            logger.error(f"Worker {self.worker_id}: Failed to report progress")
    
    async def run(self):
        """Main worker loop"""
        logger.info(f"Worker {self.worker_id} starting")
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get work from queue (with timeout to check shutdown)
                    batch_data = await asyncio.get_event_loop().run_in_executor(
                        None, self.work_queue.get, True, 1.0
                    )
                    
                    if batch_data is None:  # Poison pill
                        logger.info(f"Worker {self.worker_id}: Received shutdown signal")
                        break
                    
                    # Deserialize batch
                    batch = WorkBatch(
                        batch_id=batch_data['batch_id'],
                        studies=[Study(**s) for s in batch_data['studies']],
                        created_at=datetime.fromisoformat(batch_data['created_at'])
                    )
                    
                    # Process the batch
                    await self.process_batch(batch)
                    
                except queue.Empty:  # Changed from asyncio.TimeoutError
                    continue  # This is normal, just check for shutdown
                except Exception as e:
                    logger.error(f"Worker {self.worker_id}: Error in main loop: {e}")
                    logger.debug(traceback.format_exc())
                    await asyncio.sleep(1)
        
        finally:
            logger.info(f"Worker {self.worker_id}: Final stats - Processed: {self.processed_count}, Failed: {self.failed_count}")
    
    def shutdown(self):
        """Signal shutdown"""
        self.shutdown_event.set()


async def worker_main(worker_id: int, work_queue: mp.Queue, progress_queue: mp.Queue):
    """Main entry point for worker process"""
    # Configure logging for this process
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - Worker-{worker_id} - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/worker_{worker_id}.log"),
            logging.StreamHandler()
        ]
    )
    
    # Create and run worker
    worker = Worker(worker_id, work_queue, progress_queue)
    
    # Handle signals
    def signal_handler(sig, frame):
        logger.info(f"Worker {worker_id}: Received signal {sig}")
        worker.shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Worker {worker_id}: Fatal error: {e}")
        logger.debug(traceback.format_exc())
    finally:
        await worker.cleanup()


def run_worker(worker_id: int, work_queue: mp.Queue, progress_queue: mp.Queue):
    """Entry point for multiprocessing"""
    # Set up asyncio for Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run the async worker
    asyncio.run(worker_main(worker_id, work_queue, progress_queue))