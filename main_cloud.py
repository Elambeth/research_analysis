# main_cloud.py - Cloud-optimized entry point
import sys
import multiprocessing as mp
from pathlib import Path
import logging

# Use cloud config
sys.path.insert(0, str(Path(__file__).parent))
from src import config_cloud as config

# Replace config in other modules
import src.coordinator
sys.modules['src.config'] = config

from src.coordinator import run_coordinator

def main():
    """Cloud-optimized main entry point with result export"""
    mp.set_start_method('spawn', force=True)
    
    # Configure logging for Google Cloud
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("SUPPLEMENT RESEARCH ANALYZER - CLOUD BATCH")
    logger.info("="*60)
    logger.info(f"Workers: {config.NUM_WORKERS}")
    logger.info(f"Tasks per worker: {config.TASKS_PER_WORKER}")
    logger.info(f"Total concurrent API calls: {config.TOTAL_MAX_CONNECTIONS}")
    logger.info("Processing ALL papers (TEST_MODE disabled)")
    logger.info("="*60)
    
    try:
        # Run the main analysis
        run_coordinator()
        logger.info("🎉 Analysis completed successfully!")
        
        # Export results to Cloud Storage
        logger.info("📤 Exporting results to Cloud Storage...")
        from src.storage import CloudStorageExporter
        import asyncio
        
        exporter = CloudStorageExporter()
        asyncio.run(exporter.export_completed_batches())
        
        logger.info("✅ Batch job completed and results exported!")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Batch job interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Batch job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()