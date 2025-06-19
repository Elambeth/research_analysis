# test_cloud_upload.py - Test Cloud Storage Upload Functionality
"""
Test script to verify Cloud Storage uploads work correctly before running the main job.
Run this first to make sure everything is configured properly.
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Set up path and config
sys.path.insert(0, str(Path(__file__).parent))
from src import config_cloud as config
sys.modules['src.config'] = config

from src.models import AnalysisResult, Study
from src.storage import StorageManager, CloudStorageExporter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_result(study_id: int) -> AnalysisResult:
    """Create a test analysis result"""
    return AnalysisResult(
        study_id=study_id,
        supplement_id=1,
        safety_score="85",
        efficacy_score=72,
        quality_score=68,
        study_goal="Test the efficacy of supplement X",
        results_summary="Showed moderate improvement in test subjects",
        population_specificity="Adults aged 25-65",
        effective_dosage="200mg daily",
        study_duration="12 weeks",
        interactions="None mentioned",
        analysis_prompt_version="1.1",
        last_analyzed_at=datetime.now(timezone.utc).isoformat()
    )


async def test_cloud_storage_connection():
    """Test Cloud Storage connection"""
    logger.info("🧪 Testing Cloud Storage connection...")
    
    exporter = CloudStorageExporter()
    
    if exporter.client is None:
        logger.error("❌ Cloud Storage client failed to initialize")
        return False
    
    try:
        # Try to list objects in bucket
        blobs = list(exporter.bucket.list_blobs(max_results=1))
        logger.info(f"✅ Cloud Storage connection successful. Bucket exists with {len(list(exporter.bucket.list_blobs()))} objects")
        return True
    except Exception as e:
        logger.error(f"❌ Cloud Storage connection failed: {e}")
        return False


async def test_continuous_upload():
    """Test the continuous upload functionality"""
    logger.info("🧪 Testing continuous upload functionality...")
    
    storage_manager = StorageManager()
    
    # Create test results
    test_results = [create_test_result(i) for i in range(1, 6)]
    
    logger.info("Creating 5 test results...")
    
    # Save results (should trigger upload when batch size is reached)
    for result in test_results:
        await storage_manager.save_result(result)
    
    # Force finalization to upload any remaining results
    await storage_manager.finalize()
    
    logger.info("✅ Test upload completed")


async def verify_uploads():
    """Verify that test uploads actually made it to Cloud Storage"""
    logger.info("🔍 Verifying uploads in Cloud Storage...")
    
    exporter = CloudStorageExporter()
    
    if not exporter.client:
        logger.error("❌ Can't verify - Cloud Storage not available")
        return False
    
    try:
        # List completed files
        completed_blobs = list(exporter.bucket.list_blobs(prefix="completed/"))
        progress_blobs = list(exporter.bucket.list_blobs(prefix="progress/"))
        
        logger.info(f"Found {len(completed_blobs)} completed files in Cloud Storage:")
        for blob in completed_blobs[-5:]:  # Show last 5
            logger.info(f"  - {blob.name} (size: {blob.size} bytes, created: {blob.time_created})")
        
        logger.info(f"Found {len(progress_blobs)} progress files in Cloud Storage:")
        for blob in progress_blobs[-3:]:  # Show last 3
            logger.info(f"  - {blob.name} (size: {blob.size} bytes, created: {blob.time_created})")
        
        if len(completed_blobs) > 0:
            logger.info("✅ Upload verification successful - files found in Cloud Storage!")
            return True
        else:
            logger.warning("⚠️ No completed files found in Cloud Storage")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error verifying uploads: {e}")
        return False


async def cleanup_test_files():
    """Clean up test files (optional)"""
    logger.info("🧹 Cleaning up test files...")
    
    try:
        # Clean up local test files
        from src.config_cloud import COMPLETED_DIR, PROGRESS_DIR
        
        for test_file in COMPLETED_DIR.glob("batch_*.jsonl"):
            test_file.unlink()
            logger.info(f"Deleted local file: {test_file}")
        
        # Note: We leave Cloud Storage files as proof that upload works
        logger.info("✅ Local test files cleaned up (Cloud Storage files preserved)")
        
    except Exception as e:
        logger.error(f"Error cleaning up: {e}")


async def main():
    """Run all tests"""
    logger.info("="*60)
    logger.info("🧪 CLOUD STORAGE UPLOAD TEST")
    logger.info("="*60)
    
    # Test 1: Connection
    if not await test_cloud_storage_connection():
        logger.error("❌ Cloud Storage connection test failed. Fix this before proceeding!")
        return False
    
    # Test 2: Upload functionality
    try:
        await test_continuous_upload()
    except Exception as e:
        logger.error(f"❌ Upload test failed: {e}")
        return False
    
    # Test 3: Verification
    if not await verify_uploads():
        logger.error("❌ Upload verification failed. Files may not be reaching Cloud Storage!")
        return False
    
    # Optional cleanup
    await cleanup_test_files()
    
    logger.info("="*60)
    logger.info("🎉 ALL TESTS PASSED! Cloud Storage upload is working correctly.")
    logger.info("✅ Ready to run the main processing job safely!")
    logger.info("="*60)
    
    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)