# monitor.py
"""Monitor progress of the analysis without interfering with main process"""
import json
import time
import sys
from pathlib import Path
from datetime import datetime
import os

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import PROGRESS_DIR, COMPLETED_DIR, FAILED_DIR


def get_file_stats():
    """Get statistics about files"""
    completed_files = list(COMPLETED_DIR.glob("batch_*.jsonl"))
    failed_file = FAILED_DIR / "failed_papers.jsonl"
    
    total_results = 0
    for f in completed_files:
        try:
            with open(f, 'r') as file:
                total_results += sum(1 for _ in file)
        except:
            pass
    
    failed_count = 0
    if failed_file.exists():
        try:
            with open(failed_file, 'r') as file:
                failed_count = sum(1 for _ in file)
        except:
            pass
    
    return {
        'batch_files': len(completed_files),
        'total_results': total_results,
        'failed_papers': failed_count
    }


def display_progress():
    """Display current progress"""
    checkpoint_file = PROGRESS_DIR / "checkpoint.json"
    
    if not checkpoint_file.exists():
        print("No progress file found. Analysis may not have started yet.")
        return
    
    try:
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
        
        # Calculate statistics
        total = data['total_papers']
        processed = data['processed_papers']
        failed = data['failed_papers']
        percent = (processed / total * 100) if total > 0 else 0
        success_rate = ((processed - failed) / processed * 100) if processed > 0 else 0
        
        start_time = datetime.fromisoformat(data['start_time'])
        last_update = datetime.fromisoformat(data['last_update'])
        elapsed = (last_update - start_time).total_seconds()
        
        papers_per_sec = processed / elapsed if elapsed > 0 else 0
        remaining = (total - processed) / papers_per_sec if papers_per_sec > 0 else 0
        
        # Get file stats
        file_stats = get_file_stats()
        
        # Clear screen (works on Windows and Unix)
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Display
        print("="*60)
        print("SUPPLEMENT ANALYSIS MONITOR")
        print("="*60)
        print(f"Progress:       {processed:,} / {total:,} ({percent:.1f}%)")
        print(f"Success Rate:   {success_rate:.1f}%")
        print(f"Failed Papers:  {failed:,}")
        print("-"*60)
        print(f"Speed:          {papers_per_sec:.1f} papers/second")
        print(f"Time Elapsed:   {elapsed/60:.1f} minutes")
        print(f"Time Remaining: {remaining/60:.1f} minutes")
        print("-"*60)
        print(f"Batch Files:    {file_stats['batch_files']}")
        print(f"Saved Results:  {file_stats['total_results']:,}")
        print(f"Current Batch:  {data['current_batch_id']}")
        print("-"*60)
        
        if data.get('error_counts'):
            print("Error Types:")
            for error_type, count in data['error_counts'].items():
                print(f"  {error_type}: {count}")
            print("-"*60)
        
        print(f"Last Updated:   {last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nPress Ctrl+C to exit monitor")
        
    except Exception as e:
        print(f"Error reading progress: {e}")


def main():
    """Main monitor loop"""
    print("Starting progress monitor...")
    
    try:
        while True:
            display_progress()
            time.sleep(5)  # Update every 5 seconds
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")


if __name__ == "__main__":
    main()