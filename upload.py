
# upload.py
"""Upload analyzed results to Supabase"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.uploader import run_uploader


def main():
    """Main entry point for uploader"""
    parser = argparse.ArgumentParser(description='Upload analyzed results to Supabase')
    parser.add_argument('--files', nargs='+', 
                       help='Specific files to upload (e.g., batch_000001.jsonl)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be uploaded without uploading')
    
    args = parser.parse_args()
    
    print("="*60)
    print("SUPPLEMENT ANALYSIS UPLOADER")
    print("="*60)
    
    if args.files:
        print(f"Uploading specific files: {', '.join(args.files)}")
    else:
        print("Uploading all completed batches")
    
    if args.dry_run:
        print("\n** DRY RUN MODE - No actual uploads will occur **")
    
    print("="*60)
    print("\nStarting upload process...\n")
    
    try:
        run_uploader(args.files)
    except KeyboardInterrupt:
        print("\nUpload cancelled by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()