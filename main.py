
# main.py
"""Main entry point for the supplement analysis system"""
import sys
import argparse
import multiprocessing as mp
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.coordinator import run_coordinator


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Analyze supplement research papers in parallel')
    parser.add_argument('--resume', action='store_true', 
                       help='Resume from previous progress')
    parser.add_argument('--reset', action='store_true',
                       help='Reset progress and start fresh')
    
    args = parser.parse_args()
    
    if args.reset and args.resume:
        print("Error: Cannot use --reset and --resume together")
        sys.exit(1)
    
    # Set up multiprocessing
    mp.set_start_method('spawn', force=True)
    
    print("="*60)
    print("SUPPLEMENT RESEARCH ANALYZER")
    print("="*60)
    print(f"Workers: 5")
    print(f"Concurrent tasks per worker: 6")
    print(f"Total concurrent API calls: 30")
    print(f"Mode: {'Resume' if args.resume else 'Fresh start'}")
    print("="*60)
    print("\nStarting analysis... (Press Ctrl+C to stop gracefully)\n")
    
    try:
        run_coordinator()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
