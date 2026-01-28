#!/usr/bin/env python3
"""
F1 Pecking Order - Run Data Collector

This is the main entry point to collect F1 data.
Run this script to download lap times, tire data, and driver information
from the OpenF1 API and store it in your local database.

USAGE:
    python run_collector.py           # Fetch recent 2025 data (default)
    python run_collector.py --2024    # Fetch 2024 data instead (for testing)
    python run_collector.py --full    # Fetch ALL available 2025 data
    python run_collector.py --help    # Show this help message

The script will:
1. Connect to the free OpenF1 API
2. Download data for recent race weekends
3. Store everything in a local SQLite database (f1_data.db)
4. Print a summary of what was collected
"""

import sys
from datetime import datetime

# Import our data collector
from data_collector import (
    fetch_recent_data,
    fetch_all_2025_data,
    fetch_sample_2024_data,
)
from database import get_statistics, initialize_database


def print_welcome():
    """Prints a welcome banner."""
    print()
    print("  ╔═══════════════════════════════════════════════════════╗")
    print("  ║                                                       ║")
    print("  ║     F1 PECKING ORDER - DATA COLLECTOR                 ║")
    print("  ║                                                       ║")
    print("  ║     Fetching TRUE pace data from OpenF1 API           ║")
    print("  ║                                                       ║")
    print("  ╚═══════════════════════════════════════════════════════╝")
    print()
    print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def print_help():
    """Prints usage instructions."""
    print(__doc__)


def show_current_data():
    """Shows what data is currently in the database."""
    print("\nChecking current database...")
    initialize_database()
    stats = get_statistics()

    print("\nCurrent database contents:")
    print(f"  - Meetings: {stats['meetings']}")
    print(f"  - Sessions: {stats['sessions']}")
    print(f"  - Total laps: {stats['total_laps']}")
    print(f"  - Valid laps: {stats['valid_laps']}")
    print(f"  - Unique drivers: {stats['unique_drivers']}")
    print(f"  - Teams: {stats['teams']}")

    if stats['meetings'] == 0:
        print("\n[INFO] Database is empty. Run without flags to collect data.")


def main():
    """
    Main entry point for the data collector.

    Parses command line arguments and runs the appropriate collection function.
    """
    # Print welcome banner
    print_welcome()

    # Parse command line arguments
    args = sys.argv[1:]  # Get all arguments after the script name

    # Handle help request
    if "--help" in args or "-h" in args:
        print_help()
        return

    # Handle status request (show what data we have)
    if "--status" in args:
        show_current_data()
        return

    # Handle different collection modes
    if "--2024" in args:
        # Fetch 2024 data (useful if 2025 isn't available yet)
        print("[MODE] Fetching 2024 sample data for testing\n")
        fetch_sample_2024_data()

    elif "--full" in args:
        # Fetch all available 2025 data
        print("[MODE] Fetching ALL available 2025 season data")
        print("[WARNING] This may take a while!\n")
        fetch_all_2025_data()

    else:
        # Default: fetch recent 2025 data
        print("[MODE] Fetching recent 2025 race weekend data\n")
        fetch_recent_data()

    # Print completion message
    print("\n" + "=" * 60)
    print("COLLECTION FINISHED!")
    print("=" * 60)
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nYour F1 data is now stored in: f1_data.db")
    print("You can view the database using a tool like 'DB Browser for SQLite'")
    print("or by running: python -c \"from database import get_statistics; print(get_statistics())\"")


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\n\n[INTERRUPTED] Data collection stopped by user.")
        print("Partial data may have been saved to the database.")
        sys.exit(1)
    except Exception as e:
        # Handle unexpected errors
        print(f"\n\n[ERROR] An unexpected error occurred: {e}")
        print("\nIf this persists, please check:")
        print("  1. Your internet connection")
        print("  2. That the OpenF1 API is accessible (https://api.openf1.org)")
        print("  3. That you have write permission in this directory")
        sys.exit(1)
