"""
F1 Pecking Order - Data Collector

This file handles fetching data from the OpenF1 API and storing it in our database.
The OpenF1 API provides free access to F1 timing data including lap times, tire
compounds, and driver information.

The main function fetch_recent_data() will:
1. Get the most recent race weekends (meetings)
2. For each meeting, get all sessions (FP1, FP2, FP3, Quali, Race)
3. For each session, get all laps, stints (tire data), and drivers
4. Merge the data together and store in our database
5. Mark invalid laps (outliers, pit laps, etc.)
"""

import requests
import time
from typing import List, Dict, Optional, Any
from datetime import datetime

# Import our configuration and database functions
from config import (
    OPENF1_API_BASE_URL,
    API_REQUEST_DELAY,
    API_TIMEOUT,
    TARGET_YEAR,
    NUM_RECENT_MEETINGS,
    LAP_TIME_OUTLIER_THRESHOLD,
    MIN_VALID_LAP_TIME,
    MAX_VALID_LAP_TIME,
)
from database import (
    initialize_database,
    insert_meeting,
    insert_session,
    insert_driver,
    insert_stint,
    bulk_insert_laps,
    get_statistics,
    meeting_exists,
)


# =============================================================================
# API REQUEST HELPER
# =============================================================================

def make_api_request(endpoint: str, params: Optional[Dict] = None) -> Optional[List[Dict]]:
    """
    Makes a request to the OpenF1 API.

    This function handles:
    - Building the full URL
    - Adding a delay to be respectful to the API
    - Handling errors gracefully
    - Returning the JSON response

    Args:
        endpoint: The API endpoint (e.g., "/meetings")
        params: Optional query parameters (e.g., {"year": 2025})

    Returns:
        List of dictionaries with the API response data, or None if error
    """
    # Build the full URL
    url = f"{OPENF1_API_BASE_URL}{endpoint}"

    # Be respectful - wait before making the request
    time.sleep(API_REQUEST_DELAY)

    try:
        # Make the HTTP GET request
        response = requests.get(url, params=params, timeout=API_TIMEOUT)

        # Check if the request was successful (status code 200)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        return data

    except requests.exceptions.Timeout:
        print(f"    ERROR: Request timed out for {endpoint}")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"    ERROR: HTTP error for {endpoint}: {e}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Request failed for {endpoint}: {e}")
        return None

    except ValueError as e:
        print(f"    ERROR: Could not parse JSON from {endpoint}: {e}")
        return None


# =============================================================================
# DATA FETCHING FUNCTIONS
# =============================================================================

def fetch_meetings(year: int) -> List[Dict]:
    """
    Fetches all race weekends (meetings) for a given year.

    Args:
        year: The F1 season year (e.g., 2025)

    Returns:
        List of meeting dictionaries from the API
    """
    print(f"\nFetching meetings for {year}...")

    data = make_api_request("/meetings", params={"year": year})

    if data is None:
        print("  Failed to fetch meetings")
        return []

    # Filter out any test sessions (only keep actual race weekends)
    # Real race meetings have a meeting_name that ends with "Grand Prix" or "GP"
    meetings = [m for m in data if "Grand Prix" in m.get("meeting_name", "")]

    print(f"  Found {len(meetings)} race weekends")
    return meetings


def fetch_sessions(meeting_key: int) -> List[Dict]:
    """
    Fetches all sessions for a specific race weekend.
    Sessions include Practice 1, Practice 2, Practice 3, Qualifying, Race, etc.

    Args:
        meeting_key: The unique identifier for the race weekend

    Returns:
        List of session dictionaries from the API
    """
    data = make_api_request("/sessions", params={"meeting_key": meeting_key})

    if data is None:
        return []

    return data


def fetch_laps(session_key: int) -> List[Dict]:
    """
    Fetches all lap times for a specific session.
    This includes lap duration, sector times, and speed trap data.

    Args:
        session_key: The unique identifier for the session

    Returns:
        List of lap dictionaries from the API
    """
    data = make_api_request("/laps", params={"session_key": session_key})

    if data is None:
        return []

    return data


def fetch_stints(session_key: int) -> List[Dict]:
    """
    Fetches tire stint data for a specific session.
    A stint is the period a driver spends on one set of tires (between pit stops).

    The stint data tells us which tire compound was used for which laps.

    Args:
        session_key: The unique identifier for the session

    Returns:
        List of stint dictionaries from the API
    """
    data = make_api_request("/stints", params={"session_key": session_key})

    if data is None:
        return []

    return data


def fetch_drivers(session_key: int) -> List[Dict]:
    """
    Fetches driver information for a specific session.
    This gives us driver names, numbers, teams, and colors.

    Args:
        session_key: The unique identifier for the session

    Returns:
        List of driver dictionaries from the API
    """
    data = make_api_request("/drivers", params={"session_key": session_key})

    if data is None:
        return []

    return data


# =============================================================================
# DATA MERGING FUNCTIONS
# =============================================================================

def merge_laps_with_stints(laps: List[Dict], stints: List[Dict]) -> List[Dict]:
    """
    Merges lap data with stint data to add tire compound information to each lap.

    The API gives us laps and stints separately, but we need to know which tire
    compound was used for each lap. This function figures that out.

    Args:
        laps: List of lap records from the API
        stints: List of stint records from the API

    Returns:
        List of lap records with tire compound and age added
    """
    # Create a lookup structure for stints by driver
    # This makes it faster to find which stint a lap belongs to
    driver_stints = {}
    for stint in stints:
        driver_num = stint.get('driver_number')
        if driver_num not in driver_stints:
            driver_stints[driver_num] = []
        driver_stints[driver_num].append(stint)

    # Go through each lap and add tire information
    for lap in laps:
        driver_num = lap.get('driver_number')
        lap_num = lap.get('lap_number')

        # Default values if we can't find stint data
        lap['compound'] = None
        lap['tire_age'] = None

        # Find the stint this lap belongs to
        if driver_num in driver_stints:
            for stint in driver_stints[driver_num]:
                lap_start = stint.get('lap_start', 0)
                lap_end = stint.get('lap_end', 999)  # Use high number if not set

                # Check if this lap falls within this stint
                if lap_start <= lap_num <= lap_end:
                    lap['compound'] = stint.get('compound')

                    # Calculate tire age for this lap
                    # tire_age = laps since stint started + initial tire age
                    initial_age = stint.get('tyre_age_at_start', 0)
                    laps_in_stint = lap_num - lap_start
                    lap['tire_age'] = initial_age + laps_in_stint

                    break  # Found the right stint, stop looking

    return laps


def mark_invalid_laps(laps: List[Dict], session_type: str) -> List[Dict]:
    """
    Marks laps that shouldn't be used for pace analysis.

    Invalid laps include:
    - Pit out laps (coming out of the pits - always slow)
    - Pit in laps (going into the pits - driver lifts off)
    - Outliers (laps way slower than average - probably yellow flags, accidents)
    - Laps with missing data

    Args:
        laps: List of lap records
        session_type: Type of session (Practice, Qualifying, Race)

    Returns:
        List of lap records with is_valid_for_ranking set
    """
    if not laps:
        return laps

    # Calculate the average lap time for this session (excluding obvious outliers)
    valid_times = []
    for lap in laps:
        lap_time = lap.get('lap_duration')
        if lap_time and MIN_VALID_LAP_TIME < lap_time < MAX_VALID_LAP_TIME:
            valid_times.append(lap_time)

    if not valid_times:
        # No valid times found, mark all as invalid
        for lap in laps:
            lap['is_valid_for_ranking'] = False
        return laps

    # Calculate average lap time
    average_lap_time = sum(valid_times) / len(valid_times)

    # Mark each lap as valid or invalid
    for lap in laps:
        lap_time = lap.get('lap_duration')
        is_pit_out = lap.get('is_pit_out_lap', False)

        # Start by assuming the lap is valid
        lap['is_valid_for_ranking'] = True

        # Mark invalid: No lap time recorded
        if lap_time is None:
            lap['is_valid_for_ranking'] = False
            continue

        # Mark invalid: Pit out lap
        if is_pit_out:
            lap['is_valid_for_ranking'] = False
            continue

        # Mark invalid: Lap time outside reasonable range
        if lap_time < MIN_VALID_LAP_TIME or lap_time > MAX_VALID_LAP_TIME:
            lap['is_valid_for_ranking'] = False
            continue

        # Mark invalid: Lap time is way slower than average (probably yellow flag, etc.)
        if lap_time > average_lap_time * LAP_TIME_OUTLIER_THRESHOLD:
            lap['is_valid_for_ranking'] = False
            continue

        # For qualifying sessions, also check if it's a fast enough lap
        # (Many quali laps are slow out-laps or aborted laps)
        if session_type == "Qualifying":
            # In quali, a valid hot lap should be within 5% of the best time
            best_time = min(valid_times)
            if lap_time > best_time * 1.05:
                lap['is_valid_for_ranking'] = False

    return laps


# =============================================================================
# MAIN DATA COLLECTION FUNCTION
# =============================================================================

def fetch_recent_data(year: int = TARGET_YEAR, num_meetings: int = NUM_RECENT_MEETINGS):
    """
    Main function that fetches data for recent race weekends and stores it.

    This function:
    1. Gets all meetings for the year
    2. Takes the most recent N meetings
    3. For each meeting, fetches and stores all data

    Args:
        year: The F1 season year
        num_meetings: How many recent race weekends to fetch
    """
    print("=" * 60)
    print("F1 PECKING ORDER - DATA COLLECTOR")
    print("=" * 60)

    # Initialize the database (creates tables if they don't exist)
    print("\nInitializing database...")
    initialize_database()

    # Fetch all meetings for the year
    meetings = fetch_meetings(year)

    if not meetings:
        print("\nNo meetings found. The API might not have data for this year yet.")
        print("Try using year=2024 to get historical data.")
        return

    # Sort meetings by date (most recent first)
    meetings_sorted = sorted(
        meetings,
        key=lambda x: x.get('date_start', ''),
        reverse=True
    )

    # Take the most recent N meetings
    recent_meetings = meetings_sorted[:num_meetings]

    print(f"\nWill process {len(recent_meetings)} most recent race weekends:")
    for meeting in recent_meetings:
        print(f"  - {meeting.get('meeting_name')} ({meeting.get('date_start', 'unknown date')[:10]})")

    # Track statistics
    total_sessions = 0
    total_laps = 0
    all_drivers = set()

    # Process each meeting
    for i, meeting in enumerate(recent_meetings, 1):
        meeting_name = meeting.get('meeting_name', 'Unknown')
        meeting_key = meeting.get('meeting_key')

        print(f"\n{'='*60}")
        print(f"[{i}/{len(recent_meetings)}] Processing: {meeting_name}")
        print("=" * 60)

        # Store the meeting in our database
        meeting['year'] = year
        insert_meeting(meeting)

        # Fetch all sessions for this meeting
        print("\n  Fetching sessions...")
        sessions = fetch_sessions(meeting_key)
        print(f"  Found {len(sessions)} sessions")

        # Process each session
        for session in sessions:
            session_name = session.get('session_name', 'Unknown')
            session_key = session.get('session_key')
            session_type = session.get('session_type', '')

            print(f"\n  Processing session: {session_name}")

            # Store the session
            insert_session(session)
            total_sessions += 1

            # Fetch drivers for this session
            print(f"    Fetching drivers...")
            drivers = fetch_drivers(session_key)
            print(f"    Found {len(drivers)} drivers")

            # Store each driver
            for driver in drivers:
                insert_driver(driver, session_key)
                all_drivers.add(driver.get('full_name'))

            # Fetch laps for this session
            print(f"    Fetching laps...")
            laps = fetch_laps(session_key)
            print(f"    Found {len(laps)} laps")

            if not laps:
                continue

            # Fetch stint data (tire compounds)
            print(f"    Fetching stint data...")
            stints = fetch_stints(session_key)
            print(f"    Found {len(stints)} stints")

            # Store stints
            for stint in stints:
                insert_stint(stint, session_key)

            # Merge lap data with stint data to add tire info
            print(f"    Merging lap and stint data...")
            laps = merge_laps_with_stints(laps, stints)

            # Mark invalid laps (pit laps, outliers, etc.)
            print(f"    Marking invalid laps...")
            laps = mark_invalid_laps(laps, session_type)

            # Count valid laps
            valid_count = sum(1 for lap in laps if lap.get('is_valid_for_ranking'))
            print(f"    Valid laps for ranking: {valid_count}/{len(laps)}")

            # Store all laps in the database
            print(f"    Storing laps in database...")
            bulk_insert_laps(laps, session_key)
            total_laps += len(laps)

    # Print final summary
    print("\n" + "=" * 60)
    print("DATA COLLECTION COMPLETE!")
    print("=" * 60)

    # Get statistics from the database
    stats = get_statistics()

    print(f"\n SUMMARY:")
    print(f"  - Meetings collected: {len(recent_meetings)}")
    print(f"  - Sessions collected: {total_sessions}")
    print(f"  - Total laps collected: {total_laps}")
    print(f"  - Unique drivers: {len(all_drivers)}")

    print(f"\n DATABASE TOTALS:")
    print(f"  - Total meetings in database: {stats['meetings']}")
    print(f"  - Total sessions in database: {stats['sessions']}")
    print(f"  - Total laps in database: {stats['total_laps']}")
    print(f"  - Valid laps for ranking: {stats['valid_laps']}")
    print(f"  - Unique drivers in database: {stats['unique_drivers']}")
    print(f"  - Teams in database: {stats['teams']}")

    print("\nDrivers found:")
    for driver_name in sorted(all_drivers):
        print(f"  - {driver_name}")


def fetch_all_2025_data():
    """
    Fetches all available data for the 2025 season.
    Use this to populate the database with a full season of data.
    """
    fetch_recent_data(year=2025, num_meetings=30)  # 24 races + potential sprints


def fetch_sample_2024_data():
    """
    Fetches sample data from 2024 season for testing.
    Use this if 2025 data isn't available yet.
    """
    print("\n[INFO] Fetching 2024 data for testing purposes...")
    fetch_recent_data(year=2024, num_meetings=3)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # When this file is run directly, fetch recent data
    fetch_recent_data()
