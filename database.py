"""
F1 Pecking Order - Database Setup and Operations

This file handles all interactions with our SQLite database.
SQLite is a simple database that stores everything in a single file (f1_data.db).

Think of a database as a collection of spreadsheets (called "tables"), where
each table holds a specific type of data (meetings, sessions, laps, etc.).
"""

import sqlite3
from contextlib import contextmanager
from config import DATABASE_PATH


# =============================================================================
# DATABASE CONNECTION HELPER
# =============================================================================

@contextmanager
def get_db_connection():
    """
    Creates a connection to the database.

    The @contextmanager decorator lets us use this with the 'with' statement,
    which automatically closes the connection when we're done (even if errors occur).

    Usage:
        with get_db_connection() as conn:
            # do database stuff here
        # connection is automatically closed after this block
    """
    conn = sqlite3.connect(DATABASE_PATH)

    # This makes query results return as dictionaries instead of tuples
    # So instead of row[0], row[1], we can use row['column_name']
    conn.row_factory = sqlite3.Row

    try:
        yield conn
    finally:
        conn.close()


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def initialize_database():
    """
    Creates all the tables we need if they don't already exist.

    This is safe to run multiple times - it won't delete existing data.
    The 'IF NOT EXISTS' clause means it only creates tables that are missing.
    """

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # -----------------------------------------------------------------
        # TABLE: meetings
        # -----------------------------------------------------------------
        # Stores information about race weekends (e.g., "Abu Dhabi Grand Prix")
        # Each row represents one race weekend
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meetings (
                -- meeting_key: Unique identifier from OpenF1 API
                meeting_key INTEGER PRIMARY KEY,

                -- meeting_name: Human-readable name like "Abu Dhabi Grand Prix"
                meeting_name TEXT NOT NULL,

                -- country_name: Country where the race takes place
                country_name TEXT,

                -- circuit_name: Name of the circuit (e.g., "Yas Marina Circuit")
                circuit_name TEXT,

                -- date_start: When the race weekend begins (ISO format)
                date_start TEXT,

                -- year: The F1 season year (2024, 2025, etc.)
                year INTEGER NOT NULL
            )
        """)

        # -----------------------------------------------------------------
        # TABLE: sessions
        # -----------------------------------------------------------------
        # Stores individual sessions within a race weekend
        # Each weekend has multiple sessions: FP1, FP2, FP3, Quali, Race
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                -- session_key: Unique identifier from OpenF1 API
                session_key INTEGER PRIMARY KEY,

                -- meeting_key: Links this session to its race weekend
                meeting_key INTEGER NOT NULL,

                -- session_name: Human-readable name like "Practice 1", "Qualifying"
                session_name TEXT NOT NULL,

                -- session_type: Standardized type (Practice, Qualifying, Race, etc.)
                session_type TEXT,

                -- date_start: When this session started
                date_start TEXT,

                -- date_end: When this session ended
                date_end TEXT,

                -- FOREIGN KEY: Ensures meeting_key matches a real meeting
                FOREIGN KEY (meeting_key) REFERENCES meetings(meeting_key)
            )
        """)

        # -----------------------------------------------------------------
        # TABLE: drivers
        # -----------------------------------------------------------------
        # Stores driver information for each session
        # Drivers can change teams between races, so we store per-session
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                -- id: Auto-generated unique ID for each record
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- driver_number: The car number (e.g., 1 for Verstappen, 44 for Hamilton)
                driver_number INTEGER NOT NULL,

                -- session_key: Links this driver record to a specific session
                session_key INTEGER NOT NULL,

                -- full_name: Driver's full name (e.g., "Max VERSTAPPEN")
                full_name TEXT,

                -- team_name: Team name (e.g., "Red Bull Racing")
                team_name TEXT,

                -- team_color: Hex color code for the team (for charts/display)
                team_color TEXT,

                -- name_acronym: Three-letter code (e.g., "VER", "HAM")
                name_acronym TEXT,

                -- Ensure we don't have duplicate driver entries per session
                UNIQUE(driver_number, session_key),

                FOREIGN KEY (session_key) REFERENCES sessions(session_key)
            )
        """)

        # -----------------------------------------------------------------
        # TABLE: laps
        # -----------------------------------------------------------------
        # Stores every lap time from every session
        # This is our main data table for pace analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS laps (
                -- id: Auto-generated unique ID for each lap record
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- session_key: Which session this lap belongs to
                session_key INTEGER NOT NULL,

                -- driver_number: Which driver set this lap
                driver_number INTEGER NOT NULL,

                -- lap_number: The lap number within the session (1, 2, 3, etc.)
                lap_number INTEGER NOT NULL,

                -- lap_duration: Total lap time in seconds (e.g., 83.456)
                lap_duration REAL,

                -- sector_1_duration: Time for sector 1 in seconds
                sector_1_duration REAL,

                -- sector_2_duration: Time for sector 2 in seconds
                sector_2_duration REAL,

                -- sector_3_duration: Time for sector 3 in seconds
                sector_3_duration REAL,

                -- speed_trap: Speed at the speed trap point (km/h)
                speed_trap REAL,

                -- is_pit_out_lap: True if this is a lap coming out of the pits
                -- Pit out laps are always slow and should be excluded from analysis
                is_pit_out_lap INTEGER DEFAULT 0,

                -- compound: Tire compound used (SOFT, MEDIUM, HARD, etc.)
                compound TEXT,

                -- tire_age: How many laps old the tires are
                tire_age INTEGER,

                -- is_valid_for_ranking: Whether to include this lap in pace calculations
                -- We mark outliers, pit laps, etc. as invalid (0 = invalid, 1 = valid)
                is_valid_for_ranking INTEGER DEFAULT 1,

                -- Ensure we don't have duplicate lap entries
                UNIQUE(session_key, driver_number, lap_number),

                FOREIGN KEY (session_key) REFERENCES sessions(session_key)
            )
        """)

        # -----------------------------------------------------------------
        # TABLE: stints
        # -----------------------------------------------------------------
        # Stores tire stint information (which tire compound for which laps)
        # A "stint" is the period between pit stops on one set of tires
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stints (
                -- id: Auto-generated unique ID
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- session_key: Which session this stint belongs to
                session_key INTEGER NOT NULL,

                -- driver_number: Which driver this stint belongs to
                driver_number INTEGER NOT NULL,

                -- stint_number: First stint = 1, after first pit = 2, etc.
                stint_number INTEGER NOT NULL,

                -- compound: Tire compound (SOFT, MEDIUM, HARD, etc.)
                compound TEXT,

                -- lap_start: First lap number of this stint
                lap_start INTEGER,

                -- lap_end: Last lap number of this stint
                lap_end INTEGER,

                -- tire_age_at_start: How old the tires were at stint start
                -- (Usually 0 for new tires, but can be higher for used tires)
                tire_age_at_start INTEGER DEFAULT 0,

                -- Ensure we don't have duplicate stint entries
                UNIQUE(session_key, driver_number, stint_number),

                FOREIGN KEY (session_key) REFERENCES sessions(session_key)
            )
        """)

        # -----------------------------------------------------------------
        # Create indexes for faster queries
        # -----------------------------------------------------------------
        # Indexes make database queries faster, like an index in a book
        # helps you find pages faster

        # Index for looking up laps by session
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_laps_session
            ON laps(session_key)
        """)

        # Index for looking up laps by driver
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_laps_driver
            ON laps(driver_number)
        """)

        # Index for looking up valid laps quickly
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_laps_valid
            ON laps(is_valid_for_ranking)
        """)

        # Index for looking up sessions by meeting
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sessions_meeting
            ON sessions(meeting_key)
        """)

        # Save all our changes to the database
        conn.commit()

        print("Database initialized successfully!")
        print(f"Database location: {DATABASE_PATH}")


# =============================================================================
# DATA INSERTION FUNCTIONS
# =============================================================================

def insert_meeting(meeting_data):
    """
    Inserts a race weekend (meeting) into the database.
    Uses INSERT OR REPLACE to update if the meeting already exists.

    Args:
        meeting_data: Dictionary with meeting information from the API

    Returns:
        The meeting_key of the inserted/updated meeting
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO meetings
            (meeting_key, meeting_name, country_name, circuit_name, date_start, year)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            meeting_data.get('meeting_key'),
            meeting_data.get('meeting_name'),
            meeting_data.get('country_name'),
            meeting_data.get('circuit_name'),
            meeting_data.get('date_start'),
            meeting_data.get('year')
        ))
        conn.commit()
        return meeting_data.get('meeting_key')


def insert_session(session_data):
    """
    Inserts a session (FP1, Quali, Race, etc.) into the database.

    Args:
        session_data: Dictionary with session information from the API

    Returns:
        The session_key of the inserted/updated session
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO sessions
            (session_key, meeting_key, session_name, session_type, date_start, date_end)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            session_data.get('session_key'),
            session_data.get('meeting_key'),
            session_data.get('session_name'),
            session_data.get('session_type'),
            session_data.get('date_start'),
            session_data.get('date_end')
        ))
        conn.commit()
        return session_data.get('session_key')


def insert_driver(driver_data, session_key):
    """
    Inserts a driver record for a specific session.

    Args:
        driver_data: Dictionary with driver information from the API
        session_key: The session this driver record belongs to
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO drivers
            (driver_number, session_key, full_name, team_name, team_color, name_acronym)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            driver_data.get('driver_number'),
            session_key,
            driver_data.get('full_name'),
            driver_data.get('team_name'),
            driver_data.get('team_colour'),  # Note: API uses British spelling
            driver_data.get('name_acronym')
        ))
        conn.commit()


def insert_lap(lap_data, session_key):
    """
    Inserts a single lap record into the database.

    Args:
        lap_data: Dictionary with lap information
        session_key: The session this lap belongs to
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO laps
            (session_key, driver_number, lap_number, lap_duration,
             sector_1_duration, sector_2_duration, sector_3_duration,
             speed_trap, is_pit_out_lap, compound, tire_age, is_valid_for_ranking)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_key,
            lap_data.get('driver_number'),
            lap_data.get('lap_number'),
            lap_data.get('lap_duration'),
            lap_data.get('duration_sector_1'),
            lap_data.get('duration_sector_2'),
            lap_data.get('duration_sector_3'),
            lap_data.get('st_speed'),  # Speed trap speed
            1 if lap_data.get('is_pit_out_lap') else 0,
            lap_data.get('compound'),
            lap_data.get('tire_age'),
            1 if lap_data.get('is_valid_for_ranking', True) else 0
        ))
        conn.commit()


def insert_stint(stint_data, session_key):
    """
    Inserts a tire stint record into the database.

    Args:
        stint_data: Dictionary with stint information from the API
        session_key: The session this stint belongs to
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO stints
            (session_key, driver_number, stint_number, compound,
             lap_start, lap_end, tire_age_at_start)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            session_key,
            stint_data.get('driver_number'),
            stint_data.get('stint_number'),
            stint_data.get('compound'),
            stint_data.get('lap_start'),
            stint_data.get('lap_end'),
            stint_data.get('tyre_age_at_start', 0)  # Note: API uses British spelling
        ))
        conn.commit()


def bulk_insert_laps(laps_list, session_key):
    """
    Inserts multiple laps at once (more efficient than one at a time).

    Args:
        laps_list: List of lap dictionaries
        session_key: The session these laps belong to
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Prepare all the data for bulk insert
        lap_records = []
        for lap in laps_list:
            lap_records.append((
                session_key,
                lap.get('driver_number'),
                lap.get('lap_number'),
                lap.get('lap_duration'),
                lap.get('duration_sector_1'),
                lap.get('duration_sector_2'),
                lap.get('duration_sector_3'),
                lap.get('st_speed'),
                1 if lap.get('is_pit_out_lap') else 0,
                lap.get('compound'),
                lap.get('tire_age'),
                1 if lap.get('is_valid_for_ranking', True) else 0
            ))

        # Insert all laps in one operation
        cursor.executemany("""
            INSERT OR REPLACE INTO laps
            (session_key, driver_number, lap_number, lap_duration,
             sector_1_duration, sector_2_duration, sector_3_duration,
             speed_trap, is_pit_out_lap, compound, tire_age, is_valid_for_ranking)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, lap_records)

        conn.commit()


# =============================================================================
# DATA QUERY FUNCTIONS
# =============================================================================

# =============================================================================
# MEETING AND SESSION QUERY FUNCTIONS
# =============================================================================

def get_meeting_by_key(meeting_key):
    """
    Get a single meeting (race weekend) by its key.

    Args:
        meeting_key: The meeting's unique identifier

    Returns:
        Dictionary with meeting details or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT meeting_key, meeting_name, country_name, circuit_name, date_start, year
            FROM meetings
            WHERE meeting_key = ?
        """, (meeting_key,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None


def get_sessions_for_meeting(meeting_key):
    """
    Get all sessions for a specific meeting.

    Args:
        meeting_key: The meeting's unique identifier

    Returns:
        List of session dictionaries, ordered by date
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT session_key, meeting_key, session_name, session_type, date_start, date_end
            FROM sessions
            WHERE meeting_key = ?
            ORDER BY date_start ASC
        """, (meeting_key,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_session_by_meeting_and_type(meeting_key, session_name):
    """
    Get a specific session by meeting key and session name.

    Args:
        meeting_key: The meeting's unique identifier
        session_name: The session name (e.g., "Practice 1", "Qualifying", "Race")

    Returns:
        Dictionary with session details or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT session_key, meeting_key, session_name, session_type, date_start, date_end
            FROM sessions
            WHERE meeting_key = ? AND session_name = ?
        """, (meeting_key, session_name))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None


def get_statistics():
    """
    Returns a summary of what data we have in the database.
    Useful for checking that data collection worked correctly.

    Returns:
        Dictionary with counts of meetings, sessions, laps, and unique drivers
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Count meetings
        cursor.execute("SELECT COUNT(*) as count FROM meetings")
        meetings_count = cursor.fetchone()['count']

        # Count sessions
        cursor.execute("SELECT COUNT(*) as count FROM sessions")
        sessions_count = cursor.fetchone()['count']

        # Count total laps
        cursor.execute("SELECT COUNT(*) as count FROM laps")
        laps_count = cursor.fetchone()['count']

        # Count valid laps only
        cursor.execute("SELECT COUNT(*) as count FROM laps WHERE is_valid_for_ranking = 1")
        valid_laps_count = cursor.fetchone()['count']

        # Count unique drivers (by full_name to avoid counting same driver multiple times)
        cursor.execute("SELECT COUNT(DISTINCT full_name) as count FROM drivers")
        drivers_count = cursor.fetchone()['count']

        # Count teams
        cursor.execute("SELECT COUNT(DISTINCT team_name) as count FROM drivers")
        teams_count = cursor.fetchone()['count']

        return {
            'meetings': meetings_count,
            'sessions': sessions_count,
            'total_laps': laps_count,
            'valid_laps': valid_laps_count,
            'unique_drivers': drivers_count,
            'teams': teams_count
        }


def meeting_exists(meeting_key):
    """
    Checks if we already have data for a specific meeting.
    Useful to avoid re-downloading data we already have.

    Args:
        meeting_key: The meeting's unique identifier

    Returns:
        True if meeting exists, False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as count FROM meetings WHERE meeting_key = ?",
            (meeting_key,)
        )
        return cursor.fetchone()['count'] > 0


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # If this file is run directly, initialize the database
    print("Initializing F1 Pecking Order database...")
    initialize_database()

    # Show current statistics
    stats = get_statistics()
    print("\nCurrent database statistics:")
    print(f"  - Meetings: {stats['meetings']}")
    print(f"  - Sessions: {stats['sessions']}")
    print(f"  - Total laps: {stats['total_laps']}")
    print(f"  - Valid laps: {stats['valid_laps']}")
    print(f"  - Unique drivers: {stats['unique_drivers']}")
    print(f"  - Teams: {stats['teams']}")
