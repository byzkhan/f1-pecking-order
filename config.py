"""
F1 Pecking Order - Configuration Settings

This file contains all the settings and constants used throughout the project.
Think of it as the "control panel" where you can adjust how the system works.
"""

import os

# =============================================================================
# DATABASE SETTINGS
# =============================================================================

# Where to store our SQLite database file
# SQLite is a simple database that stores everything in a single file
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "f1_data.db")


# =============================================================================
# API SETTINGS
# =============================================================================

# The base URL for the OpenF1 API - this is where we fetch all F1 data from
OPENF1_API_BASE_URL = "https://api.openf1.org/v1"

# How long to wait between API calls (in seconds)
# This is to be respectful to the free API and avoid overwhelming their servers
API_REQUEST_DELAY = 0.5

# How long to wait for an API response before giving up (in seconds)
API_TIMEOUT = 30


# =============================================================================
# TIRE COMPOUND TIME DELTAS
# =============================================================================

# These values represent approximately how much faster (in seconds) each
# compound is compared to the HARD tire on a typical F1 circuit.
#
# For example, SOFT tires are typically about 1.0-1.2 seconds faster per lap
# than HARD tires on the same fuel load.
#
# These are approximate values based on F1 data analysis. They vary by circuit,
# temperature, and other factors, but these are reasonable averages.

TIRE_COMPOUND_DELTAS = {
    # Compound name: seconds faster than HARD (negative = faster)
    "SOFT": -1.0,      # Soft tires are ~1.0 second faster than hard
    "MEDIUM": -0.5,    # Medium tires are ~0.5 seconds faster than hard
    "HARD": 0.0,       # Hard is our baseline (0 delta)
    "INTERMEDIATE": 0.0,  # Wet tires - can't really compare to dry
    "WET": 0.0,        # Full wet tires - can't compare to dry
}

# How much time a tire loses per lap of age (tire degradation)
# Tires get slower as they wear out - approximately 0.03-0.05 seconds per lap
TIRE_DEGRADATION_PER_LAP = 0.03  # seconds


# =============================================================================
# FUEL EFFECT
# =============================================================================

# F1 cars start a race with about 110kg of fuel and burn about 1.5-2kg per lap.
# Each kg of fuel makes the car approximately 0.03-0.035 seconds slower per lap.
#
# This means a car at the START of a race (full fuel) is roughly 3+ seconds
# slower than at the END (nearly empty).

FUEL_EFFECT_PER_KG = 0.033  # seconds per kg of fuel

# Approximate fuel consumption per lap (kg)
FUEL_CONSUMPTION_PER_LAP = 1.8

# Approximate starting fuel load in qualifying (very low - just enough for out lap + hot lap)
QUALI_FUEL_LOAD_KG = 5

# Approximate starting fuel load in race (full tank)
RACE_START_FUEL_LOAD_KG = 110


# =============================================================================
# SESSION TYPE WEIGHTS FOR RANKING
# =============================================================================

# When calculating overall pace rankings, we weight different session types
# differently. Qualifying shows single-lap pace, while race shows race pace.
#
# Higher weight = more importance in the final ranking calculation

SESSION_WEIGHTS = {
    "Practice 1": 0.5,    # FP1 - Often used for testing, less representative
    "Practice 2": 0.7,    # FP2 - Usually more representative race simulations
    "Practice 3": 0.6,    # FP3 - Qualifying preparation, shorter runs
    "Sprint Qualifying": 0.8,  # Sprint quali - competitive but shorter
    "Sprint": 0.8,        # Sprint race - competitive but shorter distance
    "Qualifying": 1.0,    # Full qualifying - best single-lap pace indicator
    "Race": 1.0,          # Race - best race pace indicator
}


# =============================================================================
# LAP VALIDITY SETTINGS
# =============================================================================

# Laps that are much slower than the average are probably outliers
# (pit stops, yellow flags, accidents, etc.)
# We mark laps as invalid if they're more than this percentage slower than average
LAP_TIME_OUTLIER_THRESHOLD = 1.5  # 50% slower than session average = invalid

# Minimum lap time as a sanity check (no F1 lap is under 60 seconds)
MIN_VALID_LAP_TIME = 60.0  # seconds

# Maximum lap time as a sanity check (any lap over 3 minutes is clearly an issue)
MAX_VALID_LAP_TIME = 180.0  # seconds


# =============================================================================
# DATA COLLECTION SETTINGS
# =============================================================================

# Which year to collect data for
TARGET_YEAR = 2025

# How many recent race weekends to fetch
# Start with 3 to test the system, then increase once working
NUM_RECENT_MEETINGS = 3
