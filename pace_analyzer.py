"""
F1 Pecking Order - Pace Analyzer

This is the heart of the project - the algorithm that calculates "TRUE PACE"
by adjusting raw lap times to account for variables that make them misleading.

RAW LAP TIMES ARE MISLEADING BECAUSE:
1. Tire compound: Soft tires are ~1 second faster than Hard tires
2. Fuel load: A full tank makes the car ~3 seconds slower than empty
3. Tire age: Old tires are slower than fresh tires
4. Session type: Qualifying uses low fuel, races use high fuel

This analyzer normalizes all lap times to a common baseline so we can
fairly compare pace across different conditions.

BASELINE: We normalize everything to "Medium tires, 50kg fuel, fresh tires"
"""

import sqlite3
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from statistics import mean, median, stdev

from config import (
    DATABASE_PATH,
    TIRE_COMPOUND_DELTAS,
    TIRE_DEGRADATION_PER_LAP,
    FUEL_EFFECT_PER_KG,
    FUEL_CONSUMPTION_PER_LAP,
    QUALI_FUEL_LOAD_KG,
    RACE_START_FUEL_LOAD_KG,
    SESSION_WEIGHTS,
)
from database import get_db_connection


# =============================================================================
# PACE NORMALIZATION FUNCTIONS
# =============================================================================

def normalize_lap_time(
    raw_lap_time: float,
    compound: str,
    tire_age: int,
    session_type: str,
    lap_number: int,
    total_laps: int = 58
) -> float:
    """
    Converts a raw lap time to a "normalized" lap time that accounts for
    tire compound, tire age, and estimated fuel load.

    The normalized time represents what the lap time WOULD HAVE BEEN if:
    - The driver was on MEDIUM tires
    - The tires were fresh (0 laps old)
    - The car had 50kg of fuel (middle of a race)

    Args:
        raw_lap_time: The actual recorded lap time in seconds
        compound: Tire compound (SOFT, MEDIUM, HARD, etc.)
        tire_age: How many laps old the tires are
        session_type: Type of session (Practice, Qualifying, Race)
        lap_number: Which lap of the session this is
        total_laps: Total laps in the race (for fuel calculation)

    Returns:
        Normalized lap time in seconds
    """
    if raw_lap_time is None:
        return None

    normalized_time = raw_lap_time

    # -----------------------------------------------------------------
    # ADJUSTMENT 1: Tire Compound
    # -----------------------------------------------------------------
    # Soft tires are faster, so we ADD time to soft lap times to normalize
    # Hard tires are slower, so we SUBTRACT time from hard lap times
    # We're normalizing to MEDIUM as baseline

    compound_delta = TIRE_COMPOUND_DELTAS.get(compound, 0)
    # compound_delta is negative for soft (faster), so we subtract it
    # This ADDS time to soft laps, making them comparable to medium
    # For MEDIUM compound, delta is -0.5, so we add 0.5 to normalize to hard baseline
    # Actually, let's normalize to MEDIUM as baseline (delta = 0 for medium)

    # Recalculate: we want MEDIUM as baseline
    # SOFT is -1.0 (1 sec faster than hard) -> compared to medium, soft is 0.5 faster
    # MEDIUM is -0.5 -> this is our baseline
    # HARD is 0.0 -> 0.5 slower than medium

    medium_delta = TIRE_COMPOUND_DELTAS.get("MEDIUM", -0.5)
    compound_adjustment = compound_delta - medium_delta
    # Now: SOFT = -1.0 - (-0.5) = -0.5 (soft is 0.5 faster than medium)
    #      MEDIUM = -0.5 - (-0.5) = 0 (baseline)
    #      HARD = 0 - (-0.5) = 0.5 (hard is 0.5 slower than medium)

    # To normalize: subtract the adjustment (so soft laps get slower, hard laps get faster)
    normalized_time -= compound_adjustment

    # -----------------------------------------------------------------
    # ADJUSTMENT 2: Tire Degradation
    # -----------------------------------------------------------------
    # Older tires are slower. We subtract the degradation to normalize
    # to fresh tire pace.

    if tire_age is not None and tire_age > 0:
        degradation = tire_age * TIRE_DEGRADATION_PER_LAP
        normalized_time -= degradation

    # -----------------------------------------------------------------
    # ADJUSTMENT 3: Fuel Load (Race sessions only)
    # -----------------------------------------------------------------
    # In races, cars start heavy and get lighter. We estimate fuel load
    # based on lap number and normalize to a middle-of-race fuel load.

    if session_type == "Race":
        # Estimate current fuel load
        fuel_burned = lap_number * FUEL_CONSUMPTION_PER_LAP
        current_fuel = max(0, RACE_START_FUEL_LOAD_KG - fuel_burned)

        # Normalize to 50kg (roughly mid-race)
        target_fuel = 50
        fuel_difference = current_fuel - target_fuel

        # More fuel = slower, so subtract the fuel effect
        fuel_adjustment = fuel_difference * FUEL_EFFECT_PER_KG
        normalized_time -= fuel_adjustment

    elif session_type == "Sprint":
        # Sprint races have about 1/3 the fuel
        sprint_start_fuel = RACE_START_FUEL_LOAD_KG / 3
        fuel_burned = lap_number * FUEL_CONSUMPTION_PER_LAP
        current_fuel = max(0, sprint_start_fuel - fuel_burned)

        target_fuel = 50
        fuel_difference = current_fuel - target_fuel
        fuel_adjustment = fuel_difference * FUEL_EFFECT_PER_KG
        normalized_time -= fuel_adjustment

    elif session_type in ["Qualifying", "Sprint Qualifying"]:
        # Qualifying uses very low fuel - normalize this too
        fuel_difference = QUALI_FUEL_LOAD_KG - 50
        fuel_adjustment = fuel_difference * FUEL_EFFECT_PER_KG
        normalized_time -= fuel_adjustment

    # Practice sessions have variable fuel loads we can't determine,
    # so we don't adjust them (assume roughly mid-fuel)

    return normalized_time


# =============================================================================
# DATA RETRIEVAL FUNCTIONS
# =============================================================================

def get_all_valid_laps() -> List[Dict]:
    """
    Retrieves all valid laps from the database with driver and session info.

    Returns:
        List of lap dictionaries with all relevant fields
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                l.session_key,
                l.driver_number,
                l.lap_number,
                l.lap_duration,
                l.compound,
                l.tire_age,
                s.session_type,
                s.session_name,
                s.meeting_key,
                d.full_name as driver_name,
                d.team_name,
                d.team_color,
                d.name_acronym,
                m.meeting_name,
                m.circuit_name
            FROM laps l
            JOIN sessions s ON l.session_key = s.session_key
            JOIN drivers d ON l.driver_number = d.driver_number AND l.session_key = d.session_key
            JOIN meetings m ON s.meeting_key = m.meeting_key
            WHERE l.is_valid_for_ranking = 1
            AND l.lap_duration IS NOT NULL
            ORDER BY m.date_start DESC, s.session_key, l.driver_number, l.lap_number
        """)

        rows = cursor.fetchall()

        # Convert to list of dictionaries
        laps = []
        for row in rows:
            laps.append({
                'session_key': row['session_key'],
                'driver_number': row['driver_number'],
                'lap_number': row['lap_number'],
                'lap_duration': row['lap_duration'],
                'compound': row['compound'],
                'tire_age': row['tire_age'],
                'session_type': row['session_type'],
                'session_name': row['session_name'],
                'meeting_key': row['meeting_key'],
                'driver_name': row['driver_name'],
                'team_name': row['team_name'],
                'team_color': row['team_color'],
                'name_acronym': row['name_acronym'],
                'meeting_name': row['meeting_name'],
                'circuit_name': row['circuit_name'],
            })

        return laps


def get_session_total_laps(session_key: int) -> int:
    """
    Gets the maximum lap number in a session (for fuel calculations).
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(lap_number) as max_lap FROM laps WHERE session_key = ?",
            (session_key,)
        )
        result = cursor.fetchone()
        return result['max_lap'] if result['max_lap'] else 58


# =============================================================================
# PACE CALCULATION FUNCTIONS
# =============================================================================

def calculate_driver_pace_scores() -> Dict:
    """
    Calculates normalized pace scores for each driver.

    For each driver, we:
    1. Get all their valid laps
    2. Normalize each lap time
    3. Take the best N laps (to represent their peak pace)
    4. Weight by session type
    5. Calculate an overall pace score

    Returns:
        Dictionary with driver pace data
    """
    print("Calculating driver pace scores...")

    # Get all valid laps
    all_laps = get_all_valid_laps()
    print(f"  Processing {len(all_laps)} valid laps...")

    if not all_laps:
        return {}

    # Cache for session total laps
    session_totals = {}

    # Normalize all lap times
    for lap in all_laps:
        session_key = lap['session_key']

        # Get total laps for this session (cached)
        if session_key not in session_totals:
            session_totals[session_key] = get_session_total_laps(session_key)

        lap['normalized_time'] = normalize_lap_time(
            raw_lap_time=lap['lap_duration'],
            compound=lap['compound'],
            tire_age=lap['tire_age'],
            session_type=lap['session_type'],
            lap_number=lap['lap_number'],
            total_laps=session_totals[session_key]
        )

    # Group laps by driver
    driver_laps = defaultdict(list)
    for lap in all_laps:
        if lap['normalized_time'] is not None:
            driver_laps[lap['driver_name']].append(lap)

    # Calculate pace score for each driver
    driver_scores = {}

    for driver_name, laps in driver_laps.items():
        if not laps:
            continue

        # Get driver info from first lap
        first_lap = laps[0]
        team_name = first_lap['team_name']
        team_color = first_lap['team_color']
        name_acronym = first_lap['name_acronym']

        # Group laps by session type for weighted scoring
        session_laps = defaultdict(list)
        for lap in laps:
            session_laps[lap['session_type']].append(lap['normalized_time'])

        # Calculate weighted pace score
        # We use the best 10% of laps from each session type
        weighted_times = []
        session_details = {}

        for session_type, times in session_laps.items():
            if not times:
                continue

            # Sort times (fastest first)
            sorted_times = sorted(times)

            # Take best 10% of laps (minimum 3, maximum 20)
            num_best = max(3, min(20, len(sorted_times) // 10))
            best_times = sorted_times[:num_best]

            # Get session weight
            weight = SESSION_WEIGHTS.get(session_type, 0.5)

            # Calculate average of best times
            avg_best = mean(best_times)

            # Store session details
            session_details[session_type] = {
                'average_pace': avg_best,
                'best_lap': min(times),
                'lap_count': len(times),
                'weight': weight,
            }

            # Add weighted times
            for t in best_times:
                weighted_times.append((t, weight))

        if not weighted_times:
            continue

        # Calculate weighted average pace
        total_weight = sum(w for _, w in weighted_times)
        weighted_avg = sum(t * w for t, w in weighted_times) / total_weight

        # Calculate consistency (standard deviation of best laps)
        all_best_times = [t for t, _ in weighted_times]
        consistency = stdev(all_best_times) if len(all_best_times) > 1 else 0

        driver_scores[driver_name] = {
            'driver_name': driver_name,
            'team_name': team_name,
            'team_color': team_color or '#888888',
            'name_acronym': name_acronym,
            'pace_score': weighted_avg,
            'consistency': consistency,
            'total_laps': len(laps),
            'session_details': session_details,
            'best_normalized_lap': min(lap['normalized_time'] for lap in laps),
        }

    return driver_scores


def calculate_team_pace_scores(driver_scores: Dict) -> Dict:
    """
    Calculates team pace scores by averaging their drivers' scores.

    Args:
        driver_scores: Dictionary of driver pace scores

    Returns:
        Dictionary with team pace data
    """
    print("Calculating team pace scores...")

    # Group drivers by team
    team_drivers = defaultdict(list)
    for driver_name, data in driver_scores.items():
        team_name = data['team_name']
        if team_name:
            team_drivers[team_name].append(data)

    # Calculate team scores
    team_scores = {}

    for team_name, drivers in team_drivers.items():
        if not drivers:
            continue

        # Use the faster driver's pace as the team pace
        # (This represents the team's potential)
        best_driver = min(drivers, key=lambda d: d['pace_score'])

        # Also calculate team average
        team_avg = mean(d['pace_score'] for d in drivers)

        # Get team color from drivers
        team_color = drivers[0]['team_color'] or '#888888'

        team_scores[team_name] = {
            'team_name': team_name,
            'team_color': team_color,
            'pace_score': best_driver['pace_score'],  # Best driver's pace
            'team_average': team_avg,
            'driver_gap': max(d['pace_score'] for d in drivers) - best_driver['pace_score'],
            'drivers': [d['driver_name'] for d in drivers],
            'total_laps': sum(d['total_laps'] for d in drivers),
        }

    return team_scores


def calculate_rankings() -> Tuple[List[Dict], List[Dict]]:
    """
    Main function that calculates all rankings.

    Returns:
        Tuple of (driver_rankings, team_rankings)
        Each is a sorted list from fastest to slowest
    """
    print("\n" + "=" * 60)
    print("CALCULATING PACE RANKINGS")
    print("=" * 60)

    # Calculate driver scores
    driver_scores = calculate_driver_pace_scores()

    if not driver_scores:
        print("No data available for rankings!")
        return [], []

    # Calculate team scores
    team_scores = calculate_team_pace_scores(driver_scores)

    # Sort drivers by pace (fastest first = lowest time)
    driver_rankings = sorted(
        driver_scores.values(),
        key=lambda d: d['pace_score']
    )

    # Add position and gap to leader
    if driver_rankings:
        leader_pace = driver_rankings[0]['pace_score']
        for i, driver in enumerate(driver_rankings):
            driver['position'] = i + 1
            driver['gap_to_leader'] = driver['pace_score'] - leader_pace

    # Sort teams by pace
    team_rankings = sorted(
        team_scores.values(),
        key=lambda t: t['pace_score']
    )

    # Add position and gap to leader
    if team_rankings:
        leader_pace = team_rankings[0]['pace_score']
        for i, team in enumerate(team_rankings):
            team['position'] = i + 1
            team['gap_to_leader'] = team['pace_score'] - leader_pace

    print(f"\nRankings calculated for {len(driver_rankings)} drivers and {len(team_rankings)} teams")

    return driver_rankings, team_rankings


def get_meeting_breakdown() -> List[Dict]:
    """
    Gets pace breakdown by race weekend (meeting).

    Returns:
        List of meetings with driver pace data for each
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get all meetings
        cursor.execute("""
            SELECT meeting_key, meeting_name, circuit_name, date_start
            FROM meetings
            ORDER BY date_start DESC
        """)
        meetings = [dict(row) for row in cursor.fetchall()]

    # Get laps for each meeting
    all_laps = get_all_valid_laps()

    # Group laps by meeting
    meeting_laps = defaultdict(list)
    for lap in all_laps:
        meeting_laps[lap['meeting_key']].append(lap)

    # Calculate pace for each meeting
    for meeting in meetings:
        meeting_key = meeting['meeting_key']
        laps = meeting_laps.get(meeting_key, [])

        # Group by driver and calculate average pace
        driver_times = defaultdict(list)
        driver_info = {}

        for lap in laps:
            # Simple normalization for meeting view
            normalized = normalize_lap_time(
                lap['lap_duration'],
                lap['compound'],
                lap['tire_age'],
                lap['session_type'],
                lap['lap_number'],
            )
            if normalized:
                driver_times[lap['driver_name']].append(normalized)
                driver_info[lap['driver_name']] = {
                    'team_name': lap['team_name'],
                    'team_color': lap['team_color'],
                    'name_acronym': lap['name_acronym'],
                }

        # Calculate best pace for each driver
        driver_paces = []
        for driver_name, times in driver_times.items():
            sorted_times = sorted(times)
            best_times = sorted_times[:max(3, len(sorted_times) // 10)]
            avg_pace = mean(best_times)

            info = driver_info[driver_name]
            driver_paces.append({
                'driver_name': driver_name,
                'name_acronym': info['name_acronym'],
                'team_name': info['team_name'],
                'team_color': info['team_color'] or '#888888',
                'pace': avg_pace,
                'lap_count': len(times),
            })

        # Sort by pace
        driver_paces.sort(key=lambda d: d['pace'])

        # Add gaps
        if driver_paces:
            leader_pace = driver_paces[0]['pace']
            for i, d in enumerate(driver_paces):
                d['position'] = i + 1
                d['gap'] = d['pace'] - leader_pace

        meeting['driver_paces'] = driver_paces

    return meetings


# =============================================================================
# SESSION-LEVEL PACE ANALYSIS
# =============================================================================

def get_session_pecking_order(session_key: int) -> Dict:
    """
    Calculate pecking order for a single session with detailed stats.

    Returns a dictionary with:
    - session info
    - driver rankings with sectors, tires, lap counts
    - tire usage summary

    Args:
        session_key: The session's unique identifier

    Returns:
        Dictionary with session details and driver rankings
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get session info
        cursor.execute("""
            SELECT s.session_key, s.session_name, s.session_type, s.date_start,
                   m.meeting_key, m.meeting_name, m.circuit_name, m.country_name
            FROM sessions s
            JOIN meetings m ON s.meeting_key = m.meeting_key
            WHERE s.session_key = ?
        """, (session_key,))
        session_row = cursor.fetchone()

        if not session_row:
            return None

        session_info = dict(session_row)

        # Get all valid laps for this session with driver info
        cursor.execute("""
            SELECT
                l.driver_number,
                l.lap_number,
                l.lap_duration,
                l.sector_1_duration,
                l.sector_2_duration,
                l.sector_3_duration,
                l.speed_trap,
                l.compound,
                l.tire_age,
                d.full_name as driver_name,
                d.team_name,
                d.team_color,
                d.name_acronym
            FROM laps l
            JOIN drivers d ON l.driver_number = d.driver_number AND l.session_key = d.session_key
            WHERE l.session_key = ? AND l.is_valid_for_ranking = 1 AND l.lap_duration IS NOT NULL
            ORDER BY l.driver_number, l.lap_number
        """, (session_key,))

        laps = [dict(row) for row in cursor.fetchall()]

    if not laps:
        return {
            'session': session_info,
            'driver_rankings': [],
            'tire_summary': [],
            'stats': {'driver_count': 0, 'lap_count': 0}
        }

    # Get total laps for fuel calculation
    total_laps = max(lap['lap_number'] for lap in laps)
    session_type = session_info['session_type']

    # Process laps by driver
    driver_data = defaultdict(lambda: {
        'laps': [],
        'best_lap': None,
        'best_sectors': [None, None, None],
        'compounds_used': set(),
        'speed_traps': [],
    })

    for lap in laps:
        driver_name = lap['driver_name']

        # Normalize the lap time
        normalized = normalize_lap_time(
            raw_lap_time=lap['lap_duration'],
            compound=lap['compound'],
            tire_age=lap['tire_age'],
            session_type=session_type,
            lap_number=lap['lap_number'],
            total_laps=total_laps
        )

        lap['normalized_time'] = normalized
        driver_data[driver_name]['laps'].append(lap)

        # Track best lap
        if normalized and (driver_data[driver_name]['best_lap'] is None or
                          normalized < driver_data[driver_name]['best_lap']['normalized_time']):
            driver_data[driver_name]['best_lap'] = lap

        # Track best sectors
        for i, sector_key in enumerate(['sector_1_duration', 'sector_2_duration', 'sector_3_duration']):
            sector_time = lap[sector_key]
            if sector_time:
                current_best = driver_data[driver_name]['best_sectors'][i]
                if current_best is None or sector_time < current_best:
                    driver_data[driver_name]['best_sectors'][i] = sector_time

        # Track compounds
        if lap['compound']:
            driver_data[driver_name]['compounds_used'].add(lap['compound'])

        # Track speed traps
        if lap['speed_trap']:
            driver_data[driver_name]['speed_traps'].append(lap['speed_trap'])

    # Build driver rankings
    driver_rankings = []
    for driver_name, data in driver_data.items():
        if not data['best_lap']:
            continue

        best_lap = data['best_lap']

        # Get driver info from best lap
        driver_entry = {
            'driver_name': driver_name,
            'name_acronym': best_lap['name_acronym'],
            'team_name': best_lap['team_name'],
            'team_color': best_lap['team_color'] or '#888888',
            'best_lap': best_lap['lap_duration'],
            'normalized_time': best_lap['normalized_time'],
            'sector_1': data['best_sectors'][0],
            'sector_2': data['best_sectors'][1],
            'sector_3': data['best_sectors'][2],
            'compound': best_lap['compound'],
            'lap_count': len(data['laps']),
            'max_speed': max(data['speed_traps']) if data['speed_traps'] else None,
            'compounds_used': list(data['compounds_used']),
        }

        driver_rankings.append(driver_entry)

    # Sort by normalized time
    driver_rankings.sort(key=lambda d: d['normalized_time'] if d['normalized_time'] else float('inf'))

    # Add position and gap
    if driver_rankings:
        leader_time = driver_rankings[0]['normalized_time']
        for i, driver in enumerate(driver_rankings):
            driver['position'] = i + 1
            driver['gap_to_leader'] = driver['normalized_time'] - leader_time if driver['normalized_time'] else None

    # Calculate tire summary
    tire_summary = defaultdict(lambda: {'count': 0, 'drivers': set()})
    for driver_name, data in driver_data.items():
        for compound in data['compounds_used']:
            tire_summary[compound]['count'] += 1
            tire_summary[compound]['drivers'].add(driver_name)

    tire_summary_list = [
        {'compound': compound, 'driver_count': info['count']}
        for compound, info in tire_summary.items()
    ]
    tire_summary_list.sort(key=lambda x: x['compound'])

    return {
        'session': session_info,
        'driver_rankings': driver_rankings,
        'tire_summary': tire_summary_list,
        'stats': {
            'driver_count': len(driver_rankings),
            'lap_count': len(laps),
        }
    }


def get_meeting_pecking_order(meeting_key: int) -> Dict:
    """
    Calculate combined weekend pecking order and session summaries.

    Args:
        meeting_key: The meeting's unique identifier

    Returns:
        Dictionary with meeting info, overall rankings, and session summaries
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get meeting info
        cursor.execute("""
            SELECT meeting_key, meeting_name, country_name, circuit_name, date_start, year
            FROM meetings
            WHERE meeting_key = ?
        """, (meeting_key,))
        meeting_row = cursor.fetchone()

        if not meeting_row:
            return None

        meeting_info = dict(meeting_row)

        # Get all sessions for this meeting
        cursor.execute("""
            SELECT session_key, session_name, session_type, date_start
            FROM sessions
            WHERE meeting_key = ?
            ORDER BY date_start ASC
        """, (meeting_key,))
        sessions = [dict(row) for row in cursor.fetchall()]

    # Get session summaries with top 3
    session_summaries = []
    all_driver_times = defaultdict(list)
    driver_info = {}

    for session in sessions:
        session_data = get_session_pecking_order(session['session_key'])
        if session_data and session_data['driver_rankings']:
            # Get top 3 for preview
            top_3 = session_data['driver_rankings'][:3]

            session_summaries.append({
                'session_key': session['session_key'],
                'session_name': session['session_name'],
                'session_type': session['session_type'],
                'top_3': top_3,
                'driver_count': session_data['stats']['driver_count'],
                'lap_count': session_data['stats']['lap_count'],
            })

            # Collect all driver times for overall ranking
            for driver in session_data['driver_rankings']:
                all_driver_times[driver['driver_name']].append({
                    'normalized_time': driver['normalized_time'],
                    'session_type': session['session_type'],
                })
                driver_info[driver['driver_name']] = {
                    'name_acronym': driver['name_acronym'],
                    'team_name': driver['team_name'],
                    'team_color': driver['team_color'],
                }

    # Calculate overall weekend ranking
    overall_rankings = []
    for driver_name, times in all_driver_times.items():
        # Weight times by session type
        weighted_times = []
        for t in times:
            weight = SESSION_WEIGHTS.get(t['session_type'], 0.5)
            weighted_times.append((t['normalized_time'], weight))

        # Calculate weighted average
        total_weight = sum(w for _, w in weighted_times)
        weighted_avg = sum(t * w for t, w in weighted_times) / total_weight if total_weight > 0 else 0

        info = driver_info[driver_name]
        overall_rankings.append({
            'driver_name': driver_name,
            'name_acronym': info['name_acronym'],
            'team_name': info['team_name'],
            'team_color': info['team_color'],
            'pace': weighted_avg,
            'session_count': len(times),
        })

    # Sort by pace
    overall_rankings.sort(key=lambda d: d['pace'])

    # Add position and gap
    if overall_rankings:
        leader_pace = overall_rankings[0]['pace']
        for i, driver in enumerate(overall_rankings):
            driver['position'] = i + 1
            driver['gap'] = driver['pace'] - leader_pace

    return {
        'meeting': meeting_info,
        'overall_rankings': overall_rankings,
        'session_summaries': session_summaries,
        'sessions': sessions,
    }


# =============================================================================
# DISPLAY FUNCTIONS (for command line)
# =============================================================================

def print_rankings():
    """
    Prints the rankings to the console in a nice format.
    """
    driver_rankings, team_rankings = calculate_rankings()

    if not driver_rankings:
        print("No rankings data available!")
        return

    # Print team rankings
    print("\n" + "=" * 60)
    print("TEAM PECKING ORDER (True Pace)")
    print("=" * 60)
    print(f"{'Pos':<4} {'Team':<25} {'Pace':>10} {'Gap':>10}")
    print("-" * 60)

    for team in team_rankings:
        gap_str = f"+{team['gap_to_leader']:.3f}" if team['gap_to_leader'] > 0 else "LEADER"
        print(f"{team['position']:<4} {team['team_name']:<25} {team['pace_score']:>10.3f} {gap_str:>10}")

    # Print driver rankings
    print("\n" + "=" * 60)
    print("DRIVER PECKING ORDER (True Pace)")
    print("=" * 60)
    print(f"{'Pos':<4} {'Driver':<25} {'Team':<20} {'Pace':>10} {'Gap':>10}")
    print("-" * 70)

    for driver in driver_rankings:
        gap_str = f"+{driver['gap_to_leader']:.3f}" if driver['gap_to_leader'] > 0 else "LEADER"
        team_short = driver['team_name'][:18] if driver['team_name'] else "Unknown"
        print(f"{driver['position']:<4} {driver['driver_name']:<25} {team_short:<20} {driver['pace_score']:>10.3f} {gap_str:>10}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # When run directly, print the rankings
    print_rankings()
