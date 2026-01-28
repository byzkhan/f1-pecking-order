"""
F1 Pecking Order - Web Application

This is the Flask web application that displays the F1 pace rankings.
Flask is a simple Python web framework that lets us create web pages easily.

To run the website locally:
    python app.py

Then open http://localhost:5000 in your browser.
"""

from flask import Flask, render_template, jsonify, abort
from pace_analyzer import (
    calculate_rankings,
    get_meeting_breakdown,
    get_session_pecking_order,
    get_meeting_pecking_order,
)
from database import (
    get_statistics,
    initialize_database,
    get_meeting_by_key,
    get_sessions_for_meeting,
    get_session_by_meeting_and_type,
)

# Create the Flask application
# __name__ tells Flask where to find templates and static files
app = Flask(__name__)


# =============================================================================
# SESSION SLUG MAPPING
# =============================================================================

# Maps URL slugs to database session names
SESSION_SLUGS = {
    'fp1': 'Practice 1',
    'fp2': 'Practice 2',
    'fp3': 'Practice 3',
    'qualifying': 'Qualifying',
    'sprint-qualifying': 'Sprint Qualifying',
    'sprint-shootout': 'Sprint Shootout',
    'sprint': 'Sprint',
    'race': 'Race',
}

# Reverse mapping: session name to slug
SESSION_NAME_TO_SLUG = {v: k for k, v in SESSION_SLUGS.items()}


# =============================================================================
# WEB ROUTES
# =============================================================================

@app.route('/')
def home():
    """
    The main homepage that shows the pecking order rankings.

    When someone visits http://localhost:5000/, this function runs
    and returns the HTML page to display.
    """
    # Calculate the current rankings
    driver_rankings, team_rankings = calculate_rankings()

    # Get database statistics
    stats = get_statistics()

    # Get breakdown by race weekend
    meetings = get_meeting_breakdown()

    # Render the HTML template with our data
    # The template file is in templates/index.html
    return render_template(
        'index.html',
        driver_rankings=driver_rankings,
        team_rankings=team_rankings,
        stats=stats,
        meetings=meetings,
    )


@app.route('/api/rankings')
def api_rankings():
    """
    API endpoint that returns rankings as JSON.

    This is useful if you want to fetch the data programmatically
    or build a different frontend later.

    Access at: http://localhost:5000/api/rankings
    """
    driver_rankings, team_rankings = calculate_rankings()

    return jsonify({
        'drivers': driver_rankings,
        'teams': team_rankings,
    })


@app.route('/api/stats')
def api_stats():
    """
    API endpoint that returns database statistics as JSON.

    Access at: http://localhost:5000/api/stats
    """
    stats = get_statistics()
    return jsonify(stats)


@app.route('/methodology')
def methodology():
    """
    Page explaining how the pace calculations work.
    """
    return render_template('methodology.html')


@app.route('/race/<int:meeting_key>')
def race_detail(meeting_key):
    """
    Race weekend overview page.

    Shows the overall weekend pecking order and session preview cards.
    """
    meeting_data = get_meeting_pecking_order(meeting_key)

    if not meeting_data:
        abort(404)

    # Add slugs to sessions for URL building
    for session in meeting_data.get('sessions', []):
        session['slug'] = SESSION_NAME_TO_SLUG.get(session['session_name'], '')

    for summary in meeting_data.get('session_summaries', []):
        summary['slug'] = SESSION_NAME_TO_SLUG.get(summary['session_name'], '')

    return render_template(
        'race_detail.html',
        meeting=meeting_data['meeting'],
        overall_rankings=meeting_data['overall_rankings'],
        session_summaries=meeting_data['session_summaries'],
        sessions=meeting_data['sessions'],
    )


@app.route('/race/<int:meeting_key>/<session_slug>')
def session_detail(meeting_key, session_slug):
    """
    Session detail page.

    Shows detailed pecking order for a specific session with sectors, tires, etc.
    """
    # Convert slug to session name
    session_name = SESSION_SLUGS.get(session_slug)
    if not session_name:
        abort(404)

    # Get meeting info
    meeting = get_meeting_by_key(meeting_key)
    if not meeting:
        abort(404)

    # Get the specific session
    session = get_session_by_meeting_and_type(meeting_key, session_name)
    if not session:
        abort(404)

    # Get session pecking order
    session_data = get_session_pecking_order(session['session_key'])
    if not session_data:
        abort(404)

    # Get all sessions for this meeting (for tab navigation)
    all_sessions = get_sessions_for_meeting(meeting_key)

    # Add slugs to sessions
    for s in all_sessions:
        s['slug'] = SESSION_NAME_TO_SLUG.get(s['session_name'], '')

    return render_template(
        'session_detail.html',
        meeting=meeting,
        session=session_data['session'],
        current_slug=session_slug,
        driver_rankings=session_data['driver_rankings'],
        tire_summary=session_data['tire_summary'],
        stats=session_data['stats'],
        sessions=all_sessions,
    )


# =============================================================================
# TEMPLATE FILTERS
# =============================================================================

@app.template_filter('format_gap')
def format_gap(value):
    """
    Custom filter to format gap times nicely.

    Usage in templates: {{ gap_value | format_gap }}
    """
    if value == 0:
        return "LEADER"
    elif value > 0:
        return f"+{value:.3f}s"
    else:
        return f"{value:.3f}s"


@app.template_filter('format_pace')
def format_pace(value):
    """
    Custom filter to format pace times as mm:ss.xxx

    Usage in templates: {{ pace_value | format_pace }}
    """
    if value is None:
        return "N/A"

    minutes = int(value // 60)
    seconds = value % 60

    if minutes > 0:
        return f"{minutes}:{seconds:06.3f}"
    else:
        return f"{seconds:.3f}s"


@app.template_filter('format_sector')
def format_sector(value):
    """
    Custom filter to format sector times as ss.xxx

    Usage in templates: {{ sector_value | format_sector }}
    """
    if value is None:
        return "-"

    return f"{value:.3f}"


@app.template_filter('format_speed')
def format_speed(value):
    """
    Custom filter to format speed trap values.

    Usage in templates: {{ speed_value | format_speed }}
    """
    if value is None:
        return "-"

    return f"{value:.0f}"


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == '__main__':
    # Initialize the database (in case it doesn't exist)
    print("Initializing database...")
    initialize_database()

    # Print startup message
    print("\n" + "=" * 50)
    print("F1 PECKING ORDER - WEB SERVER")
    print("=" * 50)
    print("\nStarting web server...")
    print("Open your browser and go to: http://localhost:5001")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 50 + "\n")

    # Run the Flask development server
    # debug=True means the server will auto-reload when you change code
    # and show detailed error messages
    app.run(debug=True, host='0.0.0.0', port=5001)
