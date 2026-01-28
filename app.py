"""
F1 Pecking Order - Web Application

This is the Flask web application that displays the F1 pace rankings.
Flask is a simple Python web framework that lets us create web pages easily.

To run the website locally:
    python app.py

Then open http://localhost:5000 in your browser.
"""

from flask import Flask, render_template, jsonify
from pace_analyzer import (
    calculate_rankings,
    get_meeting_breakdown,
)
from database import get_statistics, initialize_database

# Create the Flask application
# __name__ tells Flask where to find templates and static files
app = Flask(__name__)


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
