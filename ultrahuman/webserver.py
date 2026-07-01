#!/usr/bin/env python3

"""
Used only for local testing to render the reporting templates.
Do not include this in the production lambda build.

Usage:
    python ultrahuman/webserver.py [--port PORT]

Examples:
    python ultrahuman/webserver.py
    python ultrahuman/webserver.py --port 8080

Then access:
    http://localhost:5000/?t=YOUR_JWT_TOKEN

Or generate a test JWT and report:
    http://localhost:5000/test?participant_id=BB-1234-5678
"""

from flask import Flask, request, jsonify
from ultrahuman.templates import lambda_handler, get_secret
import os
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def index():
    """
    Main route that accepts JWT token and renders the report.
    Query parameter: t (JWT token)

    Example: http://localhost:5000/?t=eyJhbGciOiJIUzI1NiIs...
    """
    jwt_token = request.args.get('t')

    if not jwt_token:
        return jsonify({
            'error': 'Missing JWT token',
            'usage': 'Add ?t=YOUR_JWT_TOKEN to the URL'
        }), 400

    # Create mock Lambda event for Function URL format
    event = {
        'queryStringParameters': {
            't': jwt_token
        }
    }

    # Mock context
    class MockContext:
        def __init__(self):
            self.function_name = 'template-generator-local-test'
            self.aws_request_id = 'local-test-123'

    context = MockContext()

    # Call the lambda handler
    response = lambda_handler(event, context)

    # Return the response
    status_code = response.get('statusCode', 500)
    body = response.get('body', '')
    headers = response.get('headers', {})

    return body, status_code, headers

@app.route('/mocktest')
def mock_test_template():
    """
    Method to test the template out with mock data.
    Query parameters:
    - start_date: Start date in YYYY-MM-DD format (optional, defaults to 7 days ago)
    - end_date: End date in YYYY-MM-DD format (optional, defaults to today)

    Example: http://localhost:5000/mocktest
    """

    """
    Example format for the dictionary
    data = dict(
        ringwear=ringwear,
        weeks_enrolled=weeks_enrolled,
        current_pregnancy_week=ga_weeks,
        surveys_completed=ema_count,
        symptoms=symptoms,
        weight=weight,
        movement=movement,
        sleep=sleep,
        temp=temp,
        hr=hr,
        bp=bp,
        # enabled flags (not currently used. Passing None to metrics disables them)
        blood_pressure_enabled=True,
        heart_rate_enabled=True,
        temperature_enabled=True,
        sleep_enabled=True,
        weight_enabled=True,
        movement_enabled=True,
        start_str=start_str,
        end_str=end_str
    )
    """
    from ultrahuman.uh_jwt_worker import UltrahumanJWTWorker
    import datetime

    # Get dates from query params or use defaults
    end_date = request.args.get('end_date')
    start_date = request.args.get('start_date')

    if not end_date:
        end_date = datetime.date.today().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
    data = dict(
        ringwear=None,
        weeks_enrolled=None,
        current_pregnancy_week=None,
        surveys_completed=None,
        symptoms=None,
        weight=None,
        movement=None,
        sleep=None,
        temp=None,
        hr=None,
        bp=None,
        start_str = start_date,
        end_str = end_date,
    )


    try:
        # Get secrets
        secrets = get_secret()

        # Initialize JWT worker
        jwt_worker = UltrahumanJWTWorker(config=secrets)

        # Generate JWT for this participant
        html = jwt_worker._generate_template(
            participant_id=None,
            start_date=start_date,
            end_date=end_date,
            dry_run=True,
            mock_data=data,
        )

        return html

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return jsonify({
            'error': 'Test failed',
            'details': str(e)
        }), 500

@app.route('/test')
def test_template():
    """
    Method which generates the participant HTML template for testing.
    Query parameters:
    - participant_id: MDH participant ID (required)
    - start_date: Start date in YYYY-MM-DD format (optional, defaults to 7 days ago)
    - end_date: End date in YYYY-MM-DD format (optional, defaults to today)

    Example: http://localhost:5000/test?participant_id=BB-1234-5678
    """
    from ultrahuman.uh_jwt_worker import UltrahumanJWTWorker
    import datetime

    participant_id = request.args.get('participant_id')
    if not participant_id:
        return jsonify({
            'error': 'Missing participant_id',
            'usage': 'Add ?participant_id=YOUR_PARTICIPANT_ID to the URL'
        }), 400

    # Get dates from query params or use defaults
    end_date = request.args.get('end_date')
    start_date = request.args.get('start_date')

    if not end_date:
        end_date = datetime.date.today().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

    try:
        # Get secrets
        secrets = get_secret()

        # Initialize JWT worker
        jwt_worker = UltrahumanJWTWorker(config=secrets)

        # Generate JWT for this participant
        html = jwt_worker._generate_template(
            participant_id=participant_id,
            start_date=start_date,
            end_date=end_date,
            dry_run=True,
        )

        return html

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return jsonify({
            'error': 'Test failed',
            'details': str(e)
        }), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'template-generator-local-test'
    })


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Local Flask server for testing UltraHuman template generation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ultrahuman/webserver.py
  python ultrahuman/webserver.py --port 8080
  python ultrahuman/webserver.py -p 3000

Then access:
  http://localhost:{PORT}/?t=YOUR_JWT_TOKEN
  http://localhost:{PORT}/test?participant_id=BB-1234-5678
        """
    )
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=5000,
        help='Port to run the server on (default: 5000)'
    )
    args = parser.parse_args()

    # Check required environment variables
    required_env_vars = ['AWS_SECRET_NAME', 'AWS_REGION']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("\nPlease set these environment variables:")
        logger.error("  export AWS_SECRET_NAME='prod/biobayb/uh/keys'")
        logger.error("  export AWS_REGION='us-east-1'")
        logger.error("  export SF_DATA_BUCKET='uoa-biobayb-uh-dev'")
        logger.error("  export SF_DATABASE_NAME='uh-biobayb-dev'")
        logger.error("  export UH_ENVIRONMENT='development'")
        logger.error("  export TEMPLATE_GENERATOR_URL='https://your-function-url.lambda-url.us-east-1.on.aws/'")
        exit(1)

    logger.info(f"Starting Flask development server on port {args.port}...")
    logger.info("Available routes:")
    logger.info(f"  - http://localhost:{args.port}/?t=YOUR_JWT_TOKEN")
    logger.info(f"  - http://localhost:{args.port}/test?participant_id=BB-1234-5678")
    logger.info(f"  - http://localhost:{args.port}/health")

    app.run(debug=True, host='0.0.0.0', port=args.port)
