from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from typing import Dict, List, Any, Optional
import json
import logging
import os
import base64
import boto3
from botocore.exceptions import ClientError
from sensorfabric.mdh import MDH
from helper import Helper
import traceback

logger = logging.getLogger(__name__)

class TemplateGenerator:
    """
    AWS Lambda function for generating weekly health reports from MDH / UltraHuman API data.
    
    This class handles:
    1. Connecting to MDH/Athena
    2. Running SQL queries to gather data
    3. Generating Jinja2 HTML template
    """
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        self.__config = config
        self.mdh = None

    def _initialize_connections(self):
        """Initialize MDH connection"""
        try:
            # Initialize MDH connection from env vars
            mdh_configuration = {
                'account_secret': self.__config.get('MDH_SECRET_KEY'),
                'account_name': self.__config.get('MDH_ACCOUNT_NAME'),
                'project_id': self.__config.get('MDH_PROJECT_ID'),
            }
            self.mdh = MDH(**mdh_configuration)
            logger.info("MDH connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MDH connection: {str(e)}")
            raise


    def generate_weekly_report_template(
        self,
        participant_id: str,
        target_week: Optional[str] = None,
    ) -> str:
        """
        Generate a Jinja2 HTML template for weekly health reports from Ultrahuman API data.
        
        Args:
            participant_id: ID of the participant to generate the report for
            target_week: Optional week to generate the report for - this is the END week, inclusive.

        Returns:
            HTML string with populated template
            
        Raises:
            Exception: If MDH connection fails
        """
        # initialize connections if not already done
        if self.mdh is None:
            self._initialize_connections()

        # participant = self.mdh.getParticipant(participant_id)
        if target_week is None:
            last_week_utc_timestamp = datetime.now()
        else:
            # TODO make target_week processing more robust.
            # TODO need to use participant's timezone? not sure.
            last_week_utc_timestamp = datetime.strptime(target_week, '%Y-%m-%d')

        helper = Helper(mdh=self.mdh, participant_id=participant_id, end_date=last_week_utc_timestamp)

        ringwear = helper.ringWearTime()
        weight = helper.weightSummary()
        movement = helper.movementSummary()
        symptoms = helper.topSymptomsRecorded()
        sleep = helper.sleepSummary()
        temp = helper.temperatureSummary()
        hr = helper.heartRateSummary()
        bp = helper.bloodPressure()
        weeks_enrolled = helper.weeksEnrolled()
        ga_weeks = helper.weeksPregnant()
        ema_count = helper.emaCompleted()
        enrolled_date = helper.enrolledDate()

        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('reportv2.html')

        data = dict(
            weeks_enrolled=weeks_enrolled,
            current_pregnancy_week=ga_weeks,
            ring_wear_percentage=ringwear['ring_wear_percent'],
            surveys_completed=ema_count,
            symptoms=symptoms,
            bp_count=bp['counts'],
            bp_trend=bp['trend'],
            bp_high_readings=bp['above_threshold_counts'],
            temp_count=temp['counts'],
            temp_trend=temp['trend'],
            temp_high_readings=temp['above_threshold_counts'],
            heart_rate_total_beats=hr['hr_counts'],
            heart_rate_avg_resting=hr['avg_rhr'],
            enrolled_date=enrolled_date,
            sleep_total_hours=sleep['hours'],
            sleep_avg_per_night=sleep['average_per_night'],
            movement_total_minutes=movement['total_movements_mins'],
            movement_avg_steps_per_day=movement['average_steps_int'],
            movement_step_trend=movement['trend'],
            weight_change=weight['change_in_weight'],
            # enabled flags
            blood_pressure_enabled=True,
            heart_rate_enabled=True,
            temperature_enabled=True,
            sleep_enabled=True,
            weight_enabled=True,
            movement_enabled=True,
            report_date=datetime.now().strftime("%Y-%m-%d %H:%M")
        )

        html = template.render(data)

        return html

def get_secret():
    """
    Uses secretmanager to fill in MDH secrets
    """
    secret_name = os.getenv("AWS_SECRET_NAME")
    region_name = os.getenv("AWS_REGION", "us-east-1")
    logger.debug(f"Env Variables - {secret_name} {region_name}")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("The request was invalid due to: " + e.response['Error']['Message'])
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("The request had invalid params - " + e.response['Error']['Message'])
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            logger.error("The requested secret can't be decrypted using the provided KMS key - " + e.response['Error']['Message'])
        elif e.response['Error']['Code'] == 'InternalServiceError':
            logger.error("The request was not processed because of an internal error. - " + e.response['Error']['Message'])
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)


def lambda_handler(event, context):
    """
    AWS Lambda handler for generating weekly health reports.
    
    Expected event format:
    {
        "participant_id": "<mdh_id>",
        "target_date": Optional[str]  # YYYY-MM-DD format
    }
    
    Returns:
    {
        "statusCode": 200,
        "body": "<HTML content>"
    }
    """
    logger.debug(f"Template Generator Lambda started with event: {json.dumps(event)}")

    try:
        # Setup environment with secrets
        secrets = get_secret()

        # Extract parameters from event
        participant_id = event.get('participant_id')
        target_date = event.get('target_date')

        if not participant_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'participant_id is required'})
            }
        
        # Initialize template generator
        generator = TemplateGenerator(secrets)

        # Generate the report
        html_report = generator.generate_weekly_report_template(
            participant_id=participant_id,
            target_week=target_date
        )

        logger.debug(f"Template generation completed for participant {participant_id}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/html'
            },
            'body': html_report
        }
        
    except Exception as e:
        error_message = f"Template Generator Lambda failed: {str(e)}"
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'Template generation failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }

