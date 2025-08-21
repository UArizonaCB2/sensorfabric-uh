import json
import os
import datetime
from typing import Dict, List, Any, Optional
import logging
import traceback
import boto3
from botocore.exceptions import ClientError
import jwt

from sensorfabric.mdh import MDH
from ultrahuman.error_handling import handle_api_error, RetryableError, NonRetryableError
from ultrahuman.helper import Helper
from jinja2 import Environment, FileSystemLoader
import uuid

logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)

if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


class UltrahumanJWTWorker:
    """
    AWS Lambda worker for Step Functions JWT generation.
    
    This class handles:
    1. Processing individual participants
    2. Generating JWT tokens with participant_id, start_date, end_date
    3. Updating MDH participant custom fields with JWT tokens
    4. Using HS256 algorithm with REPORT_SECRET from AWS Secrets Manager
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.__config = config
        self.mdh = None
        
        # JWT configuration
        self.jwt_secret = config.get('REPORT_SECRET')
        if not self.jwt_secret:
            raise ValueError("REPORT_SECRET not found in configuration")
        
        # JWT expiration (7 days by default)
        self.jwt_expiration_days = int(os.getenv('JWT_EXPIRATION_DAYS', 7))

    def _initialize_connections(self):
        """Initialize MDH connection"""
        try:
            mdh_configuration = {
                'account_secret': self.__config.get('MDH_SECRET_KEY'),
                'account_name': self.__config.get('MDH_ACCOUNT_NAME'),
                'project_id': self.__config.get('MDH_PROJECT_ID'),
            }
            self.mdh = MDH(**mdh_configuration)
            logger.debug("MDH connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MDH connection: {str(e)}")
            handle_api_error(e, {'operation': 'mdh_connection_initialization'}, 'initialize_mdh_connection')
            raise

    def _generate_jwt_token(self, participant_id: str, start_date: str, end_date: str, s3_path: str) -> str:
        """
        Generate JWT token with participant data.
        
        Args:
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            s3_path: S3 path to the HTML template
            
        Returns:
            JWT token string
        """
        try:
            # Calculate expiration timestamp
            expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=self.jwt_expiration_days)
            
            # Create JWT payload
            payload = {
                'participant_id': participant_id,
                'start_date': start_date,
                'end_date': end_date,
                's3_path': s3_path,
                'iat': datetime.datetime.now(datetime.timezone.utc),
                'exp': expiration
            }
            
            # Generate JWT token using HS256 algorithm
            token = jwt.encode(
                payload=payload,
                key=self.jwt_secret,
                algorithm='HS256'
            )
            
            logger.info(f"JWT token generated for participant {participant_id}")
            return token
            
        except Exception as e:
            logger.error(f"Failed to generate JWT token for participant {participant_id}: {str(e)}")
            raise

    def _upload_template_to_s3(self, html_content: str, participant_id: str, start_date: str, end_date: str) -> str:
        """
        Upload generated HTML template to S3 bucket.
        
        Args:
            html_content: Generated HTML content
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            
        Returns:
            S3 path to the uploaded template
        """
        try:
            # Get S3 bucket from environment
            data_bucket = os.getenv('SF_DATA_BUCKET')
            if not data_bucket:
                raise ValueError("SF_DATA_BUCKET environment variable not set")
            
            # Generate unique filename with timestamp and UUID to avoid conflicts
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_id = str(uuid.uuid4())[:8]
            filename = f"template_{participant_id}_{start_date}_{end_date}_{timestamp}_{unique_id}.html"
            s3_key = f"templates/{filename}"
            
            # Upload to S3
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=data_bucket,
                Key=s3_key,
                Body=html_content.encode('utf-8'),
                ContentType='text/html',
                ServerSideEncryption='AES256'
            )
            
            s3_path = f"s3://{data_bucket}/{s3_key}"
            logger.info(f"Template uploaded to S3: {s3_path}")
            return s3_path
            
        except Exception as e:
            logger.error(f"Failed to upload template to S3: {str(e)}")
            raise

    def _update_participant_custom_field(self, participant_id: str, jwt_token: str) -> bool:
        """
        Update MDH participant's custom fields with JWT token.
        
        Args:
            participant_id: MDH participant ID
            jwt_token: Generated JWT token
            
        Returns:
            True if update was successful
        """
        try:
            # Update custom fields with JWT token
            update_data = [{
                'participantIdentifier': participant_id,
                'customFields': {'report_jwt': jwt_token}
            }]
            self.mdh.update_participants(update_data)
            
            logger.info(f"Updated participant {participant_id} custom fields with JWT token")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update participant {participant_id} custom fields: {str(e)}")
            error_data = {
                'participant_id': participant_id,
                'operation': 'mdh_update_participant'
            }
            handle_api_error(e, error_data, 'mdh_update_participant')
            raise

    def _generate_template(
        self,
        participant_id: str,
        start_date: str,
        end_date: str
    ) -> str:
        """
        Generate HTML template for a participant and upload to S3.
        
        Args:
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            
        Returns:
            S3 path to the uploaded HTML template
        """
        # Initialize connections if not already done
        if self.mdh is None:
            self._initialize_connections()

        # Convert string dates to datetime.date objects
        end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()

        config = {
            'MDH_SECRET_KEY': self.__config.get('MDH_SECRET_KEY'),
            'MDH_ACCOUNT_NAME': self.__config.get('MDH_ACCOUNT_NAME'),
            'MDH_PROJECT_ID': self.__config.get('MDH_PROJECT_ID'),
            'MDH_PROJECT_NAME': self.__config.get('MDH_PROJECT_NAME'),
            'UH_DATABASE': self.__config.get('UH_DATABASE'),
            'UH_WORKGROUP': self.__config.get('UH_WORKGROUP'),
            'UH_S3_LOCATION': self.__config.get('UH_S3_LOCATION'),
            'participant_id': participant_id,
            'end_date': end_date_obj,
            'start_date': start_date_obj
        }

        try:
            helper = Helper(config=config)
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

            env = Environment(loader=FileSystemLoader('ultrahuman/templates'))
            template = env.get_template('reportv2.html')

            # Convert the start and end dates to something that user can read.
            start_str = start_date_obj.strftime("%B %d")
            end_str = end_date_obj.strftime("%B %d, %Y")

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

            html = template.render(data)
            
            # Upload HTML to S3 and return the S3 path
            s3_path = self._upload_template_to_s3(html, participant_id, start_date, end_date)
            return s3_path
            
        except Exception as e:
            logger.error(f"Failed to generate template for participant {participant_id}: {str(e)}")
            raise

    def process_participant(
        self, 
        participant_id: str, 
        start_date: str, 
        end_date: str,
        update_mdh: bool = True
    ) -> Dict[str, Any]:
        """
        Process JWT generation for a single participant.
        
        Args:
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD) 
            update_mdh: Whether to update MDH with the JWT token
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Initialize connections if not already done
            if self.mdh is None:
                self._initialize_connections()
            
            # Generate S3 html template + path
            s3_path = self._generate_template(participant_id, start_date, end_date)
            # Generate JWT token
            jwt_token = self._generate_jwt_token(participant_id, start_date, end_date, s3_path)
            
            # Update MDH participant custom fields if requested
            if update_mdh:
                self._update_participant_custom_field(participant_id, jwt_token)
            
            return {
                'success': True,
                'participant_id': participant_id,
                'jwt_token': jwt_token,
                'start_date': start_date,
                'end_date': end_date,
                's3_path': s3_path,
                'expires_in_days': self.jwt_expiration_days,
                'mdh_updated': update_mdh
            }
            
        except Exception as e:
            logger.error(f"JWT processing failed for participant {participant_id}: {str(e)}")
            return {
                'success': False,
                'participant_id': participant_id,
                'error': str(e)
            }


def get_secret():
    """
    Uses secretmanager to fill in MDH secrets
    """
    secret_name = os.getenv("AWS_SECRET_NAME")
    region_name = os.getenv("AWS_REGION", "us-east-1")
    
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
    AWS Lambda entry point for JWT generation worker.
    
    Expected event structure:
    {
        "participant_id": "required_participant_id",
        "start_date": "2024-01-01",
        "end_date": "2024-01-08",
        "update_mdh": true
    }
    
    Environment variables required:
    - AWS_SECRET_NAME: Name of the secret in AWS Secrets Manager
    - JWT_EXPIRATION_DAYS: JWT token expiration in days (default: 7)
    - SF_DATA_BUCKET: S3 bucket for template storage
    """
    
    logger.info(f"UltraHuman JWT Worker started for participant: {event.get('participant_id')}")

    try:
        # Setup environment with secrets
        secrets = get_secret()
        
        # Extract parameters from event
        participant_id = event.get('participant_id')
        if not participant_id:
            raise ValueError("participant_id is required")
            
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        update_mdh = event.get('update_mdh', True)
        
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required")

        worker = UltrahumanJWTWorker(config=secrets)

        # Process the participant
        result = worker.process_participant(participant_id, start_date, end_date, update_mdh)
        
        # Prepare Lambda response
        response = {
            'statusCode': 200 if result['success'] else 500,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
        logger.info(f"UltraHuman JWT Worker completed for participant {participant_id}: success={result['success']}")
        return response
        
    except RetryableError as e:
        error_message = f"Retryable error in UltraHuman JWT worker: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        raise e
        
    except Exception as e:
        error_message = f"UltraHuman JWT Worker Lambda failed: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'UltraHuman JWT Worker Lambda execution failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }