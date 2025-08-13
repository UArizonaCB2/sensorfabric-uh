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

logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)

if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


class UltrahumanJWTGenerator:
    """
    AWS Lambda function for generating JWT tokens for UltraHuman report access.
    
    This class handles:
    1. Generating JWT tokens with participant_id, start_date, end_date
    2. Updating MDH participant custom fields with JWT tokens
    3. Using HS256 algorithm with REPORT_SECRET from AWS Secrets Manager
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

    def _generate_jwt_token(self, participant_id: str, start_date: str, end_date: str) -> str:
        """
        Generate JWT token with participant data.
        
        Args:
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            
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

    def _get_active_participants(self) -> List[Dict[str, Any]]:
        """Fetch active participants from MDH."""
        try:
            participants_data = self.mdh.getAllParticipants()
            active_participants = []
            
            for participant in participants_data.get('participants', []):
                # Filter for active participants
                if participant.get('enrolled'):
                    active_participants.append(participant)

            logger.debug(f"Found {len(active_participants)} active participants")
            return active_participants

        except Exception as e:
            logger.error(f"Failed to fetch participants: {str(e)}")
            error_data = {'operation': 'mdh_get_all_participants'}
            handle_api_error(e, error_data, 'mdh_get_all_participants')
            raise

    def generate_jwt_for_participant(
        self, 
        participant_id: str, 
        start_date: str, 
        end_date: str,
        update_mdh: bool = True
    ) -> Dict[str, Any]:
        """
        Generate JWT token for a specific participant.
        
        Args:
            participant_id: MDH participant ID
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD) 
            update_mdh: Whether to update MDH with the JWT token
            
        Returns:
            Dictionary with generation results
        """
        try:
            # Initialize connections if not already done
            if self.mdh is None:
                self._initialize_connections()
            
            # Generate JWT token
            jwt_token = self._generate_jwt_token(participant_id, start_date, end_date)
            
            # Update MDH participant custom fields if requested
            if update_mdh:
                self._update_participant_custom_field(participant_id, jwt_token)
            
            return {
                'success': True,
                'participant_id': participant_id,
                'jwt_token': jwt_token,
                'start_date': start_date,
                'end_date': end_date,
                'expires_in_days': self.jwt_expiration_days,
                'mdh_updated': update_mdh
            }
            
        except Exception as e:
            logger.error(f"JWT generation failed for participant {participant_id}: {str(e)}")
            return {
                'success': False,
                'participant_id': participant_id,
                'error': str(e)
            }

    def generate_jwt_for_all_participants(
        self, 
        start_date: str, 
        end_date: str,
        update_mdh: bool = True
    ) -> Dict[str, Any]:
        """
        Generate JWT tokens for all active participants.
        
        Args:
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            update_mdh: Whether to update MDH with JWT tokens
            
        Returns:
            Dictionary with batch generation results
        """
        try:
            # Initialize connections
            self._initialize_connections()
            
            # Get active participants
            participants = self._get_active_participants()
            
            if not participants:
                return {
                    'success': True,
                    'message': 'No active participants found',
                    'participants_processed': 0,
                    'successful_generations': 0,
                    'failed_generations': 0,
                }
            
            # Generate JWT tokens for each participant
            successful_generations = 0
            failed_generations = 0
            
            for participant in participants:
                participant_id = participant.get('participantIdentifier')
                if not participant_id:
                    continue
                    
                result = self.generate_jwt_for_participant(
                    participant_id, start_date, end_date, update_mdh
                )
                
                if result['success']:
                    successful_generations += 1
                else:
                    failed_generations += 1

            return {
                'success': True,
                'message': f'JWT token generation completed',
                'participants_processed': len(participants),
                'successful_generations': successful_generations,
                'failed_generations': failed_generations,
                'start_date': start_date,
                'end_date': end_date
            }
            
        except Exception as e:
            logger.error(f"Batch JWT generation failed: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'message': 'JWT token generation failed'
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
    AWS Lambda entry point for generating JWT tokens for UltraHuman report access.
    
    Expected event structure:
    {
        "participant_id": "optional_specific_participant_id",
        "update_mdh": true  # Optional, defaults to true
    }
    
    Note: start_date and end_date are auto-generated:
    - end_date: current date
    - start_date: current date minus 7 days
    
    Environment variables required:
    - AWS_SECRET_NAME: Name of the secret in AWS Secrets Manager
    - JWT_EXPIRATION_DAYS: JWT token expiration in days (default: 30)
    
    Variables from SecretsManager:
    - REPORT_SECRET: Secret key for JWT signing (HS256 algorithm)
    - MDH_SECRET_KEY: MyDataHelps account secret
    - MDH_ACCOUNT_NAME: MyDataHelps account name
    - MDH_PROJECT_ID: MyDataHelps project ID
    """
    
    logger.debug(f"UltraHuman JWT Generator Lambda started with event: {json.dumps(event)}")

    try:
        # Setup environment with secrets
        secrets = get_secret()
        
        # Extract parameters from event
        participant_id = event.get('participant_id')
        
        # Auto-generate dates: end_date = today, start_date = today - 7 days
        today = datetime.date.today()
        end_date = today.strftime('%Y-%m-%d')
        start_date = (today - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"Auto-generated dates: start_date={start_date}, end_date={end_date}")

        generator = UltrahumanJWTGenerator(config=secrets)

        # Generate JWT token(s)
        if participant_id:
            # Generate for specific participant
            result = generator.generate_jwt_for_participant(
                participant_id, start_date, end_date
            )
        else:
            # Generate for all active participants
            result = generator.generate_jwt_for_all_participants(
                start_date, end_date
            )
        
        # Prepare Lambda response
        response = {
            'statusCode': 200 if result['success'] else 500,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
        logger.debug(f"UltraHuman JWT Generator completed: {json.dumps(result)}")
        return response
        
    except RetryableError as e:
        error_message = f"Retryable error in UltraHuman JWT generator: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        raise e
        
    except Exception as e:
        error_message = f"UltraHuman JWT Generator Lambda failed: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'UltraHuman JWT Generator Lambda execution failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


def test_locally(participant_id: Optional[str] = None):
    """
    Function to test the UltraHuman JWT Generator pipeline locally.
    
    Args:
        participant_id: Optional specific participant ID
        
    Note: start_date and end_date are auto-generated (end_date = today, start_date = today - 7 days)
    """
    # Mock event for local testing
    event = {
        'update_mdh': True
    }
    
    if participant_id:
        event['participant_id'] = participant_id
    
    # Mock context object
    class MockContext:
        def __init__(self):
            self.function_name = 'ultrahuman-jwt-generator-local-test'
            self.aws_request_id = 'local-test-123'
    
    context = MockContext()
    
    # Run the lambda handler
    response = lambda_handler(event, context)
    
    print("Response:")
    print(json.dumps(json.loads(response['body']), indent=2))
    
    return response


if __name__ == "__main__":
    # For local testing
    test_locally()