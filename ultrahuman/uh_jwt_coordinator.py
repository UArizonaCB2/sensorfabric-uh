import json
import os
import datetime
from typing import Dict, List, Any
import logging
import traceback
import boto3
from botocore.exceptions import ClientError

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


class UltrahumanJWTCoordinator:
    """
    AWS Lambda coordinator for Step Functions JWT generation fan-out.
    
    This class handles:
    1. Fetching active participants from MDH
    2. Creating Step Functions execution with participant list
    3. Returning execution ARN for monitoring
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.__config = config
        self.mdh = None

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

    def _get_active_participants(self) -> List[Dict[str, Any]]:
        """Fetch active participants from MDH."""
        try:
            participants_data = self.mdh.getAllParticipants()
            active_participants = []
            
            for participant in participants_data.get('participants', []):
                # Filter for active participants
                if participant.get('enrolled'):
                    active_participants.append({
                        'participant_id': participant.get('participantIdentifier'),
                        'email': participant.get('email', ''),
                        'enrolled_date': participant.get('enrolledDate', '')
                    })

            logger.info(f"Found {len(active_participants)} active participants")
            return active_participants

        except Exception as e:
            logger.error(f"Failed to fetch participants: {str(e)}")
            error_data = {'operation': 'mdh_get_all_participants'}
            handle_api_error(e, error_data, 'mdh_get_all_participants')
            raise

    def start_jwt_generation(
        self, 
        start_date: str, 
        end_date: str,
        participant_id: str = None
    ) -> Dict[str, Any]:
        """
        Start Step Functions execution for JWT generation.
        
        Args:
            start_date: Start date for report period (YYYY-MM-DD)
            end_date: End date for report period (YYYY-MM-DD)
            participant_id: Optional specific participant ID
            
        Returns:
            Dictionary with execution details
        """
        try:
            # Initialize connections
            self._initialize_connections()
            
            # Get participants
            if participant_id:
                participants = [{'participant_id': participant_id}]
                logger.info(f"Processing single participant: {participant_id}")
            else:
                participants = self._get_active_participants()
            
            if not participants:
                return {
                    'success': True,
                    'message': 'No active participants found',
                    'execution_arn': None,
                    'participants_count': 0
                }
            
            # Prepare Step Functions input
            step_input = {
                'start_date': start_date,
                'end_date': end_date,
                'participants': participants,
                'batch_size': int(os.getenv('JWT_BATCH_SIZE', '10')),
                'total_participants': len(participants)
            }
            
            # Start Step Functions execution
            step_functions_client = boto3.client('stepfunctions')
            state_machine_arn = os.getenv('JWT_STATE_MACHINE_ARN')
            
            if not state_machine_arn:
                raise ValueError("JWT_STATE_MACHINE_ARN environment variable not set")
            
            execution_name = f"jwt-generation-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"
            
            response = step_functions_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_name,
                input=json.dumps(step_input)
            )
            
            logger.info(f"Started Step Functions execution: {response['executionArn']}")
            
            return {
                'success': True,
                'execution_arn': response['executionArn'],
                'execution_name': execution_name,
                'participants_count': len(participants),
                'start_date': start_date,
                'end_date': end_date,
                'message': f'JWT generation started for {len(participants)} participants'
            }
            
        except Exception as e:
            logger.error(f"Failed to start JWT generation: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Failed to start JWT generation'
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
    AWS Lambda entry point for JWT generation coordinator.
    
    Expected event structure:
    {
        "participant_id": "optional_specific_participant_id",
        "start_date": "2024-01-01",  # Optional, auto-generated if not provided
        "end_date": "2024-01-08"     # Optional, auto-generated if not provided
    }
    
    Environment variables required:
    - AWS_SECRET_NAME: Name of the secret in AWS Secrets Manager
    - JWT_STATE_MACHINE_ARN: ARN of the Step Functions state machine
    - JWT_BATCH_SIZE: Number of participants per batch (default: 10)
    """
    
    logger.info(f"UltraHuman JWT Coordinator started with event: {json.dumps(event)}")

    try:
        # Setup environment with secrets
        secrets = get_secret()
        
        # Extract parameters from event
        participant_id = event.get('participant_id')
        
        end_date = event.get('end_date', None)
        start_date = event.get('start_date', None)
        if not end_date or not start_date:
            # Auto-generate dates if not provided: end_date = today, start_date = today - 7 days
            today = datetime.date.today()
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

        logger.info(f"Using dates: start_date={start_date}, end_date={end_date}")

        coordinator = UltrahumanJWTCoordinator(config=secrets)

        # Start JWT generation via Step Functions
        result = coordinator.start_jwt_generation(start_date, end_date, participant_id)
        
        # Prepare Lambda response
        response = {
            'statusCode': 200 if result['success'] else 500,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
        logger.info(f"UltraHuman JWT Coordinator completed: {json.dumps(result)}")
        return response
        
    except RetryableError as e:
        error_message = f"Retryable error in UltraHuman JWT coordinator: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        raise e
        
    except Exception as e:
        error_message = f"UltraHuman JWT Coordinator Lambda failed: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'UltraHuman JWT Coordinator Lambda execution failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }