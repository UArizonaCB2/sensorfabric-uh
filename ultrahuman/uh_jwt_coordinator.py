import json
import os
import re
import datetime
from typing import Dict, List, Any
import logging
import traceback
import boto3
from botocore.exceptions import ClientError

import sensorfabric.endpoints as ep
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

# The weekly report goes to the MDH "Active participants" segment. MDH owns the
# segment rules, so they are queried rather than reimplemented here: edits made
# to the segment in the MDH UI take effect on the next run with no code change.
#
# Supplied as configuration, never hardcoded: the environment variable wins, and
# the value may also arrive on the secret payload. There is deliberately no
# default. If it is absent or malformed the run fails rather than falling back
# to a broader audience.
ACTIVE_SEGMENT_ID_KEY = 'MDH_ACTIVE_SEGMENT_ID'
_GUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


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

    def _resolve_segment_id(self) -> str:
        """Return the configured "Active participants" segment id.

        Raises NonRetryableError when absent or malformed. Retrying will not
        fix a configuration problem, and the alternative -- carrying on with a
        broader audience -- would send reports to participants the study has
        excluded.
        """
        segment_id = (os.getenv(ACTIVE_SEGMENT_ID_KEY)
                      or self.__config.get(ACTIVE_SEGMENT_ID_KEY)
                      or '')
        segment_id = str(segment_id).strip()

        if not segment_id:
            raise NonRetryableError(
                f'{ACTIVE_SEGMENT_ID_KEY} is not configured. The weekly report '
                'selects participants from the MDH "Active participants" '
                'segment and will not fall back to all enrolled participants.')

        if not _GUID_PATTERN.match(segment_id):
            raise NonRetryableError(
                f'{ACTIVE_SEGMENT_ID_KEY} is not a GUID. MDH segment ids are '
                'GUIDs, and MDH rejects or ignores anything else, so the query '
                'is refused rather than risking an unfiltered result.')

        return segment_id

    def _project_participant_total(self) -> int:
        """Total participants in the project, fetched with a single-row page.

        Used only to prove the segment filter was applied. MDH silently ignores
        an unrecognised filter parameter and returns the whole project, so a
        segment result equal to the project total is treated as unfiltered.
        """
        url = (ep.MDH_BASE + ep.MDH_PROJ.format(projectID=self.mdh.project_id)
               + 'participants')
        response = self.mdh.makeGetRequests(url, params={'pageNumber': 0,
                                                         'pageSize': 1})
        return response.get('totalParticipants')

    def _get_active_participants(self) -> List[Dict[str, Any]]:
        """Fetch the current members of the MDH "Active participants" segment.

        Queried fresh on every run and never cached, so participants added,
        removed or aged out of the segment are picked up automatically. Fails
        closed: any configuration problem, API failure, unapplied filter,
        incomplete pagination or empty result raises rather than degrading to a
        wider audience.
        """
        segment_id = self._resolve_segment_id()
        try:
            participants_data = self.mdh.getAllParticipants(
                queryParam={'segmentID': segment_id})
            active_participants = []
            
            members = participants_data.get('participants', [])

            # Pagination must have completed. getAllParticipants pages until it
            # has totalParticipants, so a shortfall means a truncated result and
            # some participants would silently miss their report.
            reported_total = participants_data.get('totalParticipants')
            if reported_total is not None and len(members) != reported_total:
                raise NonRetryableError(
                    f'Segment {segment_id} returned {len(members)} participants '
                    f'but MDH reported a total of {reported_total}. Refusing to '
                    'continue with a partial list.')

            # Prove the filter was applied. An unrecognised parameter is ignored
            # by MDH, which hands back the entire project.
            project_total = self._project_participant_total()
            if (project_total and len(members) == project_total):
                raise NonRetryableError(
                    f'Segment {segment_id} returned every participant in the '
                    f'project ({project_total}), so the segmentID filter was '
                    'not applied. Refusing to send reports to an unfiltered '
                    'audience.')

            if not members:
                raise NonRetryableError(
                    f'Segment {segment_id} returned no participants. The '
                    '"Active participants" segment is never legitimately empty, '
                    'so this is treated as a configuration or upstream failure '
                    'rather than a week with nobody to report on.')

            for participant in members:
                active_participants.append({
                    'participant_id': participant.get('participantIdentifier'),
                    'email': participant.get('email', ''),
                    'enrolled_date': participant.get('enrolledDate', '')
                })

            logger.info(
                f"Found {len(active_participants)} participants in MDH segment "
                f"{segment_id} (project total {project_total})")
            return active_participants

        except (NonRetryableError, RetryableError):
            # Already classified above; re-raise untouched so the DLQ reason
            # stays specific instead of being relabelled as an API fault.
            raise
        except Exception as e:
            logger.error(f"Failed to fetch participants: {str(e)}")
            error_data = {'operation': 'mdh_get_participants_by_segment',
                          'segment_id': segment_id}
            handle_api_error(e, error_data, 'mdh_get_participants_by_segment')
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
        "end_date": "2024-01-07"     # Optional, auto-generated if not provided
    }

    Both dates are inclusive. When either is missing, both are auto-generated
    as the last 7 completed days (start = today - 7, end = yesterday).
    
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
            # Auto-generate dates if not provided: the report covers the last 7
            # COMPLETED calendar days, so end_date is yesterday and start_date is
            # today - 7. The schedule fires at 00:00 America/Phoenix, so "today"
            # has barely begun and carries no sleep session yet; including it gave
            # an 8-day window whose end_date was also the next run's start_date.
            today = datetime.date.today()
            end_date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
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