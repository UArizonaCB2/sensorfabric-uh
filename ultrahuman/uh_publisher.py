import json
import os
import datetime
from typing import Dict, List, Any, Optional
import logging
import traceback
import boto3
from botocore.exceptions import ClientError

from sensorfabric.mdh import MDH
from ultrahuman.error_handling import handle_api_error, RetryableError, NonRetryableError


# Configure logging
logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

# suppress boto3 verbose logging
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


DEFAULT_PROJECT_NAME = 'uh-biobayb-dev'


class UltrahumanSNSPublisher:
    """
    AWS Lambda function for publishing UltraHuman data collection requests via SNS.
    
    This class handles:
    1. Fetching active participants from MDH
    2. Publishing SNS messages for each participant to trigger data collection
    3. Dead letter queue handling for failed message publishing
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.__config = config
        self.mdh = None
        self.sns_client = None
        self.dry_run = False
        # SNS configuration from environment variables
        self.sns_topic_arn = os.getenv('UH_SNS_TOPIC_ARN')
        self.dead_letter_queue_url = os.getenv('UH_DLQ_URL')
        
        # Validate required environment variables
        if not self.sns_topic_arn:
            raise ValueError("UH_SNS_TOPIC_ARN environment variable must be set")
        
        # Date configuration
        self.target_date = None
        
        # Initialize AWS clients
        self.sns_client = boto3.client('sns')
        self.sqs_client = boto3.client('sqs') if self.dead_letter_queue_url else None

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
            logger.debug("MDH connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MDH connection: {str(e)}")
            # MDH connection failures are typically retryable
            handle_api_error(e, {'operation': 'mdh_connection_initialization'}, 'initialize_mdh_connection')
            raise

    def _set_dry_run(self, dry_run: bool = False):
        self.dry_run = dry_run
        logger.debug(f"Dry run set to: {self.dry_run}")

    def _set_target_date(self, target_date: Optional[str] = None):
        """Set the target date for data collection."""
        if not target_date:
            # Default to yesterday to ensure data is available
            self.target_date = datetime.datetime.strftime(
                (datetime.datetime.now() - datetime.timedelta(days=1)), 
                '%Y-%m-%d'
            )
        else:
            self.target_date = target_date

        logger.debug(f"Target date set to: {self.target_date}")

    def _get_active_participants(self) -> List[Dict[str, Any]]:
        """Fetch active participants from MDH."""
        try:
            participants_data = self.mdh.getAllParticipants()
            active_participants = []
            
            for participant in participants_data.get('participants', []):
                # Filter for active participants
                custom_fields = participant.get('customFields', {})
                last_sync_date = custom_fields.get('uh_sync_date', None)
                # check last sync date to make sure we don't oversync
                if last_sync_date is None:
                    continue
                if participant.get('enrolled'):
                    active_participants.append(participant)

            logger.debug(f"Found {len(active_participants)} active participants")
            return active_participants

        except Exception as e:
            logger.error(f"Failed to fetch participants: {str(e)}")
            # Handle MDH API errors
            error_data = {'operation': 'mdh_get_all_participants'}
            handle_api_error(e, error_data, 'mdh_get_all_participants')
            raise

    def _extract_participant_email(self, participant: Dict[str, Any]) -> Optional[str]:
        """Extract email from participant data with fallback logic."""
        custom_fields = participant.get('customFields', {})
        demographics = participant.get('demographics', {})
        
        # Priority order: custom uh_email, demographics email, account email
        custom_email = custom_fields.get('uh_email')
        demographics_email = demographics.get('email')
        account_email = participant.get('accountEmail')
        
        if custom_email and len(custom_email) > 0:
            return custom_email
        elif demographics_email and len(demographics_email) > 0:
            return demographics_email
        elif account_email and len(account_email) > 0:
            return account_email
        
        return None

    def _publish_sns_message(self, participant: Dict[str, Any]) -> Dict[str, Any]:
        """Publish SNS message for a single participant."""
        participant_id = participant.get('participantIdentifier')
        email = self._extract_participant_email(participant)
        
        if not email:
            logger.warning(f"No email found for participant {participant_id}")
            return {
                'participant_id': participant_id,
                'success': False,
                'error': 'No email found for participant'
            }
        
        # Extract timezone from custom fields
        custom_fields = participant.get('customFields', {})
        timezone = custom_fields.get('timeZone', 'America/Phoenix')
        
        # Create SNS message payload
        message_data = {
            'participant_id': participant_id,
            'email': email,
            'target_date': self.target_date,
            'timezone': timezone,
            'dry_run': self.dry_run,
            'custom_fields': custom_fields
        }
        
        try:
            # Publish message to SNS topic
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=json.dumps(message_data),
                Subject=f'UltraHuman Data Collection Request - {participant_id}',
                MessageAttributes={
                    'participant_id': {
                        'DataType': 'String',
                        'StringValue': participant_id
                    },
                    'target_date': {
                        'DataType': 'String',
                        'StringValue': self.target_date
                    },
                    'operation': {
                        'DataType': 'String',
                        'StringValue': 'uh_data_collection'
                    }
                }
            )
            
            logger.info(f"Successfully published SNS message for participant {participant_id}")
            return {
                'participant_id': participant_id,
                'success': True,
                'message_id': response['MessageId'],
                'email': email
            }
            
        except Exception as e:
            error_msg = f"Failed to publish SNS message for participant {participant_id}: {str(e)}"
            logger.error(error_msg)
            
            # Handle SNS publish errors
            error_data = {
                'participant_id': participant_id,
                'participant_data': participant,
                'operation': 'sns_publish'
            }
            
            try:
                handle_api_error(e, error_data, 'sns_publish')
            except RetryableError:
                # Re-raise retryable errors to trigger SNS retry
                raise
            
            # Non-retryable error was sent to DLQ, return success to prevent retries
            return {
                'participant_id': participant_id,
                'success': True,  # Return success to prevent SNS retries
                'message': f'Non-retryable error sent to DLQ: {error_msg}'
            }

    def _send_to_dead_letter_queue(self, participant: Dict[str, Any], error_message: str):
        """Send failed participant data to dead letter queue."""
        if not self.sqs_client or not self.dead_letter_queue_url:
            logger.warning("Dead letter queue not configured, skipping DLQ send")
            return
        
        try:
            dlq_message = {
                'participant_data': participant,
                'target_date': self.target_date,
                'error_message': error_message,
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'operation': 'uh_sns_publish_failed'
            }
            
            self.sqs_client.send_message(
                QueueUrl=self.dead_letter_queue_url,
                MessageBody=json.dumps(dlq_message),
                MessageAttributes={
                    'participant_id': {
                        'DataType': 'String',
                        'StringValue': participant.get('participantIdentifier', 'unknown')
                    },
                    'error_type': {
                        'DataType': 'String',
                        'StringValue': 'sns_publish_failed'
                    }
                }
            )
            
            logger.info(f"Sent failed participant {participant.get('participantIdentifier')} to dead letter queue")
            
        except Exception as e:
            logger.error(f"Failed to send message to dead letter queue: {str(e)}")

    def publish_participant_messages(self, target_date: Optional[str] = None, participant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Main orchestration method for publishing SNS messages for all active participants.
        
        Args:
            target_date: Optional date string (YYYY-MM-DD) for data collection
            
        Returns:
            Dictionary with publishing results and statistics
        """
        try:
            # Initialize connections
            self._initialize_connections()
            
            # Set target date
            self._set_target_date(target_date)
            
            if participant_id is not None:
                participants = [self.mdh.getParticipant(participant_id)]
            # Get active participants
            else:
                participants = self._get_active_participants()
            
            logger.info(f"Found participants: {participants}")

            if not participants:
                return {
                    'success': True,
                    'message': 'No active participants found',
                    'target_date': self.target_date,
                    'participants_processed': 0,
                    'successful_publishes': 0,
                    'failed_publishes': 0,
                    'results': []
                }
            
            # Publish SNS messages for each participant
            results = []
            successful_publishes = 0
            failed_publishes = 0
            for participant in participants:
                result = self._publish_sns_message(participant)
                results.append(result)
                if result['success']:
                    successful_publishes += 1
                else:
                    failed_publishes += 1

            return {
                'success': True,
                'message': f'SNS message publishing completed for {self.target_date}',
                'target_date': self.target_date,
                'participants_processed': len(participants),
                'successful_publishes': successful_publishes,
                'failed_publishes': failed_publishes,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"SNS message publishing failed: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'message': 'SNS message publishing failed'
            }


def get_secret():
    """
    Uses secretmanager to fill in MDH secrets
    """
    secret_name = os.getenv("AWS_SECRET_NAME")
    region_name = os.getenv("AWS_REGION", "us-east-1")
    
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
    AWS Lambda entry point for publishing UltraHuman data collection requests via SNS.
    
    Expected event structure:
    {
        "target_date": "2023-12-15"  # Optional: specific date to collect data for
    }
    
    Environment variables required:
    - UH_SNS_TOPIC_ARN: SNS topic ARN for publishing messages
    - UH_DLQ_URL: SQS queue URL for dead letter messages (optional)
    - UH_ENVIRONMENT: Environment to use ('development' or 'production').
    - AWS_SECRET_NAME: Name of the secret in AWS Secrets Manager
    Variables from SecretsManager:
    - MDH_SECRET_KEY: MyDataHelps account secret
    - MDH_ACCOUNT_NAME: MyDataHelps account name
    - MDH_PROJECT_NAME: MyDataHelps project name
    - MDH_PROJECT_ID: MyDataHelps project ID
    - UH_DEV_BASE_URL: Development base URL for UltraHuman API
    - UH_DEV_API_KEY: Development API key for UltraHuman API
    - UH_PROD_BASE_URL: Production base URL for UltraHuman API
    - UH_PROD_API_KEY: Production API key for UltraHuman API
    """
    
    logger.debug(f"UltraHuman SNS Publisher Lambda started with event: {json.dumps(event)}")

    # setup environment with secrets
    secrets = get_secret()

    try:
        publisher = UltrahumanSNSPublisher(config=secrets)

        # Extract target date from event if provided
        target_date = event.get('target_date', None)
        participant_id = event.get('participant_id', None)
        dry_run = event.get('dry_run', False)
        publisher._set_dry_run(dry_run)
        # Publish messages for all participants
        if participant_id is not None:
            result = publisher.publish_participant_messages(target_date, participant_id)
        else:
            result = publisher.publish_participant_messages(target_date)
        
        # Prepare Lambda response
        response = {
            'statusCode': 200 if result['success'] else 500,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
        logger.debug(f"UltraHuman SNS Publisher completed: {json.dumps(result)}")
        return response
        
    except RetryableError as e:
        # Re-raise retryable errors to trigger retry mechanism (if publisher is triggered by events)
        error_message = f"Retryable error in UltraHuman publisher: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        raise e
        
    except Exception as e:
        error_message = f"UltraHuman SNS Publisher Lambda failed: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'UltraHuman SNS Publisher Lambda execution failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


# Convenience function for local testing
def test_locally(target_date: Optional[str] = None):
    """
    Function to test the UltraHuman SNS Publisher pipeline locally.
    
    Args:
        target_date: Optional date string (YYYY-MM-DD) for data collection
    """
    # Mock event for local testing
    event = {}
    if target_date:
        event['target_date'] = target_date
    
    # Mock context object
    class MockContext:
        def __init__(self):
            self.function_name = 'ultrahuman-sns-publisher-local-test'
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
