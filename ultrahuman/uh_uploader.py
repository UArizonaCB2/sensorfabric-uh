import json
import os
import datetime
from typing import Dict, List, Any, Optional
import logging
import traceback
import awswrangler as wr
import boto3
import botocore
from sensorfabric.mdh import MDH
from ultrahuman.uh import UltrahumanAPI
from ultrahuman.utils import flatten_json_to_columns, convert_dict_timestamps
from ultrahuman.error_handling import handle_api_error, RetryableError
import pandas as pd
import pytz


# Configure logging
logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.DEBUG)


if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

# suppress boto3 verbose logging
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

DEFAULT_DATABASE_NAME = 'uh-biobayb-dev'
DEFAULT_PROJECT_NAME = 'uh-biobayb-dev'
DEFAULT_TIMEZONE = 'America/Phoenix'


class UltrahumanDataUploader:
    """
    AWS Lambda function for SNS-driven UltraHuman sensor data collection.
    
    This class handles:
    1. Processing SNS messages containing participant data
    2. Collecting UltraHuman sensor data for specified participants
    3. Uploading parquet data to S3 in organized folder structure
    4. Updating participant sync dates in MDH
    """
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        self.mdh = None
        self.uh_api = None
        self.timezone = pytz.timezone(config.get('TIMEZONE', DEFAULT_TIMEZONE))

        # S3 configuration from config
        self.data_bucket = os.getenv('SF_DATA_BUCKET', None)
        self.database_name = os.getenv('SF_DATABASE_NAME', None)
        # data bucket is required for upload. default to 
        if self.data_bucket is None:
            raise ValueError("SF_DATA_BUCKET environment variable must be set")
        if self.database_name is None:
            raise ValueError("SF_DATABASE_NAME environment variable must be set")
        # MDH configuration from config
        self.__mdh_config = {
            'account_secret': config.get('MDH_SECRET_KEY'),
            'account_name': config.get('MDH_ACCOUNT_NAME'),
            'project_id': config.get('MDH_PROJECT_ID'),
        }

        # UH configuration
        self.__uh_config = {
            'environment': os.getenv('UH_ENVIRONMENT', 'production'),
            'api_key': config.get('UH_API_KEY', None),
            'base_url': config.get('UH_BASE_URL', None)
        }

        # Date configuration
        self.target_date = kwargs.get('target_date', None)

    def _check_create_database(self):
        if self.database_name not in wr.catalog.databases():
            logger.debug(f"Database {self.database_name} does not exist. Creating...")
            wr.catalog.create_database(self.database_name, exist_ok=True)
            logger.debug(f"Created database: {self.database_name}")

    def _initialize_connections(self):
        """Initialize MDH and Ultrahuman API connections."""
        try:
            # Initialize MDH connection
            self.mdh = MDH(**self.__mdh_config)
            logger.debug("MDH connection initialized successfully")
            
            # Initialize Ultrahuman API
            self.uh_api = UltrahumanAPI(config=self.__uh_config)
            logger.debug(f"Ultrahuman API initialized for {self.__uh_config['environment']} environment")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {str(e)}")
            # Connection initialization failures are typically retryable (network, auth server issues)
            handle_api_error(e, {'operation': 'connection_initialization'}, 'initialize_connections')
            raise

    def _set_target_date(self, target_date: Optional[str] = None):
        """Set the target date for data collection."""
        if not target_date:
            # Default to yesterday to ensure data is available
            self.target_date = datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=1)), '%Y-%m-%d')
        else:
            self.target_date = target_date

        logger.debug(f"Collecting data for date: {self.target_date}")

    def _process_sns_message(self, sns_message: Dict[str, Any]) -> Dict[str, Any]:
        """Process SNS message to extract participant data.
        
        Args:
            sns_message: SNS message containing participant data
            
        Returns:
            Dict with participant data for UltraHuman collection
            
        Raises:
            ValueError: If required fields are missing
        """
        try:
            # Parse the SNS message body
            message_body = json.loads(sns_message.get('Message', '{}'))
            logger.debug(f"Processing SNS message: {message_body}")
            # Extract required fields
            participant_id = message_body.get('participant_id')
            email = message_body.get('email')
            target_date = message_body.get('target_date')
            
            # Validate required fields
            if not participant_id:
                raise ValueError("Missing required field: participant_id")
            if not email:
                raise ValueError("Missing required field: email")
            if not target_date:
                raise ValueError("Missing required field: target_date")
            
            # Extract optional fields with defaults
            timezone = message_body.get('timezone', DEFAULT_TIMEZONE)
            custom_fields = message_body.get('custom_fields', {})
            
            participant_data = {
                'participantIdentifier': participant_id,
                'email': email,
                'target_date': target_date,
                'timezone': timezone,
                'customFields': custom_fields
            }
            
            logger.debug(f"Processed SNS message for participant {participant_id}")
            return participant_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse SNS message JSON: {str(e)}")
            raise ValueError(f"Invalid JSON in SNS message: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to process SNS message: {str(e)}")
            raise

    def _collect_and_upload_participant_data(self, participant: Dict[str, Any]) -> Dict[str, Any]:
        self._check_create_database()
        """Collect and upload UltraHuman data for a single participant."""
        participant_id = participant.get('participantIdentifier')
        
        # For SNS-based processing, email and timezone come directly from message
        email = participant.get('accountEmail')
        demographics = participant.get('demographics', {})
        if 'email' in participant and participant.get('email') is not None and participant.get('email') != '':
            email = participant.get('email')
        if 'uh_email' in participant and participant.get('uh_email') is not None and participant.get('uh_email') != '':
            email = participant.get('uh_email')
        timezone = demographics.get('timeZone', DEFAULT_TIMEZONE)
        
        # Use target_date from SNS message if available, otherwise use instance target_date
        target_date = participant.get('target_date', self.target_date)
        logger.debug(f"Collecting data for participant {participant_id} on {target_date}")
        if not email:
            logger.warning(f"No email found for participant {participant_id}")
            return {'participant_id': participant_id, 'success': False, 'error': 'No email found'}
        # new uh_sync_timestamp for athena syncing
        uh_sync_timestamp = participant.get('uh_sync_timestamp', None)
        if uh_sync_timestamp is not None:
            uh_sync_timestamp = int(datetime.datetime.fromisoformat(uh_sync_timestamp).timestamp())
            logger.debug(f"Found uh_sync_timestamp: {uh_sync_timestamp}")
        else:
            logger.debug("No uh_sync_timestamp found")

        record_count = 0
        # Get DataFrame for this participant and date
        try:
            json_obj = self.uh_api.get_metrics(email, target_date)
        except Exception as e:
            # Handle UH API errors
            error_data = {
                'participant_id': participant_id,
                'email': email,
                'target_date': target_date,
                'operation': 'uh_api_get_metrics'
            }
            handle_api_error(e, error_data, 'uh_api_get_metrics')
            # If we reach here, it's a non-retryable error that was sent to DLQ
            return {
                'participant_id': participant_id,
                'success': False,
                'error': f'Non-retryable UH API error: {str(e)}'
            }
        # we may want to store the raw json file alongside the parquet files.
        # pull out UH keys -
        # response structure is {"data": {"metric_data": [{}]}
        data = json_obj.get('data', {})
        if type(data) == list:
            return {
                'participant_id': participant_id,
                'success': False,
                'error': 'No data found'
            }
        metrics_data = data.get('metric_data', [])
        last_uh_timestamp = 0
        for metric in metrics_data:
            metric_type = metric.get('type') if type(metric) == dict else None
            if type(metric) == dict and 'object_values' in metric and len(metric['object_values']) <= 0:
                # empty data.
                continue
            flattened = flatten_json_to_columns(json_data=metric, participant_id=participant_id, fill=True)
            converted = convert_dict_timestamps(flattened, timezone)
            # logger.debug(f"Flattened data: {flattened}")
            logger.debug(f"Converted data: {converted}")
            # logger.debug(f"Metric type: {metric_type}")
            # push data into dataframe and then s3 through the wrangler.
            obj_values_value = converted.get('object_values_value', None)
            if obj_values_value is None:
                logger.info(f"Empty sensor data: {metric_type}")
                continue
            df = pd.DataFrame.from_dict(converted, orient="columns")
            if df.empty or 'object_values_timestamp' not in df.columns:
                logger.info(f"Empty sensor data: {metric_type}")
                continue
            else:
                logger.debug(f"Dataframe columns: {df.columns}")
                if 'object_values_timestamp' in df.columns:
                    new_uh_timestamp = df['object_values_timestamp'].max()
                    logger.debug(f"New uh timestamp: {new_uh_timestamp}")
                    if new_uh_timestamp > last_uh_timestamp:
                        last_uh_timestamp = new_uh_timestamp
                else:
                    logger.debug("No object_values_timestamp column")
                if uh_sync_timestamp is not None and 'object_values_timestamp' in df.columns:
                    df = df[df['object_values_timestamp'] > uh_sync_timestamp]
                    logger.debug("Removed old timestamp values")
                else:
                    logger.debug("No uh_sync_timestamp found or no object_values_timestamp column")
                wr.s3.to_parquet(
                    df=df,
                    path=f"s3://{self.data_bucket}/raw/dataset/{metric_type}",
                    dataset=True,
                    database=self.database_name,
                    table=metric_type,
                    s3_additional_kwargs={
                        'Metadata': {
                            'participant_id': participant_id,
                            'participant_email': email,
                            'data_date': target_date,
                            'data_type': 'ultrahuman_metrics',
                            'metric_type': metric_type,
                            'upload_timestamp': datetime.datetime.now(self.timezone).isoformat(),
                            'record_count': str(len(df))
                        }
                    },
                    partition_cols=['pid'],
                    mode='append',
                )
                record_count += len(df)

            logger.debug(f"Successfully uploaded data for participant {participant_id}")
            if record_count > 0:
                # Update participant's sync date in MDH
                logger.debug(f"Updating participant uh_sync_timestamp for {participant_id} to {last_uh_timestamp}...")
                self._update_participant_sync_date(participant_id, last_uh_timestamp)
                logger.debug(f"Finished updating participant uh_sync_timestamp for {participant_id} to {last_uh_timestamp}")
            return {
                'participant_id': participant_id,
                'success': True,
                'record_count': record_count,
            }

    def _update_participant_sync_date(self, participant_id: str, last_sync_timestamp: int) -> None:
        """Update participant's uh_sync_timestamp field in MDH.
        
        Args:
            participant_id: The participant identifier to update
            
        Raises:
            Exception: If update fails
        """
        # Generate current ISO8601 timestamp
        if last_sync_timestamp == 0 or last_sync_timestamp is None:
            logger.debug(f'No uh_sync_timestamp found for participant_id: {participant_id}: last_sync_timestamp: {last_sync_timestamp}')
            return
        last_uh_date = datetime.datetime.fromtimestamp(last_sync_timestamp, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        # Update participant's custom field
        update_data = [
            {
                'participantIdentifier': participant_id,
                'customFields': {
                    'uh_sync_timestamp': last_uh_date
                }
            }
        ] 
        # Use MDH API to update participant
        logger.debug(f"[In _update_participant_sync_date] Updating participant uh_sync_timestamp for {participant_id} to {last_sync_timestamp}...")
        try:
            self.mdh.update_participants(update_data)
        except Exception as e:
            logger.error(f"[In _update_participant_sync_date] Failed to update uh_sync_timestamp for participant {participant_id}: {str(e)}")
            return
        logger.debug(f"[In _update_participant_sync_date] Updated uh_sync_timestamp for participant {participant_id} to {last_sync_timestamp}")

    def process_sns_messages(self, sns_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process SNS messages containing participant data for UltraHuman collection.
        
        Args:
            sns_records: List of SNS records from Lambda event
            
        Returns:
            Dictionary with processing results and statistics
        """
        try:
            # Initialize connections
            self._initialize_connections()
            logger.debug(f"Processing {len(sns_records)} SNS records")
            if not sns_records:
                return {
                    'success': True,
                    'message': 'No SNS records to process',
                    'participants_processed': 0,
                    'successful_uploads': 0,
                    'failed_uploads': 0
                }
            
            # Process each SNS message
            results = []
            successful_uploads = 0
            failed_uploads = 0
            total_data_size = 0
            
            for record in sns_records:
                logger.debug(f"Processing SNS record: {record}")
                try:
                    # Extract participant data from SNS message
                    participant_data = self._process_sns_message(record['Sns'])
                    
                    # Collect and upload data for this participant
                    result = self._collect_and_upload_participant_data(participant_data)
                    results.append(result)
                    
                    if result['success']:
                        successful_uploads += 1
                        total_data_size += result.get('record_count', 0)
                    else:
                        failed_uploads += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process SNS record: {str(e)}")
                    failed_uploads += 1
                    results.append({
                        'participant_id': 'unknown',
                        'success': False,
                        'error': f'SNS processing error: {str(e)}'
                    })
            
            return {
                'success': True,
                'message': f'SNS message processing completed',
                'participants_processed': len(sns_records),
                'successful_uploads': successful_uploads,
                'failed_uploads': failed_uploads,
                'total_records_uploaded': total_data_size,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"SNS message processing failed: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'message': 'SNS message processing failed'
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
    except botocore.exceptions.ClientError as e:
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
    AWS Lambda entry point for UltraHuman data collection via SNS.
    
    Expected event structure (SNS):
    {
        "Records": [
            {
                "EventSource": "aws:sns",
                "Sns": {
                    "Message": "{\"participant_id\": \"123\", \"email\": \"user@example.com\", \"target_date\": \"2023-12-15\", \"timezone\": \"America/Phoenix\"}"
                }
            }
        ]
    }
    
    Environment variables required:
    - SF_DATA_BUCKET: S3 bucket for data storage
    - UH_ENVIRONMENT: Environment to use ('development' or 'production').
    - AWS_SECRET_NAME: Name of the secret in AWS Secrets Manager
    Variables from SecretsManager:
    - MDH_SECRET_KEY: MyDataHelps account secret
    - MDH_ACCOUNT_NAME: MyDataHelps account name
    - MDH_PROJECT_NAME: MyDataHelps project name
    - MDH_PROJECT_ID: MyDataHelps project ID
    - UH_BASE_URL: Development base URL for UltraHuman API
    - UH_API_KEY: Development API key for UltraHuman API
    """
    
    logger.debug(f"UltraHuman SNS data collection Lambda started with event: {json.dumps(event)}")

    # setup environment with secrets
    secrets = get_secret()

    try:
        uploader = UltrahumanDataUploader(config=secrets)
        # logger.info("got event:")
        # logger.info(event)
        # Check if this is an SNS event
        if 'Records' in event:
            # Process SNS records
            sns_records = [record for record in event['Records'] if record.get('EventSource') == 'aws:sns']
            
            if sns_records:
                logger.info(f"Processing {len(sns_records)} SNS records")
                result = uploader.process_sns_messages(sns_records)
            else:
                result = {
                    'success': False,
                    'message': 'No SNS records found in event',
                    'participants_processed': 0,
                    'successful_uploads': 0,
                    'failed_uploads': 0
                }
        else:
            # Legacy mode - log warning and return error
            logger.warning("Received non-SNS event. This Lambda now requires SNS events.")
            result = {
                'success': False,
                'message': 'This Lambda now requires SNS events. Legacy scheduled mode is deprecated.',
                'participants_processed': 0,
                'successful_uploads': 0,
                'failed_uploads': 0
            }
        
        # Prepare Lambda response
        response = {
            'statusCode': 200 if result['success'] else 500,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        logger.info(f"UltraHuman SNS data collection completed")
        logger.debug(f"Result: {json.dumps(result)}")
        return response
        
    except RetryableError as e:
        # Re-raise retryable errors to trigger SNS retry policy
        error_message = f"Retryable error in UltraHuman uploader: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        raise e
        
    except Exception as e:
        error_message = f"UltraHuman SNS data collection Lambda failed: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'UltraHuman SNS data collection Lambda execution failed'
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }


# Convenience function for local testing
def test_locally(participant_id: str = "BB-3234-3734", email: str = "agill2560@gmail.com", target_date: Optional[str] = None):
    """
    Function to test the UltraHuman SNS data collection pipeline locally.
    
    Args:
        participant_id: Test participant ID
        email: Test participant email
        target_date: Optional date string (YYYY-MM-DD) to collect data for specific date
    """
    # Set default target date if not provided
    if not target_date:
        target_date = datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=1)), '%Y-%m-%d')
    
    # Mock SNS event for local testing
    event = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "Sns": {
                    "Message": json.dumps({
                        "participant_id": participant_id,
                        "email": email,
                        "target_date": target_date,
                        "timezone": DEFAULT_TIMEZONE
                    })
                }
            }
        ]
    }
    
    # Mock context object
    class MockContext:
        def __init__(self):
            self.function_name = 'ultrahuman-sns-uploader-local-test'
            self.aws_request_id = 'local-test-123'
    
    context = MockContext()
    
    # Run the lambda handler
    response = lambda_handler(event, context)
    
    print("Response:")
    print(json.dumps(json.loads(response['body']), indent=2))
    
    return response

# Legacy test function for backward compatibility
def test_locally_legacy(target_date: Optional[str] = None):
    """
    Legacy test function - now deprecated.
    
    Args:
        target_date: Optional date string (YYYY-MM-DD) to collect data for specific date
    """
    print("WARNING: test_locally_legacy is deprecated. Use test_locally() with SNS parameters.")
    
    # Mock legacy event
    event = {}
    if target_date:
        event['target_date'] = target_date
    
    # Mock context object
    class MockContext:
        def __init__(self):
            self.function_name = 'ultrahuman-data-uploader-legacy-test'
            self.aws_request_id = 'legacy-test-123'
    
    context = MockContext()
    
    # Run the lambda handler
    response = lambda_handler(event, context)
    
    print("Response:")
    print(json.dumps(json.loads(response['body']), indent=2))
    
    return response


if __name__ == "__main__":
    # For local testing with SNS
    test_locally()
