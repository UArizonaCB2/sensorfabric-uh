import json
import os
import datetime
from typing import Dict, List, Any, Optional
import logging
import traceback
import copy
import awswrangler as wr
import boto3
import botocore
from sensorfabric.mdh import MDH
from ultrahuman.uh import UltrahumanAPI
from ultrahuman.utils import flatten_json_to_columns, convert_dict_timestamps
from ultrahuman.error_handling import handle_api_error, RetryableError
import pandas as pd
import pytz
import io

# Configure logging
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)
WHITELISTED_TABLES = [
    'avg_sleep_hrv',
    # 'bedtime_end', # these are actually keys in Sleep data that apply to all sub data. 
    # 'bedtime_start',# these are actually keys in Sleep data that apply to all sub data 
    'hr',
    'hr_graph',
    'hrv',
    'movement_graph',
    'night_rhr',
    'quick_metrics',
    'quick_metrics_tiled',
    'sleep_graph',
    'sleep_stages',
    'steps',
    'temp'
]

if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

# suppress boto3 verbose logging
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logger = logging.getLogger()

DEFAULT_DATABASE_NAME = 'uh-biobayb-dev'
DEFAULT_PROJECT_NAME = 'uh-biobayb-dev'
DEFAULT_TIMEZONE = 'UTC'


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
        self.dry_run = False
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

    def _set_dry_run(self, dry_run: bool = False):
        self.dry_run = dry_run
        logger.debug(f"Dry run set to: {self.dry_run}")

    def _set_target_date(self, target_date: Optional[str] = None):
        """Set the target date for data collection."""
        if not target_date:
            # Default to yesterday to ensure data is available
            self.target_date = datetime.datetime.strftime(datetime.datetime.now(datetime.UTC), '%Y-%m-%d')
        else:
            self.target_date = target_date

        logger.debug(f"Collecting data for date: {self.target_date}")

    def _process_metric_data(self, metric: Dict[str, Any], participant_id: str, email: str, target_date: str, timezone: str, uh_sync_timestamp: Optional[int] = None, bedtime_start: Optional[int] = None, bedtime_end: Optional[int] = None) -> Dict[str, Any]:
        """Process a single metric's data and upload to S3.

        Args:
            metric: Single metric data from UH API
            participant_id: Participant identifier
            email: Participant email
            target_date: Target date for data collection
            timezone: Participant timezone
            uh_sync_timestamp: Last sync timestamp to filter data

        Returns:
            Dict with processing results including record count and max timestamp
        """

        metric_type = metric.get('type') if isinstance(metric, dict) else None
        if not metric_type:
            logger.info(f"Empty metric data: {metric_type}")
            logger.debug(f"Empty metric data: {metric}")
            return {'record_count': 0, 'max_timestamp': 0}
        if bedtime_start is not None:
            metric['bedtime_start'] = bedtime_start
        if bedtime_end is not None:
            metric['bedtime_end'] = bedtime_end
        flattened = flatten_json_to_columns(json_data=metric, participant_id=participant_id, fill=True)
        converted = convert_dict_timestamps(flattened, timezone)
        logger.debug(f"Converted data: {converted}")

        try:
            df = pd.DataFrame.from_dict(converted, orient="columns")
        except ValueError as e:
            logger.error(f"Failed to create dataframe from converted data: {str(e)}")
            return {'record_count': 0, 'max_timestamp': 0}

        if df.empty or len(df) == 0:
            logger.info(f"Empty sensor data: {metric_type}")
            return {'record_count': 0, 'max_timestamp': 0}

        logger.debug(f"Dataframe columns: {df.columns}")
        max_timestamp = 0

        if 'object_values_timestamp' in df.columns:
            max_timestamp = df['object_values_timestamp'].max()
            logger.debug(f"New uh timestamp: {max_timestamp}")

        if uh_sync_timestamp is not None and 'object_values_timestamp' in df.columns:
            df = df[df['object_values_timestamp'] > uh_sync_timestamp]
            logger.debug("Removed old timestamp values")

        if df.empty:
            logger.info(f"No new data after timestamp filtering: {metric_type}")
            return {'record_count': 0, 'max_timestamp': max_timestamp}

        # Go ahead and also add the timezone as a column to the frame.
        df['timezone'] = timezone

        if self.dry_run:
            logger.info(f"Dry run: would upload {len(df)} records for {metric_type}")
            return {'record_count': len(df), 'max_timestamp': max_timestamp}

        try:
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
            return {'record_count': len(df), 'max_timestamp': max_timestamp}
        except Exception as e:
            logger.error(f"Failed to upload data to S3: {str(e)}")
            return {'record_count': 0, 'max_timestamp': max_timestamp}

    def _upload_json_data(self, json_obj: Dict[str, Any], participant_id: str, data_date: datetime.date) -> bool:
        """Upload raw JSON data to partitioned S3 path

        Args:
            json_obj: Json dictionary object that we wish to upload
            participant_id : MDH participant id
            data_date: date for this json file

        Returns:
            True if JSON was successfully uploaded to S3, False otherwise
        """

        if self.dry_run:
            return True

        # For MDH each file has a single JSON record. So not worried about contatinating
        # multiple JSON records into a single new-line sepearated json file.
        json_str = json.dumps(json_obj)
        # Creating an in memory buffer. utf-8 should be good. Don't think we have any odd strings.
        json_buffer = io.BytesIO(json_str.encode('utf-8'))

        # Upload everything to s3
        try:
            wr.s3.upload(
                local_file=json_buffer,
                path=f"s3://{self.data_bucket}/raw/json/pid={participant_id}/date={data_date.isoformat()}/data.json"
            )
        except Exception as e:
            logger.error(f"Failed to upload raw JSON data to S3: {str(e)}")
            return False

        return True

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

        # Use target_date from SNS message if available, otherwise use instance target_date
        target_date = participant.get('target_date', self.target_date)
        logger.debug(f"Collecting data for participant {participant_id} on {target_date}")
        if not email:
            logger.warning(f"No email found for participant {participant_id}")
            return {'participant_id': participant_id, 'success': False, 'error': 'No email found'}

        # Get when we last synced data for this participant.
        custom_fields = participant.get('customFields', {})
        uh_sync_date = custom_fields.get('uh_sync_date', None)
        logger.info(f"uh_sync_date - {uh_sync_date}")
        if uh_sync_date is None or len(uh_sync_date) <= 0:
            logger.info("No uh_sync_date found. Falling back to uh_start_date")
            # Fall back to `uh_start_date` if `uh_sync_timestamp` is None
            uh_sync_date = custom_fields.get('uh_start_date', None)

        if uh_sync_date is not None and len(uh_sync_date) > 0:
            # MDH returns this in UTC timezone
            try:
                uh_sync_date = datetime.date.fromisoformat(uh_sync_date)
            except ValueError:
                logger.error(f"Unable to convert {uh_sync_date} into a valid datetime.date object. Format should be YYYY-MM-DD")

            logger.info(f"Found starting timestamp: {uh_sync_date}")
        else:
            logger.info("No uh_sync_date or uh_start_date is found or set. This participant will be ignored")
            return {
                'participant_id': participant_id,
                'success': False,
                'error': 'Neither uh_sync_timestamp or uh_start_date is set'
            }

        # Also get the epoch value from MDH or set to 0 if it is not present.
        uh_sync_epoch = custom_fields.get('uh_sync_epoch', '')
        uh_sync_epoch = int(uh_sync_epoch) if uh_sync_epoch.isnumeric() else 0

        # Right now we will only support looking back 45 days.
        MAX_SYNC_DAYS = 45
        last_uh_timestamp = uh_sync_epoch
        # This now holds the total record count for all days
        record_count = 0
        while uh_sync_date <= datetime.date.fromisoformat(target_date):
            logger.info(f"uh_sync_timestamp - {uh_sync_date}")

            # Get DataFrame for this participant and date
            try:
                json_obj = self.uh_api.get_metrics(email, uh_sync_date.strftime('%Y-%m-%d'))
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

            # Upload the raw JSON S3.
            res = self._upload_json_data(json_obj, participant_id, uh_sync_date)
            if not res:
                logger.error(f"Failed to upload raw json data for {participant_id} for {uh_sync_date.isoformat()}")

            # pull out UH keys -
            # response structure is {"data": {"metric_data": [{}]}
            data = json_obj.get('data', {})
            # Going to save this raw json data into S3.
            # UH is horrible at being consistent with their outputs when there is no data,
            # so this is a horrible hack.
            # Get data sizes for all metrics.
            mmap = {}
            for metrics in data.get('metric_data', []):
                values = metrics.get('object', {}).get('values', [])
                count = len(values)
                last_timestamp = values[-1].get('timestamp', 0) if len(values) > 0 else 0
                mmap[metrics.get('type', 'unkown')] = {
                    'count': count,
                    'last_timestamp': last_timestamp,
                }

            logger.info(f"UH metric counts {participant_id} is {mmap} on {uh_sync_date.isoformat()}")
            # We are going to use temperature as our north star. If that is not there or there is no data
            # we cowardly refuse to process this or do anything for this day.
            if ('temp' not in mmap) or mmap.get('temp').get('count') <= 0:
                # Just move on to the next day.
                uh_sync_date += datetime.timedelta(days=1)
                continue

            # Given that timestamps in every metric are all over the place, and our primary end-point
            # is temperature, we are going to use temperature as our anchor and check for `last_uh_timestamp`
            if last_uh_timestamp >= mmap.get('temp').get('last_timestamp'):
                # Yee we have everything, from this day. Why we are here, is a mystery. Let's move to the next day.
                uh_sync_date += datetime.timedelta(days=1)
                continue

            uh_latest_timezone = data.get('latest_time_zone', DEFAULT_TIMEZONE)
            if type(data) == list:
                return {
                    'participant_id': participant_id,
                    'success': False,
                    'error': 'No data found'
                }
            metrics_data = data.get('metric_data', [])

            for metric in metrics_data:
                metric_type = metric.get('type') if isinstance(metric, dict) else None
                if not metric_type:
                    continue

                # Process Sleep metrics with special handling for embedded sub-metrics
                if metric_type == 'Sleep':
                    logger.debug(f"Processing Sleep metric.")
                    sleep_obj = metric.get('object')
                    for obj in sleep_obj.items():
                        if obj[0] not in WHITELISTED_TABLES:
                            logger.debug(f"Skipping {obj[0]} as it is not whitelisted.")
                            continue
                        newObj = {'type': obj[0], 'object': copy.deepcopy(obj[1])}
                        result = self._process_metric_data(newObj, participant_id, email, uh_sync_date.strftime('%Y-%m-%d'), uh_latest_timezone, last_uh_timestamp, bedtime_start=sleep_obj.get('bedtime_start'), bedtime_end=sleep_obj.get('bedtime_end'))
                        record_count += result['record_count']
                        logger.debug(f"Processed {newObj['type']}: {result['record_count']} records, max_timestamp: {mmap.get('temp').get('last_timestamp')}")
                else:
                    # Process standard metrics
                    if metric_type not in WHITELISTED_TABLES:
                        logger.debug(f"Skipping {metric_type} as it is not whitelisted.")
                        continue
                    result = self._process_metric_data(metric, participant_id, email, uh_sync_date.strftime('%Y-%m-%d'), uh_latest_timezone, last_uh_timestamp, bedtime_start=None, bedtime_end=None)
                    # Update record count and timestamp tracking
                    record_count += result['record_count']

                logger.debug(f"Processed {metric_type}: {result['record_count']} records, max_timestamp: {mmap.get('temp').get('last_timestamp')}")

            # Update the last_uh_timestamp with the one from temperature. So all metrics remained synchronized.
            last_uh_timestamp = mmap.get('temp').get('last_timestamp')

            if self.dry_run:
                logger.info(f"Dry run: would upload {record_count} records for participant {participant_id}")
                return {
                    'participant_id': participant_id,
                    'success': True,
                    'record_count': record_count,
                    'dry_run': True
                }

            if record_count > 0:
                logger.debug(f"Successfully uploaded data for participant {participant_id}")
                # Update participant's sync date in MDH
                logger.debug(f"Updating participant uh_sync_date and uh_sync_epoch for {participant_id} to {uh_sync_date}, {last_uh_timestamp}")
                self._update_participant_sync_date(participant_id, last_uh_timestamp, uh_sync_date)
                logger.debug(f"Finished updating participant uh_sync_* for {participant_id}")

            # Goto the next day if needed.
            uh_sync_date += datetime.timedelta(days=1)

        return {
            'participant_id': participant_id,
            'success': True,
            'record_count': record_count,
        }

    def _update_participant_sync_date(self, participant_id: str, last_sync_epoch: int, last_sync_date: datetime.date) -> None:
        """Update participant's uh_sync_timestamp field in MDH.
        
        Args:
            participant_id: The participant identifier to update
            
        Raises:
            Exception: If update fails
        """
        # Update participant's custom field
        update_data = [
            {
                'participantIdentifier': participant_id,
                'customFields': {
                    'uh_sync_date': last_sync_date.isoformat(),
                    'uh_sync_epoch': str(last_sync_epoch)
                }
            }
        ]

        logger.info(update_data)

        # Use MDH API to update participant
        logger.debug(f"[In _update_participant_sync_date] Updating participant for {participant_id} to {update_data}")
        try:
            self.mdh.update_participants(update_data)
        except Exception as e:
            logger.error(f"[In _update_participant_sync_date] Failed to update for participant {participant_id}: {str(e)}")
            return
        logger.debug(f"[In _update_participant_sync_date] Updated for participant {participant_id} to {update_data}")

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
        dry_run = event.get('dry_run', False)
        uploader._set_dry_run(dry_run)
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
        target_date = datetime.datetime.strftime((datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)), '%Y-%m-%d')
    
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
                        "dry_run": False,
                        "timezone": DEFAULT_TIMEZONE,
                        "custom_fields": {
                            #"uh_sync_timestamp": "2025-09-09T07:00:00Z",
                            "uh_start_date": "2025-09-01",
                            "uh_sync_date": "2025-09-18",
                            "uh_sync_epoch": "1758241904"
                        },
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
