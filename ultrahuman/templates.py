from typing import Dict, Any
import json
import logging
import os
import base64
import boto3
from botocore.exceptions import ClientError
import jwt


logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)

if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


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

    def _validate_jwt_token(self, token: str) -> Dict[str, Any]:
        """
        Validate JWT token and extract participant_id, start_date, end_date.
        
        Args:
            token: JWT token string
            
        Returns:
            Dictionary with decoded token payload
            
        Raises:
            jwt.InvalidTokenError: If token is invalid or expired
        """
        try:
            # Get the REPORT_SECRET from config
            secret = self.__config.get('REPORT_SECRET')
            if not secret:
                raise ValueError("REPORT_SECRET not found in configuration")
            
            # Decode and validate the JWT token
            payload = jwt.decode(
                token,
                secret,
                algorithms=['HS256']
            )
            
            # Validate required fields
            required_fields = ['participant_id', 's3_path']
            for field in required_fields:
                if field not in payload:
                    raise jwt.InvalidTokenError(f"Missing required field: {field}")
            
            logger.info(f"JWT token validated successfully for participant {payload['participant_id']}")
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.error("JWT token has expired")
            raise
        except jwt.InvalidSignatureError:
            logger.error("JWT token has invalid signature")
            raise
        except jwt.InvalidTokenError as e:
            logger.error(f"JWT token validation failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during JWT validation: {str(e)}")
            raise jwt.InvalidTokenError(f"Token validation error: {str(e)}")


    def generate_weekly_report_template(
        self,
        s3_path: str
    ) -> str:
        """
        Fetch pre-generated HTML template from S3.
        
        Args:
            s3_path: S3 path to the pre-generated HTML template

        Returns:
            HTML string from S3
            
        Raises:
            Exception: If S3 fetch fails
        """
        try:
            # Parse S3 path to extract bucket and key
            if not s3_path.startswith('s3://'):
                raise ValueError(f"Invalid S3 path format: {s3_path}")
            
            s3_path_parts = s3_path[5:].split('/', 1)  # Remove 's3://' prefix
            if len(s3_path_parts) != 2:
                raise ValueError(f"Invalid S3 path format: {s3_path}")
            
            bucket_name = s3_path_parts[0]
            object_key = s3_path_parts[1]
            
            # Fetch HTML content from S3
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            html_content = response['Body'].read().decode('utf-8')
            
            logger.info(f"Successfully fetched template from S3: {s3_path}")
            return html_content
            
        except Exception as e:
            logger.error(f"Failed to fetch template from S3 path {s3_path}: {str(e)}")
            raise Exception(f"Template fetch failed: {str(e)}")

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
    AWS Lambda handler for generating weekly health reports with JWT authentication.
    
    Supports both direct lambda invocation and HTTP requests via Function URL.
    
    For Function URL (HTTP GET):
    - Query parameter 't' contains the JWT token
    
    For direct invocation:
    - Event contains 't' field with JWT token
    
    Returns:
    {
        "statusCode": 200,
        "body": "<HTML content>",
        "headers": {...}
    }
    """
    logger.debug(f"Template Generator Lambda started with event: {json.dumps(event)}")

    try:
        # Setup environment with secrets
        secrets = get_secret()
        
        # Initialize template generator
        generator = TemplateGenerator(secrets)

        # Extract JWT token - handle both HTTP and direct invocation
        jwt_token = None

        # Check if this is an HTTP request (Function URL)
        logger.debug(f"Event: {json.dumps(event)}")
        if 'queryStringParameters' in event and event['queryStringParameters']:
            jwt_token = event['queryStringParameters'].get('t')
        # Check if this is a direct lambda invocation
        elif 't' in event:
            jwt_token = event.get('t')

        if not jwt_token:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'jwt token is required'}),
                'headers': {
                    'Content-Type': 'application/json'
                }
            }

        try:
            # Validate JWT token and extract parameters
            payload = generator._validate_jwt_token(jwt_token)
            logger.debug(f"Got payload: {payload}")
            participant_id = payload['participant_id']
            s3_path = payload['s3_path']
            
            logger.info(f"JWT authentication successful for participant {participant_id}")
            
        except jwt.InvalidTokenError as e:
            logger.error(f"JWT validation failed: {str(e)}")
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Invalid or expired JWT token'}),
                'headers': {
                    'Content-Type': 'application/json'
                }
            }

        # Generate the report
        html_report = generator.generate_weekly_report_template(s3_path)

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
