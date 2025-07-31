import json
import os
import datetime
import logging
import boto3
from typing import Dict, Any, Optional
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Exception for errors that should trigger SNS retry policy (5xx, 429)."""
    pass


class NonRetryableError(Exception):
    """Exception for errors that should be sent to DLQ (4xx except 429, validation errors)."""
    pass


def is_retryable_error(error: Exception) -> bool:
    """
    Determine if an error should be retried by SNS retry policy.
    
    Retryable errors:
    - HTTP 5xx server errors (MDH/UH API server issues)
    - HTTP 429 rate limiting errors
    - Network/connection errors
    
    Non-retryable errors:
    - HTTP 4xx client errors (except 429)
    - Authentication/authorization errors
    - Validation errors
    - Data parsing errors
    
    Args:
        error: Exception to analyze
        
    Returns:
        True if error should trigger SNS retry policy, False if should go to DLQ
    """
    # Check for HTTP response errors
    if hasattr(error, 'response') and hasattr(error.response, 'status_code'):
        status_code = error.response.status_code
        # Retry on 5xx server errors and 429 rate limiting
        if status_code >= 500 or status_code == 429:
            return True
        # Don't retry on 4xx client errors (except 429)
        elif 400 <= status_code < 500:
            return False
    
    # Check for requests library exceptions
    if isinstance(error, requests.exceptions.RequestException):
        # Retry on connection/timeout errors
        if isinstance(error, (requests.exceptions.ConnectionError, 
                            requests.exceptions.Timeout,
                            requests.exceptions.ConnectTimeout,
                            requests.exceptions.ReadTimeout)):
            return True
        # Don't retry on HTTP errors (handled above) or other request errors
        return False
    
    # Check for AWS service errors
    if isinstance(error, ClientError):
        error_code = error.response.get('Error', {}).get('Code', '')
        # Retry on throttling and server errors
        if error_code in ['Throttling', 'ThrottledException', 'ServiceUnavailable', 'InternalServerError']:
            return True
        # Don't retry on client errors
        elif error_code in ['ValidationException', 'InvalidParameterException', 'AccessDenied']:
            return False
    
    # Check for specific error types
    error_message = str(error).lower()
    
    # Retry on network/connection issues
    if any(keyword in error_message for keyword in ['connection', 'timeout', 'network', 'dns']):
        return True
    
    # Don't retry on validation/parsing errors
    if any(keyword in error_message for keyword in ['validation', 'parse', 'json', 'schema', 'format']):
        return False
    
    # Default to non-retryable for unknown errors to avoid infinite loops
    logger.warning(f"Unknown error type, treating as non-retryable: {type(error).__name__}: {error}")
    return False


def send_to_dlq(error_data: Dict[str, Any], error_message: str, operation: str) -> None:
    """
    Send error data to the dead letter queue.
    
    Args:
        error_data: Dictionary containing data that failed processing
        error_message: Error message describing the failure
        operation: String describing the operation that failed
    """
    dlq_url = os.getenv('UH_DLQ_URL')
    if not dlq_url:
        logger.warning("UH_DLQ_URL not configured, cannot send to dead letter queue")
        return
    
    try:
        sqs_client = boto3.client('sqs')
        
        dlq_message = {
            'error_data': error_data,
            'error_message': error_message,
            'operation': operation,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'function_name': os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'unknown')
        }
        
        message_attributes = {
            'operation': {
                'DataType': 'String',
                'StringValue': operation
            },
            'error_type': {
                'DataType': 'String',
                'StringValue': 'non_retryable_error'
            }
        }
        
        # Add participant_id if available in error_data
        if 'participant_id' in error_data:
            message_attributes['participant_id'] = {
                'DataType': 'String',
                'StringValue': str(error_data['participant_id'])
            }
        
        sqs_client.send_message(
            QueueUrl=dlq_url,
            MessageBody=json.dumps(dlq_message),
            MessageAttributes=message_attributes
        )
        
        logger.info(f"Sent non-retryable error to DLQ for operation: {operation}")
        
    except Exception as e:
        logger.error(f"Failed to send message to dead letter queue: {str(e)}")


def handle_api_error(error: Exception, error_data: Dict[str, Any], operation: str) -> None:
    """
    Handle API errors by either raising for retry or sending to DLQ.
    
    Args:
        error: The exception that occurred
        error_data: Data context for the error (participant info, etc.)
        operation: String describing the operation that failed
        
    Raises:
        RetryableError: If error should trigger SNS retry policy
    """
    if is_retryable_error(error):
        # Log and raise for SNS retry
        logger.warning(f"Retryable error in {operation}: {str(error)}")
        raise RetryableError(f"Retryable error in {operation}: {str(error)}") from error
    else:
        # Log, send to DLQ, and don't raise (return success to prevent retries)
        logger.error(f"Non-retryable error in {operation}: {str(error)}")
        send_to_dlq(error_data, str(error), operation)