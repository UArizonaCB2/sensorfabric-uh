import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
import re
import awswrangler as wr
import logging

DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)
DEVELOPMENT_EMAIL = "shresth@ultrahuman.com"
DEFAULT_PROD_BASE_URL = "https://partner.ultrahuman.com/api/v1/metrics"
DEFAULT_DEV_BASE_URL = 'https://www.staging.ultrahuman.com/api/v1/metrics'
DEFAULT_DEV_API_KEY = 'eyJhbGciOiJIUzI1NiJ9.eyJzZWNyZXQiOiJjZGM5MjdkYjQ3ZjA5ZDhhNzQxYiIsImV4cCI6MjQzMDEzNTM5Nn0.x3SqFIubvafBxZ1GxvPRqHCd0CLa4_jip8LHbopzLsQ'


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


class UltrahumanAPI:
    """
    API client for Ultrahuman metrics data.
    Supports both development (staging) and production environments.
    
    Environment variables:
    - UH_ENVIRONMENT: 'development' or 'production' (default: 'development')
    - UH_BASE_URL: Base URL for the UH API
    - UH_API_KEY: API key
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Ultrahuman API client.
        
        Parameters
        ----------
        environment : str, optional
            Environment to use ('development' or 'production').
            If not provided, will read from UH_ENVIRONMENT env var.
            Defaults to 'development' if not set.
        """
        self.environment = os.getenv("UH_ENVIRONMENT", None)
        if self.environment is None:
            raise ValueError("UH_ENVIRONMENT environment variable must be set")
        self.__base_url = config.get("base_url", DEFAULT_PROD_BASE_URL)
        self.__api_key = config.get("api_key", None)

        if self.__api_key is None:
            # try getting from environment as a last resort.
            # if still not found, raise error
            logger.debug(f"API key not found in config. Trying environment variables.")
            self.__api_key = os.getenv("UH_API_KEY", None)
            if self.__api_key is None:
                raise ValueError("API key is required")

    @staticmethod
    def _validate_and_format_date(date: Optional[str] = None) -> str:
        """
        Validate and format date input.
        
        Parameters
        ----------
        date : str, optional
            Date in various formats (ISO8601, DD/MM/YYYY, etc.).
            If None, defaults to today's date.
            
        Returns
        -------
        str
            Date in DD/MM/YYYY format required by the API
            
        Raises
        ------
        ValueError
            If the date string is invalid
        """
        if date is None:
            # Default to today's date
            return datetime.now().strftime('%Y-%m-%d')
        
        # If already in YYYY-MM-DD format, validate and return
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            try:
                datetime.strptime(date, '%Y-%m-%d')
                return date
            except ValueError:
                raise ValueError(f"Invalid date: {date}. Expected YYYY-MM-DD format.")
        
        # Try to parse various date formats
        date_formats = [
            '%Y-%m-%d',        # ISO8601 YYYY-MM-DD
            '%Y-%m-%dT%H:%M:%S',  # ISO8601 with time
            '%Y-%m-%dT%H:%M:%SZ', # ISO8601 with time and Z
            '%Y-%m-%d %H:%M:%S',  # YYYY-MM-DD HH:MM:SS
            '%m/%d/%Y',        # MM/DD/YYYY (US format)
            '%d-%m-%Y',        # DD-MM-YYYY
            '%Y%m%d',          # YYYYMMDD
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse date: {date}. Supported formats include ISO8601 (YYYY-MM-DD), DD/MM/YYYY, MM/DD/YYYY, and others.")


    def get_metrics(self, email: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch metrics data for a participant.
        
        Parameters
        ----------
        email : str
            Participant email address
        date : str, optional
            Date in various formats (ISO8601, DD/MM/YYYY, etc.).
            Defaults to today's date if not provided.
            
        Returns
        -------
        dict
            JSON response from the API
            
        Raises
        ------
        requests.RequestException
            If the API request fails
        ValueError
            If the response is not valid JSON or date is invalid
        """
        formatted_date = self._validate_and_format_date(date)
        
        if self.environment == 'development':
            email = DEVELOPMENT_EMAIL

        headers = {
            'Authorization': self.__api_key
        }
        
        params = {
            'email': email,
            'date': formatted_date
        }
        
        try:
            response = requests.get(
                self.__base_url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise requests.RequestException(f"Failed to fetch metrics data: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid JSON response: {e}")
    
    def get_metrics_as_dataframe(self, email: str, date: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch metrics data and return as pandas DataFrame.
        
        Parameters
        ----------
        email : str
            Participant email address
        date : str, optional
            Date in various formats (ISO8601, DD/MM/YYYY, etc.).
            Defaults to today's date if not provided.
            
        Returns
        -------
        pd.DataFrame
            DataFrame containing the metrics data
            
        Raises
        ------
        requests.RequestException
            If the API request fails
        ValueError
            If the response cannot be converted to DataFrame or date is invalid
        """
        formatted_date = self._validate_and_format_date(date)
        if self.environment == 'development':
            email = DEVELOPMENT_EMAIL

        metrics_data = self.get_metrics(email, formatted_date)

        try:
            # Convert JSON response to DataFrame
            # Handle different possible response structures
            if isinstance(metrics_data, dict):
                if 'data' in metrics_data and isinstance(metrics_data['data'], list):
                    # Response has data wrapper
                    df = pd.DataFrame(metrics_data['data'])
                elif isinstance(metrics_data, dict) and len(metrics_data) > 0:
                    # Response is a single record or has multiple fields
                    df = pd.DataFrame([metrics_data])
                else:
                    # Empty response
                    df = pd.DataFrame()
            elif isinstance(metrics_data, list):
                # Response is a list of records
                df = pd.DataFrame(metrics_data)
            else:
                raise ValueError("Unexpected response format")
            
            # Add metadata columns
            df['fetch_timestamp'] = datetime.now().isoformat()
            df['participant_email'] = email
            df['request_date'] = formatted_date
            
            return df
            
        except Exception as e:
            raise ValueError(f"Failed to convert response to DataFrame: {e}")

    def save_metrics_to_s3(self, email: str, date: Optional[str] = None, 
                          s3_path: str = None, bucket: str = None, key: str = None) -> str:
        """
        Fetch metrics data and save directly to S3 as parquet.
        
        Parameters
        ----------
        email : str
            Participant email address
        date : str, optional
            Date in various formats (ISO8601, DD/MM/YYYY, etc.).
            Defaults to today's date if not provided.
        s3_path : str, optional
            Full S3 path (s3://bucket/key). If provided, bucket and key are ignored.
        bucket : str, optional
            S3 bucket name. Required if s3_path not provided.
        key : str, optional
            S3 key/path. Required if s3_path not provided.
            
        Returns
        -------
        str
            S3 path where the data was saved
            
        Raises
        ------
        ValueError
            If neither s3_path nor bucket+key are provided
        """
        if not s3_path and not (bucket and key):
            raise ValueError("Either s3_path or both bucket and key must be provided")
        
        if s3_path:
            full_path = s3_path
        else:
            full_path = f"s3://{bucket}/{key}"
        
        df = self.get_metrics_as_dataframe(email, date)
        
        try:
            # Save directly to S3 using awswrangler
            wr.s3.to_parquet(
                df=df,
                path=full_path,
                index=False
            )
            return full_path
            
        except Exception as e:
            raise ValueError(f"Failed to save to S3: {e}")
    
    def save_metrics_parquet(self, email: str, date: Optional[str] = None, filename: Optional[str] = None) -> str:
        """
        Fetch metrics data and save as parquet file.
        
        Parameters
        ----------
        email : str
            Participant email address
        date : str, optional
            Date in various formats (ISO8601, DD/MM/YYYY, etc.).
            Defaults to today's date if not provided.
        filename : str, optional
            Output filename. If not provided, generates one based on email and date.
            
        Returns
        -------
        str
            Path to the saved parquet file
        """
        formatted_date = self._validate_and_format_date(date)
        if filename is None:
            # Generate filename from email and date
            safe_email = email.replace('@', '_at_').replace('.', '_')
            safe_date = formatted_date.replace('/', '-')
            filename = f"ultrahuman_metrics_{safe_email}_{safe_date}.parquet"
        
        parquet_bytes = self.get_metrics_as_parquet(email, formatted_date)
        
        with open(filename, 'wb') as f:
            f.write(parquet_bytes)
            
        return filename
