#!/usr/bin/env python3

"""
Author : Shravan Aras.
Email : shravanaras@arizona.edu
Date : 09/18/2023
Organization : University of Arizona

This package is part of the SensorFabric project.

Description :
Just a set of utility files to help out.
"""
from datetime import datetime
from typing import Dict, List, Any, Union, Optional
import pytz
import jsonschema
import json
import os


def flatten_json_to_columns(json_data: Dict[str, Any], fill: bool = False, separator: str = "_", participant_id: Optional[str] = None) -> Dict[str, List[Any]]:
    """
    Flatten a JSON structure from row-oriented to column-oriented format.
    
    This function takes a JSON object and converts it to a columnar format where:
    - Scalar values become single-element lists (or repeated if fill=True)
    - Arrays become columns with their values
    - Nested objects get flattened with separator-joined keys
    
    Parameters
    ----------
    json_data : dict
        The input JSON object to flatten
    fill : bool, default False
        If True, scalar values are repeated to match the length of the longest array
        If False, scalar values become single-element lists
    separator : str, default "_"
        Separator used to join nested object keys
        
    Returns
    -------
    dict
        Column-oriented dictionary where each key maps to a list of values
        
    Examples
    --------
    >>> input_data = {
    ...     "device_name": "AppleWatch",
    ...     "data_type": "hr", 
    ...     "values": [{"timestamp": 1000.0, "value": 84}, {"timestamp": 1600.0, "value": 85}]
    ... }
    >>> flatten_json_to_columns(input_data, fill=False)
    {'device_name': ['AppleWatch'], 'data_type': ['hr'], 'values_timestamp': [1000.0, 1600.0], 'values_value': [84, 85]}
    
    >>> flatten_json_to_columns(input_data, fill=True)  
    {'device_name': ['AppleWatch', 'AppleWatch'], 'data_type': ['hr', 'hr'], 'values_timestamp': [1000.0, 1600.0], 'values_value': [84, 85]}
    """
    result = {}
    
    # insert participant ID
    if participant_id is not None:
        json_data['pid'] = participant_id

    # First pass: flatten all values and identify the maximum length
    max_length = 1
    
    def _flatten_recursive(obj: Any, prefix: str = "") -> Dict[str, List[Any]]:
        """Recursively flatten nested structures."""
        flattened = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                flattened.update(_flatten_recursive(value, new_key))
        elif isinstance(obj, list):
            if not obj:
                # Empty list
                flattened[prefix] = []
            elif all(isinstance(item, dict) for item in obj):
                # List of dictionaries - flatten each dict and combine
                all_keys = set()
                for item in obj:
                    all_keys.update(item.keys())
                
                for key in all_keys:
                    new_key = f"{prefix}{separator}{key}" if prefix else key
                    values = []
                    for item in obj:
                        values.append(item.get(key))
                    flattened[new_key] = values
            else:
                # List of scalars or mixed types
                flattened[prefix] = obj
        else:
            # Scalar value
            flattened[prefix] = [obj]
            
        return flattened
    
    # Flatten the JSON structure
    flattened_data = _flatten_recursive(json_data)
    
    # Find the maximum length among all arrays
    if fill:
        for values in flattened_data.values():
            if isinstance(values, list) and len(values) > max_length:
                max_length = len(values)
    
    # Second pass: apply fill logic if needed
    for key, values in flattened_data.items():
        if fill and isinstance(values, list) and len(values) == 1 and max_length > 1:
            # Replicate single values to match max_length
            result[key] = values * max_length
        else:
            result[key] = values
    
    return result


def convert_dict_timestamps(data: Union[Dict[str, Any], List[Any]], timezone: Optional[str] = None) -> Union[Dict[str, Any], List[Any]]:
    """
    Recursively convert Unix timestamps to ISO8601 format in a dictionary or list.
    
    This function processes dictionaries and lists recursively, looking for keys that contain
    "timestamp", "_start", or "_end". For matching keys, it creates additional keys with
    ISO8601 formatted timestamps in UTC and optionally in a specified timezone.
    
    Parameters
    ----------
    data : dict or list
        The input data structure to process
    timezone : str, optional
        Timezone string (e.g., "America/Phoenix") to create additional timezone-aware timestamps
        
    Returns
    -------
    dict or list
        The processed data structure with additional ISO8601 timestamp keys
        
    Examples
    --------
    >>> input_data = {
    ...     "timestamp": [1672531200, 1672531201],
    ...     "object_sleep_graph_data_start": [1672531200, 1672531200],
    ...     "nested": {
    ...         "inner_timestamp": 1672531200
    ...     }
    ... }
    >>> convert_dict_timestamps(input_data, "America/Phoenix")
    {
        "timestamp": [1672531200, 1672531201],
        "object_sleep_graph_data_start": [1672531200, 1672531200],
        "nested": {
            "inner_timestamp": 1672531200,
            "inner_timestamp_iso8601": "2023-01-01T00:00:00Z",
            "inner_timestamp_iso8601_tz": "2022-12-31T17:00:00-07:00"
        },
        "timestamp_iso8601": ["2023-01-01T00:00:00Z", "2023-01-01T00:00:01Z"],
        "object_sleep_graph_data_start_iso8601": ["2023-01-01T00:00:00Z", "2023-01-01T00:00:00Z"],
        "timestamp_iso8601_tz": ["2022-12-31T17:00:00-07:00", "2022-12-31T17:00:01-07:00"],
        "object_sleep_graph_data_start_iso8601_tz": ["2022-12-31T17:00:00-07:00", "2022-12-31T17:00:00-07:00"]
    }
    """

    def _is_timestamp_key(key: str) -> bool:
        """Check if a key contains timestamp-related terms."""
        key_lower = key.lower()
        return any(term in key_lower for term in ["timestamp", "_start", "_end"])
    
    def _convert_unix_to_iso8601(unix_timestamp: Union[int, float], target_timezone: Optional[str] = None) -> str:
        """Convert Unix timestamp to ISO8601 format."""
        try:
            # Convert to UTC datetime
            dt_utc = datetime.fromtimestamp(unix_timestamp)
            
            if target_timezone:
                # Convert to specified timezone
                target_tz = pytz.timezone(target_timezone)
                dt_tz = dt_utc.astimezone(target_tz)
                return dt_tz.isoformat()
            else:
                # Return UTC with Z suffix
                return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError, OSError):
            # Return original value if conversion fails
            return str(unix_timestamp)
    
    def _process_timestamp_value(value: Any, target_timezone: Optional[str] = None) -> Any:
        """Process a timestamp value (single value or list)."""
        if isinstance(value, (int, float)):
            return _convert_unix_to_iso8601(value, target_timezone)
        elif isinstance(value, list):
            return [_convert_unix_to_iso8601(v, target_timezone) if isinstance(v, (int, float)) else v for v in value]
        else:
            return value
    
    def _process_dict(d: Dict[str, Any]) -> Dict[str, Any]:
        """Process a dictionary recursively."""
        result = {}
        timestamp_keys = []
        
        # First pass: process all existing keys recursively
        for key, value in d.items():
            if isinstance(value, dict):
                result[key] = _process_dict(value)
            elif isinstance(value, list):
                result[key] = _process_list(value)
            else:
                result[key] = value
            
            # Track timestamp keys for later processing
            if _is_timestamp_key(key):
                timestamp_keys.append(key)
        
        # Second pass: add ISO8601 versions of timestamp keys
        for key in timestamp_keys:
            value = result[key]
            
            # Add UTC ISO8601 version
            iso8601_key = f"{key}_iso8601"
            result[iso8601_key] = _process_timestamp_value(value)
            
            # Add timezone-specific version if timezone is provided
            if timezone:
                iso8601_tz_key = f"{key}_iso8601_tz"
                result[iso8601_tz_key] = _process_timestamp_value(value, timezone)
        
        return result
    
    def _process_list(lst: List[Any]) -> List[Any]:
        """Process a list recursively."""
        result = []
        for item in lst:
            if isinstance(item, dict):
                result.append(_process_dict(item))
            elif isinstance(item, list):
                result.append(_process_list(item))
            else:
                result.append(item)
        return result
    
    # Main processing logic
    if isinstance(data, dict):
        return _process_dict(data)
    elif isinstance(data, list):
        return _process_list(data)
    else:
        return data


def validate_sensor_data_schema(json_data: List[Dict[str, Any]]) -> None:
    """
    Validate the sensor data against the JSON schema.
    
    Args:
        json_data: The sensor data to validate
        
    Raises:
        jsonschema.ValidationError: If validation fails
        FileNotFoundError: If schema file is not found
    """
    # Get the schema file path relative to this module
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, 'schemas', 'sensor_data_schema_flattened.json')
    
    try:
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")
    
    # Validate the data against the schema
    try:
        jsonschema.validate(json_data, schema)
    except jsonschema.ValidationError as e:
        raise jsonschema.ValidationError(f"Sensor data validation failed: {e.message}")
