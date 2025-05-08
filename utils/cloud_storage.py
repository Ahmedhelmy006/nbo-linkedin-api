# utils/cloud_storage.py
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any
from google.cloud import storage
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

class CloudStorageManager:
    """
    Manages Google Cloud Storage operations for API call logging.
    """
    
    def __init__(
        self, 
        bucket_name: str = "lookup_status", 
        file_path: str = "rocket_reach_requests.json/rocket_reach_requests.json",
        credentials_path: str = None
    ):
        """
        Initialize the cloud storage manager.
        
        Args:
            bucket_name: Name of the cloud bucket (default: "lookup_status")
            file_path: Path to the JSON file in the bucket
            credentials_path: Path to the service account JSON file
        """
        self.bucket_name = bucket_name
        self.file_path = file_path
        
        # Use environment variable if credentials_path not provided
        if not credentials_path:
            credentials_path = os.environ.get(
                'GOOGLE_APPLICATION_CREDENTIALS',
                'data/service_accounts/setting-up-g-analytics-1cb08bb4364e.json'
            )
        
        try:
            # Initialize Google Cloud Storage client
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.storage_client = storage.Client(credentials=self.credentials)
            self.bucket = self.storage_client.bucket(bucket_name)
            logger.info(f"Successfully initialized Google Cloud Storage client for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Error initializing Google Cloud Storage: {e}")
            raise
    
    def read_json(self) -> Dict:
        """
        Read JSON data from cloud storage.
        
        Returns:
            Dictionary containing the JSON data
        """
        try:
            blob = self.bucket.blob(self.file_path)
            
            # Check if the file exists
            if not blob.exists():
                logger.info(f"File {self.file_path} does not exist in bucket. Creating empty data.")
                return {}
            
            # Download as string and parse JSON
            content = blob.download_as_text()
            data = json.loads(content)
            logger.info(f"Successfully read JSON data from {self.file_path}")
            return data
        except Exception as e:
            logger.error(f"Error reading JSON from cloud storage: {e}")
            return {}
    
    def write_json(self, data: Dict) -> bool:
        """
        Write JSON data to cloud storage.
        
        Args:
            data: Dictionary data to write as JSON
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(self.file_path)
            
            # Convert data to JSON string
            json_data = json.dumps(data, indent=2)
            
            # Upload to bucket
            blob.upload_from_string(json_data, content_type='application/json')
            logger.info(f"Successfully wrote JSON data to {self.file_path}")
            return True
        except Exception as e:
            logger.error(f"Error writing JSON to cloud storage: {e}")
            return False
    
    def update_call_history(self, function_name: str, timestamp: datetime) -> bool:
        """
        Update API call history by adding a new timestamp.
        
        Args:
            function_name: Name of the API function called
            timestamp: When the API was called
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read current data
            data = self.read_json()
            
            # Initialize function list if it doesn't exist
            if function_name not in data:
                data[function_name] = []
            
            # Add timestamp (convert to ISO format for JSON compatibility)
            data[function_name].append(timestamp.isoformat())
            
            # Write updated data back to storage
            success = self.write_json(data)
            
            # Log the result
            if success:
                logger.info(f"Successfully updated call history for {function_name}")
            else:
                logger.warning(f"Failed to update call history for {function_name}")
                
            return success
        except Exception as e:
            logger.error(f"Error updating call history: {e}")
            return False
    
    def get_call_history(self, function_name: str) -> List[datetime]:
        """
        Get the call history for a specific function.
        
        Args:
            function_name: Name of the API function
            
        Returns:
            List of datetime objects representing call timestamps
        """
        try:
            # Read data from storage
            data = self.read_json()
            
            # Get timestamps for the specified function
            timestamps = data.get(function_name, [])
            
            # Convert ISO timestamp strings back to datetime objects
            return [datetime.fromisoformat(ts) for ts in timestamps]
        except Exception as e:
            logger.error(f"Error getting call history: {e}")
            return []