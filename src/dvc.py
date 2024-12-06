####################################################################################################
# 
# This script contains functions to versionize data, upload files to an Azure Blob Storage container automatically 

from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from io import StringIO 

import subprocess
import json
from dotenv import load_dotenv
import os
import re
import pandas as pd

def extract_data_from_filename(filename: str):
    """ Extract data from a filename using a regular expression pattern.

    The expected filename format is: 'YYYYMMDD_EID_POSITION_NAME_DAILYCOUNT.txt'

    Returns: A dictionary containing the extracted data.
        {"date": "YYYYMMDD", 
        "exercise": "Exercise Name", 
        "position": "Position Name", 
        "name": "Name", 
        "daily_count": "Daily Count"}
    """
    # Define mappings based on the provided information
    exercise_mapping = {
        0: "Walk",
        1: "Squat",
        2: "Sit-Ups",
        3: "Bizeps Curl",
        4: "Push-Up"
    }
    
    position_mapping = {
        0: "Pocket",
        1: "Wrist"
    }
    
    # Regular expression pattern to match the filename format
    pattern = r"(\d{8})_(\d)_([01])_(\w+)_(\w+)\.txt"
    
    # Use regex to extract components from the filename
    match = re.match(pattern, filename)
    
    if match:
        # Extract the components from the matched groups
        date = match.group(1)  # YYYYMMDD
        eid = int(match.group(2))  # Exercise ID (integer)
        position = int(match.group(3))  # Position (integer)
        name = match.group(4)  # Name
        daily_count = match.group(5)  # Daily Count
        
        # Map EID and Position to their full names using the provided dictionaries
        exercise = exercise_mapping.get(eid, "Unknown")
        position_name = position_mapping.get(position, "Unknown")
        
        # Create the resulting dictionary
        data = {
            "date": date,
            "exercise": exercise,
            "position": position_name,
            "name": name,
            "daily_count": daily_count
        }
        
        return data
    else:
        raise ValueError(f"Filename '{filename}' does not match expected format.")

class MyAzureClient(BaseEstimator, TransformerMixin):
    """
    Base class for Azure clients that provides common functionality such as 
    loading environment variables and creating an MLClient instance.
    """

    def __init__(self):
        load_dotenv()
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.resource_group = os.getenv("AZURE_RESOURCE_GROUP")
        self.workspace = os.getenv("AZURE_WORKSPACE")    
        self.storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        self.connection_string = self.get_connection_string(self.storage_account, self.resource_group) if not os.getenv("CONNECTION_STRING") else os.getenv("CONNECTION_STRING") 
        self.shared_access_token = os.getenv("SHARED_ACCESS_TOKEN")

        if not self.subscription_id: 
            raise ValueError("No Azure subscription ID found.")
        if not self.resource_group:
            raise ValueError("No Azure resource group found.")
        if not self.workspace:
            raise ValueError("No Azure workspace found.")
        if not self.storage_account:
            raise ValueError("No Azure storage account found.")

        self.ml_client = MLClient(
            DefaultAzureCredential(), self.subscription_id, self.resource_group, self.workspace
        )
    
    @staticmethod
    def get_connection_string(storage_account: str, resource_group: str):
        """ Get the connection string for an Azure Storage account using the Azure CLI.

        Args:
            storage_account (str): The name of the Azure Storage account.
            resource_group (str): The name of the resource group containing the storage account.
        """

        try:
            # Run the Azure CLI command to get the connection string for the storage account
            command = [
                "az", "storage", "account", "show-connection-string",
                "--name", storage_account,
                "--resource-group", resource_group,
                "--query", "connectionString",  # Use a JMESPath query to get just the connection string
                "--output", "json"  # Output as JSON format
            ]

            # Execute the command and capture the output
            result = subprocess.run(command, capture_output=True, text=True, check=True)

            # Parse the JSON output and extract the connection string
            connection_string = json.loads(result.stdout.strip())

            if connection_string:
                return connection_string
            else:
                raise ValueError("Could not retrieve the connection string.")
        
        except subprocess.CalledProcessError as e:
            print(f"Error running 'az' command: {e.stderr}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"Raw output: {result.stdout}")
            return None
        except Exception as ex:
            print(f"An error occurred: {ex}")
            return None

    def fit(self, X, y=None):
        # No fitting logic needed
        return self

class AzureDataUploader(MyAzureClient):
    """
    Custom class that uploads files to an Azure Blob Storage container and adds tags to the blobs.
    """

    def __init__(self):
        super().__init__()

    @staticmethod 
    def add_tags_to_blob(blob_client: BlobClient):
        """
        Adds tags to a blob based on the information extracted from its name.

        Args:
            blob_client: The blob object to add tags to.
        """
        # Extract the data from the blob's name using the extract_data_from_filename function
        try:
            extracted_data = extract_data_from_filename(blob_client.blob_name)
        except ValueError as e:
            print(f"Skipping blob '{blob_client.blob_name}' due to error: {e}")
            return

        # Prepare tags based on extracted data
        new_tags = {
            "date": extracted_data["date"],
            "exercise": extracted_data["exercise"],
            "position": extracted_data["position"],
            "name": extracted_data["name"],
            "daily_count": extracted_data["daily_count"]
        }

        tags = blob_client.get_blob_tags()
        tags.update(new_tags)   

        try:
            blob_client.set_blob_tags(tags)
            print(f"Tags added to blob '{blob_client.blob_name}': {new_tags}")
        except ResourceNotFoundError as e:
            print(f"Blob '{blob_client.blob_name}' not found in the container.")
        except Exception as e:
            print(f"Error adding tags to blob '{blob_client.blob_name}': {e}") 

    def versionize_data(self):
        """
        Versionize the data in the Azure Blob Storage container each time new data is uploaded.
        """

        ml_client = MLClient(
            DefaultAzureCredential(), self.subscription_id, self.resource_group, self.workspace
        )
        path = "https://mlopsgroup023113987413.blob.core.windows.net/movements"
        my_data = Data(
            path=path,
            type=AssetTypes.URI_FOLDER,
            name="movements_test"
        )
        
        ml_client.data.create_or_update(my_data)

    def transform(self, X: str):
        """
        Main function to upload files to an Azure Blob Storage container and add tags to the blobs.

        Args:
            X: The input data, expected to be a directory path on the local machine.
        """

        if not os.path.exists(X):
            raise FileNotFoundError(f"Path {X} does not exist on the local machine.")

        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_name = "movements"
        container_client = blob_service_client.get_container_client(container_name)

        for filename in os.listdir(X):
            file_path = os.path.join(X, filename)

            if os.path.isdir(file_path) or not filename.endswith(".txt"):
                continue
            
            blob_client = container_client.get_blob_client(filename)

            try:
                # Check if the blob already exists by trying to fetch its properties
                blob_client.get_blob_properties()
            except Exception as e:
                print(f"Uploading file '{filename}' to container '{container_name}'...")
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data)
                print(f"File '{filename}' uploaded successfully.")

                # Add tags to the blob
                self.add_tags_to_blob(blob_client)
        
        self.versionize_data()

        return X  

class AzureDataLoader(MyAzureClient):
    """
    Custom class to load data from an Azure Blob Storage container.
    """

    def __init__(self) -> None:
        super().__init__()
        self.container_client = ContainerClient.from_container_url(self.shared_access_token)

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        """
        Load data from Azure Blob Storage and return a list of DataFrames.
        Add extracted data from the file name as new columns to the DataFrames.

        Returns:
            all_dfs: A list of DataFrames containing the loaded data.
        """

        all_dfs = []
        for blob in self.container_client.list_blobs():
            print(f"Reading blob: {blob.name}")
            # Create a BlobClient for each blob
            blob_client = self.container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob()

            # Assuming the blobs are CSV files
            blob_content = blob_data.content_as_text()
            blob_name = blob_client.blob_name

            df = pd.read_csv(StringIO(blob_content), sep='\t')

            # Add the extracted data as columns to the DataFrame
            extracted_data = extract_data_from_filename(blob_name)
            for key, value in extracted_data.items():
                df[key] = value

            all_dfs.append(df)  # Store the DataFrame

        return all_dfs


