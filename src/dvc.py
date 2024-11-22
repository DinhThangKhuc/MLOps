####################################################################################################
# 
# This script contains functions to versionize data, upload files to an Azure Blob Storage container automatically 

from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError

import subprocess
import json
from dotenv import load_dotenv
import os
import re

# Load credentials from the .env file
load_dotenv()
subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
resource_group = os.getenv("AZURE_RESOURCE_GROUP")
workspace = os.getenv("AZURE_WORKSPACE")    
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")

if not subscription_id: 
    raise ValueError("No Azure subscription ID found.")
if not resource_group:
    raise ValueError("No Azure resource group found.")
if not workspace:
    raise ValueError("No Azure workspace found.")
if not storage_account:
    raise ValueError("No Azure storage account found.")

def versionize_data(ds_name: str, author: str):
    ml_client = MLClient(
        DefaultAzureCredential(), subscription_id, resource_group, workspace
    )

    # Set the path, supported paths include:
    # local: './<path>/<file>' (this will be automatically uploaded to cloud storage)
    # blob:  'wasbs://<container_name>@<account_name>.blob.core.windows.net/<path>/<file>'
    # ADLS gen2: 'abfss://<file_system>@<account_name>.dfs.core.windows.net/<path>/<file>'
    # Datastore: 'azureml://datastores/<data_store_name>/paths/<path>/<file>'
    path = "https://mlopsgroup023113987413.blob.core.windows.net/movements"

    #try:
        #existing_data = ml_client.data.get(name=ds_name)
        #current_version = existing_data.version
        # Increment the version number
        #VERSION = str(int(current_version) + 1)
    #except Exception as e:
        #raise ResourceNotFoundError(f"Data asset {ds_name} not found: {e}")
    
    #print(VERSION)

    my_data = Data(
        path=path,
        type=AssetTypes.URI_FILE,
        description="author: " + author,
        name="movements_dataset_v",
        #version=VERSION,
    )
    
    ml_client.create_or_update(my_data)

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

def upload_files_to_blob(folder_path: str, author: str):
    """
    Uploads all files from a given local folder to a Blob container in Azure Storage.
    Checks if the file already exists before uploading.

    Args:
        folder_path (str): Local folder path containing the files to upload.
    """
    connection_string = get_connection_string(storage_account, resource_group)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "movements"
    container_client = blob_service_client.get_container_client(container_name)

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if os.path.isdir(file_path) or not filename.endswith(".txt"):
            continue
        
        # Create a BlobClient for the specific file in the container
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
            add_tags_to_blob(blob_client)
    
    versionize_data("movements", author)

# List all blobs in the container
#blobs = container_client.list_blobs()

upload_files_to_blob("data", "Thang")
