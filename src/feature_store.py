from logging_helper import logger
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient
from io import StringIO 
import pandas as pd
from dvc import MyAzureClient
from utils import extract_data_from_filename


class AzureFeatureSetUploader(MyAzureClient):

    def __init__(self):
        super().__init__()

    def versionize_data(self):
        ml_client = MLClient(
            DefaultAzureCredential(),
            self.subscription_id,
            self.resource_group,
            self.workspace
        )
        path = "https://featurestore1.blob.core.windows.net/featureset"
        my_data = Data(
            path=path,
            type=AssetTypes.URI_FOLDER,
            name="featurestore"
        )

        ml_client.data.create_or_update(my_data)

    def transform(self, X):
        container_name = "featureset"
        container_client = ContainerClient.from_container_url(self.shared_access_token_feature_store)

        for df, filename in X:
            # Create a BlobClient for the specific file in the container
            blob_client = container_client.get_blob_client(filename)

            # Drop multiple columns
            df.drop(["DeviceName",
                     "time",
                     "Temperature(°C)",
                     "Version()",
                     "Battery level(%)"],
                     axis=1,
                     inplace=True)

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            logger.info(f"Uploading csv file '{filename}' to container '{container_name}'...")
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
            logger.info(f"File '{filename}' uploaded successfully.")

        self.versionize_data()

        # Pass-through
        return X  


class AzureFeatureSetLoader(MyAzureClient):
    def __init__(self) -> None:
        super().__init__()
        self.container_client = ContainerClient.from_container_url(self.shared_access_token_feature_store)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        all_dfs = []
        for blob in self.container_client.list_blobs():
            logger.info(f"Reading blob: {blob.name}")
            # Create a BlobClient for each blob
            blob_client = self.container_client.get_blob_client(blob.name)

            # Download the blob's content
            blob_data = blob_client.download_blob()
            # Assuming the blobs are CSV files
            blob_content = blob_data.content_as_text()
            df = pd.read_csv(StringIO(blob_content), sep=',')
            all_dfs.append([df, extract_data_from_filename(blob.name)])  # Store the DataFrame

        return all_dfs
