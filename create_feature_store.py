from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import FeatureStore

ml_client = MLClient(
    DefaultAzureCredential(), 
    subscription_id="e3bfc907-4e07-467d-a81a-2de709c8c86f",
    resource_group_name="ws24_mlops_group02",
    workspace_name="mlopsgroup02"
)

featurestore = FeatureStore(
    name="Feature_store_group02",
    location="westeurope",
    description="Feature Store Gruppe 02"
)

ml_client.feature_stores.begin_create(featurestore)

