from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import FeatureStore

ml_client = MLClient(
    DefaultAzureCredential(), 
    subscription_id=AZURE_SUBSCRIPTION_ID,
    resource_group_name=AZURE_RESSOURCE_GROUP,
    workspace_name=AZURE_WORKSPACE
)

featurestore = FeatureStore(
    name="Feature_store_group02",
    location="westeurope",
    description="Feature Store Gruppe 02"
)

ml_client.feature_stores.begin_create(featurestore)

