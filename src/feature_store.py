from azure.ai.ml import MLClient
from azure.ai.ml.entities import FeatureStore
from azure.ai.ml.entities import FeatureSet, FeatureSetSpec
from azure.identity import DefaultAzureCredential

# Initialize the MLClient
credential = DefaultAzureCredential()
subscription_id = "your_subscription_id"
resource_group = "your_resource_group"
workspace_name = "your_workspace_name"

ml_client = MLClient(credential, subscription_id, resource_group, workspace_name)

# Step 1: Create a Feature Store
feature_store_name = "my_feature_store"

feature_store = FeatureStore(
    name=feature_store_name,
    description="Feature store for reusable ML features",
    offline_storage="azureml:my_blob_storage",  # Offline storage
    online_storage="azureml:my_cosmos_db"       # Online storage for real-time access
)

ml_client.feature_stores.create_or_update(feature_store)
print(f"Feature store '{feature_store_name}' created or updated.")

# Step 2: Define a Feature Set
feature_set_name = "customer_features"
feature_set_spec = FeatureSetSpec(
    description="Customer features for prediction",
    entities=["customer_id"],  # Primary key or entity
    source="datastore://my_datastore/customer_data",
    transformation_script="transformations.py",  # Optional transformation logic
)

feature_set = FeatureSet(
    name=feature_set_name,
    feature_store=feature_store_name,
    spec=feature_set_spec
)

ml_client.feature_sets.create_or_update(feature_set)
print(f"Feature set '{feature_set_name}' created or updated in feature store '{feature_store_name}'.")

# Step 3: Consume Features
feature_set = ml_client.feature_sets.get(name=feature_set_name)
print(f"Retrieved feature set: {feature_set.name}")
