from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline

import subprocess
import json
from dotenv import load_dotenv
import os
import re
import pandas as pd

class DataTypeFixer(BaseEstimator, TransformerMixin):
    """ Custom transformer to fix data types in a DataFrame.
    
    This transformer fixes data types in a DataFrame by converting columns to the appropriate data types.
    """
    
    def __init__(self, date_format="%Y-%m-%d"):
        self.date_format = date_format
        
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        # Convert time columns to datetime
        X['time'] = pd.to_datetime(X['time'])
        return X