# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC  %pip install great-expectations azure-storage-blob

# COMMAND ----------

import great_expectations as ge
import pandas as pd
import json
from datetime import datetime 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from azure.storage.blob import BlobServiceClient
import io

# COMMAND ----------

# MAGIC %run /Workspace/Users/hrithikbudhiraja@gmail.com/getestingfw/code/configDetails

# COMMAND ----------

# MAGIC %run /Workspace/Users/hrithikbudhiraja@gmail.com/getestingfw/code/utilities
