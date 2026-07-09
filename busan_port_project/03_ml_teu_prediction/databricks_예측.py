# Databricks notebook source
# MAGIC %md
# MAGIC # Public-safe forecasting notebook template
# MAGIC Replace the environment variable defaults with your own storage and catalog configuration.

# COMMAND ----------

import os

container = os.getenv("AZURE_STORAGE_CONTAINER_OPERATION", "your-container-operation")
container2 = os.getenv("AZURE_STORAGE_CONTAINER_RAW", "your-raw-container")
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "yourstorageaccount")

blob_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
blob_path2 = f"abfss://{container2}@{storage_account}.dfs.core.windows.net/"

files = dbutils.fs.ls(blob_path)
files2 = dbutils.fs.ls(blob_path2)

for file_info in files:
    print(file_info.name, file_info.size)

for file_info in files2:
    print(file_info.name, file_info.size)

# COMMAND ----------

catalog_name = os.getenv("DATABRICKS_CATALOG", "your_catalog")
gold_table_name = f"{catalog_name}.gold.gold_forecast_output"

print("Configure the remaining notebook cells with your project-specific logic.")
print("Example gold output table:", gold_table_name)
