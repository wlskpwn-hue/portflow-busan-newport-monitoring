# Databricks notebook source
# MAGIC %md
# MAGIC # Public-safe permissions template
# MAGIC Replace the example external locations, catalog names, and principals with your own values.

# COMMAND ----------

external_locations = [
    "your_project_raw",
    "your_project_demo_raw",
    "your_project_raw_schedule",
    "your_project_container_operation",
]

principals = [
    "user1@example.com",
    "user2@example.com",
    "service-principal@example.com",
]

catalog_name = "your_catalog"
schemas = ["bronze", "silver", "gold"]

for location_name in external_locations:
    print(f"-- External location: {location_name}")
    for principal in principals:
        print(f"GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `{location_name}` TO `{principal}`;")
        print(f"GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION `{location_name}` TO `{principal}`;")
        print(f"GRANT BROWSE ON EXTERNAL LOCATION `{location_name}` TO `{principal}`;")
        print(f"GRANT READ FILES ON EXTERNAL LOCATION `{location_name}` TO `{principal}`;")
        print(f"GRANT WRITE FILES ON EXTERNAL LOCATION `{location_name}` TO `{principal}`;")

for principal in principals:
    print(f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{principal}`;")
    for schema_name in schemas:
        print(f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{principal}`;")
        print(
            f"GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{principal}`;"
        )
