# Databricks notebook source
# MAGIC %md
# MAGIC ### Public-safe Azure SQL connection template

# COMMAND ----------

import os

jdbc_hostname = os.getenv("AZURE_SQL_HOST", "your-sql-server.database.windows.net")
jdbc_port = int(os.getenv("AZURE_SQL_PORT", "1433"))
jdbc_database = os.getenv("AZURE_SQL_DATABASE", "your_database")

jdbc_url = (
    f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};"
    f"database={jdbc_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

sql_user = os.getenv("AZURE_SQL_USER", "your_sql_user")
secret_scope = os.getenv("DATABRICKS_SECRET_SCOPE", "your_secret_scope")
secret_key = os.getenv("DATABRICKS_SECRET_KEY", "your_secret_key")
sql_password = dbutils.secrets.get(scope=secret_scope, key=secret_key)

connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table export example

# COMMAND ----------

catalog_name = os.getenv("DATABRICKS_CATALOG", "your_catalog")
gold_schema = os.getenv("DATABRICKS_GOLD_SCHEMA", "gold")

gold_tables = [
    "gold_integrated_schedule",
    "gold_schedule_change_history",
    "gold_hourly_terminal_workload",
    "gold_today_terminal_schedule",
    "gold_explainable_ai_metrics",
    "gold_forecast_output",
]

for table_name in gold_tables:
    source_table = f"{catalog_name}.{gold_schema}.{table_name}"
    target_table = f"{gold_schema}.{table_name}"
    table_df = spark.table(source_table)
    display(table_df)
    table_df.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode="overwrite",
        properties=connection_properties,
    )
