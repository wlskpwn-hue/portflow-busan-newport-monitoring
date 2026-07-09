# Databricks notebook source
# MAGIC %md
# MAGIC ### [1] azure key vaulte로부터 sql password 가져오기

# COMMAND ----------

# dbutils.secrets.listScopes()

# COMMAND ----------

jdbc_hostname = "4dt-project2-team3-sqlserver.database.windows.net"
jdbc_port = 1433
jdbc_database = "4dt-project2-team3"

jdbc_url = (
    f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};"
    f"database={jdbc_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

sql_user = "dt4team3"
sql_password = dbutils.secrets.get(
    scope="4dt_project2_team3_2",
    key="azureSQL-to-databricks"
)

connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# azure SQL database에 test 테이블 생성 

# from pyspark.sql.functions import current_timestamp

# test_df = spark.createDataFrame(
#     [(1, "Databricks to Azure SQL 연결 성공")],
#     ["id", "message"]
# ).withColumn("created_at", current_timestamp())

# test_df.write.jdbc(
#     url=jdbc_url,
#     table="gold.connection_test",
#     mode="append",
#     properties=connection_properties
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### [2] gold 테이블 연결

# COMMAND ----------

# ============================
# gold1. gold_integrated_schedule 
# ============================
gold_df = spark.table("dt4_project2_team3_databricks.gold.gold_integrated_schedule")

display(gold_df)

# COMMAND ----------

gold_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_integrated_schedule",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# ============================
# gold2. gold_schedule_change_history 
# ============================
gold2_df = spark.table("dt4_project2_team3_databricks.gold.gold_schedule_change_history")

display(gold2_df)

# COMMAND ----------

gold2_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_schedule_change_history",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# ============================
# gold3. gold_hourly_terminal_workload 
# ============================
gold3_df = spark.table("dt4_project2_team3_databricks.gold.gold_hourly_terminal_workload")

display(gold3_df)

# COMMAND ----------

gold3_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_hourly_terminal_workload",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# ============================
# gold4. gold_today_terminal_schedule
# ============================
gold4_df = spark.table("dt4_project2_team3_databricks.gold.gold_today_terminal_schedule")

display(gold4_df)

# COMMAND ----------

gold4_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_today_terminal_schedule",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# ============================
# gold5. gold_explainable_ai_metrics
# # ============================
gold5_df = spark.table("dt4_project2_team3_databricks.gold.gold_explainable_ai_metrics")

display(gold5_df)

# COMMAND ----------

gold5_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_explainable_ai_metrics",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# # ============================
# # gold6. gold_forecast_output 
# # ============================
gold6_df = spark.table("dt4_project2_team3_databricks.gold.gold_forecast_output")

display(gold6_df)

# COMMAND ----------

gold6_df.write.jdbc(
    url=jdbc_url,
    table="gold.gold_forecast_output",
    mode="overwrite",
    properties=connection_properties
)

# COMMAND ----------

# test_df = spark.read.jdbc(
#     url=jdbc_url,
#     table="gold.gold_integrated_schedule",
#     properties=connection_properties
# )

# display(test_df.limit(5))