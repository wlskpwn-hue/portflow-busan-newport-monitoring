# Databricks notebook source
# MAGIC %md
# MAGIC # 1. 외부위치 권한부여

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------
# MAGIC -- container: raw
# MAGIC -------------------------------
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------
# MAGIC -- container: dmeo-raw
# MAGIC -------------------------------
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_demo-raw`
# MAGIC TO `4dt019@msacademy.msai.kr`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------
# MAGIC -- container: raw-schedule
# MAGIC -------------------------------
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_raw-schedule`
# MAGIC TO `4dt019@msacademy.msai.kr`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------
# MAGIC -- container: container-operation
# MAGIC -------------------------------
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL TABLE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT CREATE EXTERNAL VOLUME
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT BROWSE
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT WRITE FILES
# MAGIC ON EXTERNAL LOCATION `dt4_project2_team3_container-operation`
# MAGIC TO `4dt019@msacademy.msai.kr`;

# COMMAND ----------

# ==========
# 1. 파일 읽기
# ===========

# blob storage 컨테이너 이름(사용할 컨테이너 이름으로 바꾸면 됩니다.)
container = "container-operation" # 여기만 수정하세요!!!! 

# 스토리지 계정(고정)
storage_account = "dt4team3storage"

blob_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

files = dbutils.fs.ls(blob_path)
for f in files:
    print(f.name, f.size)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. 카탈로그.스키마 권한 부여

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG `dt4_project2_team3_databricks`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt004@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt004@msacademy.msai.kr`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG `dt4_project2_team3_databricks`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt009@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt009@msacademy.msai.kr`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG `dt4_project2_team3_databricks`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt019@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt019@msacademy.msai.kr`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG `dt4_project2_team3_databricks`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`bronze`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`silver`
# MAGIC TO `4dt035@msacademy.msai.kr`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE
# MAGIC ON SCHEMA `dt4_project2_team3_databricks`.`gold`
# MAGIC TO `4dt035@msacademy.msai.kr`;