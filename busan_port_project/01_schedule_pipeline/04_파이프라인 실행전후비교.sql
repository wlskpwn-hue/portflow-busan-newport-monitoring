-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04_파이프라인 실행작업 (아직 파이프라인 실행 전)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. 실행 전 확인

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [1] 실행 전 row count 백업 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC len([f for f in dbutils.fs.ls("file:/Workspace/Shared/busan_port_project/01_schedule_pipeline/bronze_staging") if f.name.endswith(".parquet")])

-- COMMAND ----------

SELECT COUNT(*) AS bronze_count
FROM dt4_project2_team3_databricks.bronze.bronze_berth_schedule;
-- 원래 0개였습니다.
-- 시뮬레이션 돌릴 때 실행버튼을 잘못눌러서 225로 찍혔습니다.

-- COMMAND ----------

SELECT COUNT(*) AS silver_count
FROM dt4_project2_team3_databricks.silver.silver_berth_schedule;

-- COMMAND ----------

SELECT COUNT(*) AS gold_count
FROM dt4_project2_team3_databricks.gold.gold_integrated_schedule;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [2] 실행 전 snapshot별 개수 확인

-- COMMAND ----------

SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.bronze.bronze_berth_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;

-- COMMAND ----------

SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.silver.silver_berth_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [3] 골드 확인

-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_integrated_schedule;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_schedule_change_history;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_hourly_terminal_workload;


-- COMMAND ----------


SELECT COUNT(*) FROM gold.gold_today_terminal_schedule;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. 실행 후 확인

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [1] 실행 후 row count

-- COMMAND ----------

SELECT COUNT(*) AS bronze_count
FROM dt4_project2_team3_databricks.bronze.bronze_berth_schedule;

-- COMMAND ----------

SELECT COUNT(*) AS silver_count
FROM dt4_project2_team3_databricks.silver.silver_berth_schedule;

-- COMMAND ----------

SELECT COUNT(*) AS gold_count
FROM dt4_project2_team3_databricks.gold.gold_integrated_schedule;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [2] 실행 후 snapshot별 개수 확인

-- COMMAND ----------

--------------------------------
-- 실행 후 bronze --------------
--------------------------------
SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.bronze.bronze_berth_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;

-- COMMAND ----------

--------------------------------
-- 실행 후 silver --------------
--------------------------------
SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.silver.silver_berth_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [3] 골드 확인

-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_integrated_schedule ;

-- COMMAND ----------

SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.gold.gold_integrated_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_schedule_change_history;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_hourly_terminal_workload;


-- COMMAND ----------

SELECT COUNT(*) FROM gold.gold_today_terminal_schedule;

-- COMMAND ----------

SELECT
  snapshot_id,
  terminal_id,
  COUNT(*) AS row_count
FROM dt4_project2_team3_databricks.gold.gold_today_terminal_schedule
GROUP BY snapshot_id, terminal_id
ORDER BY snapshot_id, terminal_id;