# Databricks notebook source
# MAGIC %md
# MAGIC Parquet 읽기
# MAGIC       ↓
# MAGIC Bronze 메타데이터 생성
# MAGIC       ↓
# MAGIC Bronze Delta 저장 (append)
# MAGIC       ↓
# MAGIC Bronze → Silver 표준화
# MAGIC       ↓
# MAGIC Validation
# MAGIC       ↓
# MAGIC silver_df 생성
# MAGIC       ↓
# MAGIC ETA 관련 컬럼 생성
# MAGIC       ↓
# MAGIC business_key 생성
# MAGIC       ↓
# MAGIC 파생 컬럼 생성
# MAGIC       ↓
# MAGIC Silver Delta 저장 (append)

# COMMAND ----------

'''%sql
-- 테이블 유지, 행만 비우기
TRUNCATE TABLE dt4_project2_team3_databricks.bronze.bronze_berth_schedule;
TRUNCATE TABLE dt4_project2_team3_databricks.silver.silver_berth_schedule;
TRUNCATE TABLE dt4_project2_team3_databricks.silver.silver_error_log;'''

# COMMAND ----------

import os 

staging_dir = "/Workspace/Shared/busan_port_project/01_schedule_pipeline/bronze_staging"
print(os.listdir(staging_dir))

# COMMAND ----------

# DBTITLE 1,민지님
# ==========================
# 이 코드를 노트북 제일 위에 붙여넣고 실행해주시면, bronze_staging 폴더에 있는 14개 parquet 파일이 순서대로 df라는 변수로 하나씩 읽혀요. 그 루프 안에 (제가 표시해둔 주석 부분에) snapshot_time, hash 같은 추가컬럼 붙이는 로직을 넣어주시고, 그 결과를 bronze로 저장해주시면 됩니다.
# ==========================
import json
import pandas as pd

staging_dir = "/Workspace/Shared/busan_port_project/01_schedule_pipeline/bronze_staging"

with open(f"{staging_dir}/manifest.json", "r", encoding="utf-8") as f:
    manifest = json.load(f)

processed_dfs = []  # 추가컬럼까지 붙인 결과를 여기에 쌓음

for entry in manifest["entries"]:
    df = pd.read_parquet(f"{staging_dir}/{entry['parquet_file']}")
    terminal_no = entry["terminal_no"]
    file_name = entry["file"]

    # 여기서 snapshot_time, hash 등 추가컬럼 작업
    # df["snapshot_time"] = ...
    # df["hash"] = ...

    processed_dfs.append(df)

# COMMAND ----------

print(json.dumps(manifest, ensure_ascii=False, indent=2))

# COMMAND ----------

import hashlib
import json
import pandas as pd
from datetime import datetime 


# COMMAND ----------

# DBTITLE 1,snapshot_time
#terminal_1_schedule_20260626_10.xls -> 2026-06-26 10:00
def parse_snapshot_time(file_name):
    base = file_name.rsplit(".", 1)[0]
    parts =base.split("_")
    date_str = parts[-2]
    hour_str = parts[-1]
    return datetime.strptime(f"{date_str} {hour_str}:00", "%Y%m%d %H:%M")

 

# COMMAND ----------

# DBTITLE 1,row_hash
def make_row_hash(row_dict):
    row_str = json.dumps(row_dict, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()

# COMMAND ----------

# DBTITLE 1,file_hash
def make_file_hash(df):
    combined= df.to_json(orient="records", force_ascii=False)
    return hashlib.md5(combined.encode()).hexdigest()
    

# COMMAND ----------

# DBTITLE 1,셀 8
processed_dfs =[]

for entry in manifest["entries"]:
    df = pd.read_parquet(f"{staging_dir}/{entry['parquet_file']}")
    terminal_no = entry["terminal_no"]
    file_name = entry["file"]
    parquet_file = entry["parquet_file"]

    snapshot_time = parse_snapshot_time(file_name)
    snapshot_id = f"{terminal_no}_{snapshot_time.strftime('%Y%m%d_%H')}"
    file_hash = make_file_hash(df)
    downloaded_at = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["terminal_id"] = f"terminal_{terminal_no}"
    df["snapshot_id"] = snapshot_id
    df["source_file_name"] = file_name
    df["source_file_path"] = f"/raw/terminal_{terminal_no}/{file_name}"
    df["source_sheet_name"] = "Sheet1"
    df["source_row_number"] = range(1, len(df) + 1)
    df["data_row_number"] = range(1, len(df) + 1)
    df["raw_row_json"] = df.drop(columns=[
        "terminal_id","snapshot_id","source_file_name","source_file_path",
        "source_sheet_name","source_row_number","data_row_number"
    ]).apply(lambda row: json.dumps(row.to_dict(), ensure_ascii=False, default=str), axis=1)
    df["row_hash"] = df["raw_row_json"].apply(
        lambda s: hashlib.md5(s.encode()).hexdigest()
    )
    df["file_hash"] = file_hash
    df["ingested_at"] = ingested_at
    df["downloaded_at"] = downloaded_at
    df["bronze_row_id"] = [
        f"brz_{terminal_no:02d}_{snapshot_time.strftime('%Y%m%d_%H')}_{i:04d}"
        for i in range(1, len(df) + 1)
    ]

    processed_dfs.append(df)

print(f"총 {len(processed_dfs)}개 파일 처리 완료")
for i, d in enumerate(processed_dfs):
    print(f"  [{i}] {manifest['entries'][i]['file']} → {len(d)}행, {d.shape[1]}열")

# COMMAND ----------

# DBTITLE 1,확인
# 확인
sample= processed_dfs[0]
print(sample[["bronze_row_id", "terminal_id", "snapshot_id", "row_hash", "ingested_at"]].head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC bronze delta 테이블에 적재 

# COMMAND ----------

# DBTITLE 1,bronze 적재
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

combined_df = pd.concat(processed_dfs, ignore_index=True)
print(f"총 {len(combined_df)}행 적재 예정")

spark_df = spark.createDataFrame(combined_df.astype(str)).toDF(*[col.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for col in combined_df.columns])

(spark_df.write.format("delta")
.mode("append")
.saveAsTable("dt4_project2_team3_databricks.bronze.bronze_berth_schedule"))


print("bronze_berth_schedule 적재 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC silver 변환

# COMMAND ----------

# DBTITLE 1,silver 시작.
# 부두별로 첫 2행씩 확인. 
for i, entry in enumerate(manifest["entries"]):
    terminal_no = entry["terminal_no"]
    if i% 2== 0:
        df = processed_dfs[i]
        print(f"\n=== terminal_{terminal_no} ===")
        print(df.columns.tolist())
        print(df.head(2).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC 부두별 컬럼명 통일. 

# COMMAND ----------

import re
import json
import uuid
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, BooleanType

# COMMAND ----------

parsed_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,COLUMN_MAP
# terminal_1은 인코딩 깨진 raw bytes 기준으로 매핑 (기존 노트북과 동일)
COLUMN_MAP = {
    "terminal_1": {
        "col_1": "berth",
        "col_2": "carrier",
        "col_3": "mother_vessel",
        "col_4": "sun_vessel",
        "col_5": "head_bridge_stern",
        "col_6": "vessel_name",
        "col_7": "route",
        "col_8": "closing",
        "col_9": "eta",
        "col_10": "etd",
        "col_11": "discharge",
        "col_12": "loading",
        "col_13": "shift",
        "col_14": "amp",
        "col_15": "status",
    },
    "terminal_2": {
        "col_1":  "_drop",
        "col_2":  "vessel_name",
        "col_3":  "mother_vessel",
        "col_4":  "sun_vessel",
        "col_5":  "carrier",
        "col_6":  "route",
        "col_7":  "head_bridge_stern",
        "col_8":  "eta",
        "col_9":  "etd",
        "col_10": "berth",
        "col_11": "closing",
        "col_12": "discharge",
        "col_13": "loading",
        "col_14": "shift",
        "col_15": "_drop",
        "col_16": "_drop",
        "col_17": "_drop",
    },
    "terminal_3": {
        "No":           "_drop",
        "선석":          "berth",
        "항로":          "route",
        "모선항차":       "mother_vessel",
        "선박명":         "vessel_name",
        "선사항차":       "sun_vessel",
        "접안":          "_drop",
        "선사":          "carrier",
        "반입_시작일시":   "_drop",
        "반입_마감일시":   "closing",
        "입항일시":       "eta",
        "출항일시":       "etd",
        "작업_시작일시":   "_drop",
        "작업_완료일시":   "_drop",
        "양하":          "discharge",
        "선적":          "loading",
        "S/H":          "shift",
        "전배":          "_drop",
    },
    "terminal_4": {
        "col_1":  "berth",
        "col_2":  "carrier",
        "col_3":  "mother_vessel",
        "col_4":  "sun_vessel",
        "col_5":  "vessel_name",
        "col_6":  "route",
        "col_7":  "_drop",
        "col_8":  "eta",
        "col_9":  "etd",
        "col_10": "_drop",
        "col_11": "closing",
        "col_12": "discharge",
        "col_13": "loading",
        "col_14": "shift",
        "col_15": "amp",
        "col_16": "status",
    },
    "terminal_5": {
        "선석":                                        "berth",
        "선사":                                        "carrier",
        "모선항차(선사항차)\nHead (Bridge) Stern":        "_drop",
        "선명__ROUTE_":                               "vessel_name",
        "반입마감시한":                                  "closing",
        "접안_예정_일시":                               "eta",
        "출항_예정_일시":                               "etd",
        "작업량_양하_/_적하_/_Shift":                   "_workload",
        "상태":                                        "status",
    },
    "terminal_6": {
        "plvBerth":     "berth",
        "cdvOperator":  "carrier",
        "plvVslvoy":    "mother_vessel",
        "cdvName":      "vessel_name",
        "plvEvoyout":   "sun_vessel",
        "plvRoute":     "route",
        "plvAtb":       "eta",
        "plvAtd":       "etd",
        "plvDisvan":    "discharge",
        "plvLodvan":    "loading",
        "plvShiftvan":  "shift",
        "plvStatus":    "status",
    },
    "terminal_7": {
        "선석":                    "berth",
        "선사코드":                 "carrier",
        "모선항차(선사항차)":         "mother_vessel",
        "모선명(Route)":            "vessel_name",
        "반입마감시한":               "closing",
        "접안예정일시":               "eta",
        "출항예정일시":               "etd",
        "작업시작시간":               "_drop",
        "작업완료시간":               "_drop",
        "Head (Bridge) Stern":    "head_bridge_stern",
        "작업량_양하/적하/Shift": "_workload",
        "상태":                    "status",
    },
}

# COMMAND ----------

# DBTITLE 1,workload
#1,015 /960 / 12 -> (1015, 960 ,12)
def parse_workload(val: str):
    if not val:
        return None, None, None
    parts = re.split(r"\s*/\s*|／|｜|\|", str(val).strip())
    parts = [p.strip().replace(",", "") for p in parts if p.strip() != ""]
    try:
        discharge = int(parts[0]) if len(parts) > 0 and parts[0].replace(",", "").isdigit() else None
        loading   = int(parts[1]) if len(parts) > 1 and parts[1].replace(",", "").isdigit() else None
        shift     = int(parts[2]) if len(parts) > 2 and parts[2].replace(",", "").isdigit() else None
        return discharge, loading, shift
    except:
        return None, None, None

# COMMAND ----------

# DBTITLE 1,eta_work_date
# eta에서 6시간 뺴서 항만 작업일 계싼 
def calc_eta_work_date(eta_str):
    try:
        eta_dt = pd.to_datetime(eta_str)
        return (eta_dt - pd.Timedelta(hours=6)).date()
    except:
        return None

# COMMAND ----------

# DBTITLE 1,to_int
def to_int(val):
    try: 
        return int(str(val).strip().replace(",",""))
    except:
        return None
    

# COMMAND ----------

# DBTITLE 1,bronze_row_to_silver
def bronze_row_to_silver(row:dict, terminal_id: str) -> dict:
    mapping = COLUMN_MAP.get(terminal_id, {})

    silver = {
        "terminal_name": row.get("terminal_name"),
        "terminal_id" : terminal_id,
        "snapshot_id" : row.get("snapshot_id"),
        "bronze_row_id" : row.get("bronze_row_id"), 
        "source_file_name": row.get("source_file_name"), 
        "source_file_path": row.get("source_file_path"),
        "source_row_number" : row.get("source_row_number"),
        "row_hash": row.get("row_hash"),
        "parsed_at": parsed_at,
        "berth": None,
        "carrier": None,
        "vessel_name": None,
        "mother_vessel": None,
        "sun_vessel": None,
        "head_bridge_stern": None,
        "route": None,
        "eta": None,
        "etd": None,
        "closing": None,
        "discharge": None,
        "loading": None,
        "shift": None,
        "amp": None,
        "status": None,
        "eta_work_date": None,
        "business_key": None, 
        "is_valid": True,
        "error_msg": None, 
    }

    try:
        for orig_col, std_col in mapping.items():
            val = row.get(orig_col, "")
            if terminal_id == "terminal_3":
                print(orig_col, "->", std_col, "=", repr(val))

            if terminal_id == "terminal_5":
                print(f"{orig_col} -> {std_col} : {repr(val)}")

            if val is None or str(val).strip() in ("", "nan", "None"):
                continue

            if std_col == "_drop":
                continue

            elif std_col == "_workload":
                d, l, s = parse_workload(val)
                silver["discharge"] = d
                silver["loading"] = l
                silver["shift"] = s

            else:
                silver[std_col] = str(val).strip()
                if terminal_id == "terminal_3":
                    print("저장:", std_col, "=", silver[std_col])

                if terminal_id == "terminal_5":
                    print(f"저장 → {std_col} = {silver[std_col]}")


        #숫자 타입 변환
        for col in ["discharge", "loading", "shift"]:
            if silver[col] is not None and isinstance(silver[col], str):
                silver[col] = to_int(silver[col])

        errors= [] 

        # 필수값 체크 
        if not silver["berth"] and not silver["vessel_name"]:
            errors.append("선석/선명 모두 없음")
        if not silver["eta"]: 
            errors.append("ETA 없음")
        if silver["etd"]:
            try:
                pd.to_datetime(silver["etd"])
            except:
                errors.append("ETD 날짜 형식 오류")
        if silver["closing"]:
            try:
                pd.to_datetime(silver["closing"])
            except:
                errors.append("Closing 날짜 형식 오류")
        if silver["eta"]: #eta 날짜 형식 검증 
            try: 
                pd.to_datetime(silver["eta"])
            except:
                errors.append("ETA 날짜 형식 오류")
        if silver["eta"] and silver["etd"]:  #eta가 eta보다 빠를 때 
            try:
                eta = pd.to_datetime(silver["eta"])
                etd = pd.to_datetime(silver["etd"])

                if etd < eta: 
                    errors.append("ETD가 ETA보다 빠름")
            except:
                pass
        if silver["closing"] and silver["eta"]:
            try:
                closing = pd.to_datetime(silver["closing"])
                eta = pd.to_datetime(silver["eta"])

                if closing > eta:
                    errors.append("Closing이 ETA보다 늦음")
            except:
                pass
        for col in ["discharge", "loading", "shift"]: #d, l, s 음수일 때 
            if silver[col] is not None and silver[col] < 0:
                errors.append(f"{col} 음수")
        if errors:
            silver["is_valid"] =False
            silver["error_msg"] = ", ".join(errors)

    except Exception as e: 
        silver["is_valid"] = False
        silver["error_msg"] = str(e)

    try: 
        if silver["eta"]:
            eta_work_date = (
                pd.to_datetime(silver["eta"]) - timedelta(hours=6)
            ).strftime("%Y-%m-%d")
            silver["eta_work_date"] = eta_work_date

            vessel_key = silver["mother_vessel"] or silver["vessel_name"]
            silver["business_key"] = (
                f"{silver['terminal_id']}_"
                f"{vessel_key}_"
                f"{eta_work_date}"
            )
    except Exception: 
        pass

    return silver

print("column_map + 변환 함수 정의 완료")

# COMMAND ----------

# bronze delta 테이블에서 읽어서 silver 변환 
df_bronze = spark.read.table("dt4_project2_team3_databricks.bronze.bronze_berth_schedule")
bronze_rows_collected = df_bronze.collect()

# COMMAND ----------

silver_rows = []
error_rows = []

# COMMAND ----------

for row in bronze_rows_collected:
    row_dict = row.asDict()
    terminal_id = row_dict["terminal_id"]
    silver_row = bronze_row_to_silver(row_dict, terminal_id)
    silver_rows.append(silver_row)


    if not silver_row["is_valid"]:
        error_rows.append({
            "error_id": f"err_{uuid.uuid4().hex[:8]}",
            "bronze_row_id": silver_row["bronze_row_id"],
            "terminal_id": silver_row["terminal_id"], 
            "source_file_name": silver_row["source_file_name"],
            "source_row_number": row_dict.get("source_row_number"),
            "error_column":     None,
            "raw_value":        None,
            "error_type":       "PARSE_ERROR",
            "error_message":    silver_row["error_msg"],
            "detected_at":      parsed_at,

        })

valid_cnt = sum(1 for r in silver_rows if r["is_valid"])
invalid_cnt = len(silver_rows) - valid_cnt


print(f"silver 변환 완료")
print(f"전체: {len(silver_rows)}행")
print(f"정상:  {valid_cnt}행")
print(f"오류: {invalid_cnt}행")
print(f"error log: {len(error_rows)}건")

# COMMAND ----------

# DBTITLE 1,오류 유형 분석
import pandas as pd
from collections import Counter

# 오류 메시지 유형별 집계
error_msgs = [r["error_msg"] for r in silver_rows if not r["is_valid"]]
msg_counter = Counter(error_msgs)

print(f"=== 오류 유형별 건수 (총 {len(error_msgs)}건) ===")
for msg, cnt in msg_counter.most_common():
    print(f"  [{cnt:4d}건] {msg}")

# 터미널별 '선석/선명 모두 없음' 건수
print("\n=== 터미널별 '선석/선명 모두 없음' 건수 ===")
no_key_rows = [r for r in silver_rows if r["error_msg"] == "선석/선명 모두 없음"]
terminal_counter = Counter(r["terminal_id"] for r in no_key_rows)
for tid, cnt in sorted(terminal_counter.items()):
    print(f"  {tid}: {cnt}건")

# COMMAND ----------

# DBTITLE 1,silver_schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType

silver_schema = StructType([
    StructField("terminal_id",       StringType(),  True),
    StructField("terminal_name",     StringType(),  True),
    StructField("snapshot_id",       StringType(),  True),
    StructField("bronze_row_id",     StringType(),  True),
    StructField("source_file_name",  StringType(),  True),
    StructField("source_file_path",  StringType(),  True),
    StructField("source_row_number", StringType(),  True),
    StructField("row_hash",          StringType(),  True),
    StructField("parsed_at",         StringType(),  True),
    StructField("eta_work_date",     StringType(),  True),
    StructField("business_key",      StringType(),  True),
    StructField("berth",             StringType(),  True),
    StructField("carrier",           StringType(),  True),
    StructField("vessel_name",       StringType(),  True),
    StructField("mother_vessel",     StringType(),  True),
    StructField("sun_vessel",        StringType(),  True),
    StructField("head_bridge_stern", StringType(),  True),
    StructField("route",             StringType(),  True),
    StructField("eta",               StringType(),  True),
    StructField("etd",               StringType(),  True),
    StructField("closing",           StringType(),  True),
    StructField("discharge",         IntegerType(), True),
    StructField("loading",           IntegerType(), True),
    StructField("shift",             IntegerType(), True),
    StructField("amp",               StringType(),  True),
    StructField("status",            StringType(),  True),
    StructField("is_valid",          BooleanType(), True),
    StructField("error_msg",         StringType(),  True),
])

silver_df= spark.createDataFrame(silver_rows, schema=silver_schema)

print("silver DataFrame 생성 완료")
print(f"행 수: {silver_df.count()}")

# COMMAND ----------

# DBTITLE 1,silver_df 확인
silver_df.printSchema()

silver_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,silver_df
from pyspark.sql.functions import col, concat_ws, date_format, expr, to_timestamp, hour, dayofweek, when, upper, coalesce, lit
from pyspark.sql import functions as F

# 29번 셀과 31번 셀을 하나로 통합
silver_df = (silver_df
    # [기존 29번 로직] 날짜 컬럼 timestamp로 변환
    .withColumn("eta", F.coalesce(
        F.to_timestamp("eta"),
        F.to_timestamp("eta", "yyyy/MM/dd HH:mm"),
        F.to_timestamp("eta", "yyyy-MM-dd HH:mm")
    ))
    .withColumn("etd", F.coalesce(
        F.to_timestamp("etd"),
        F.to_timestamp("etd", "yyyy/MM/dd HH:mm"),
        F.to_timestamp("etd", "yyyy-MM-dd HH:mm")
    ))
    .withColumn("closing", F.coalesce(
        F.to_timestamp("closing"),
        F.to_timestamp("closing", "yyyy/MM/dd HH:mm"),
        F.to_timestamp("closing", "yyyy-MM-dd HH:mm")
    ))
    .withColumn("eta_date", F.to_date("eta")) # eta 날짜
    .withColumn("eta_work_date", F.to_date(F.col("eta_work_date"), "yyyy-MM-dd")) # 오전 6시 기준 작업일시
    .withColumn("business_key", F.concat_ws("_", "terminal_id", "mother_vessel", "sun_vessel", "eta_work_date")) # business key 생성
    
    # [기존 31번 로직 파생 컬럼 생성]
    .withColumn("total_workload", 
        F.coalesce(F.col("discharge"), F.lit(0)) + 
        F.coalesce(F.col("loading"), F.lit(0)) + 
        F.coalesce(F.col("shift"), F.lit(0))
    )
    .withColumn("eta_hour", F.hour("eta"))
    .withColumn("eta_dayofweek", F.dayofweek("eta"))
    .withColumn("is_amp", F.when(F.upper(F.col("amp")) == "Y", 1).otherwise(0))
)

# COMMAND ----------

# DBTITLE 1,날짜 변환 성공 여부 확인. 이거는 안 지우고 기달.
#터미널 날짜 변환 성공 여부 확인
silver_df.groupBy("terminal_id").agg(
    F.sum(F.col("eta").isNull().cast("int")).alias("null_eta"),
    F.sum(F.col("etd").isNull().cast("int")).alias("null_etd"),
    F.sum(F.col("closing").isNull().cast("int")).alias("null_closing")
).orderBy("terminal_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC error_df는 is_valid=False 인 데이터만 저장. 
# MAGIC 근데 현재 validation이 거의  
# MAGIC if not silver["berth"] and not silver["vessel_name"]:
# MAGIC 이 하나뿐이라면 error log가 비어 있을 가능성이 있음. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,error_df
#error_log
error_df = (
    silver_df.filter(F.col("is_valid") == False)
    .select(
        "snapshot_id",
        "terminal_id",
        "bronze_row_id",
        "source_file_name",
        "source_file_path",
        "source_row_number",
        "row_hash",
        "parsed_at",
        "error_msg"
    )
)

print(f"error 건수: {error_df.count()}")
display(error_df)

# COMMAND ----------

# DBTITLE 1,error_df 저장
(
    error_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("dt4_project2_team3_databricks.silver.silver_error_log")
)

print(" error log 저장 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC silver snapshot_id 중복 제거 후 append로 저장. 
# MAGIC

# COMMAND ----------

snapshot_ids = [row.snapshot_id for row in silver_df.select("snapshot_id").distinct().collect()]
snapshot_id_list_sql = ", ".join([f"'{sid}'" for sid in snapshot_ids])   # ← snapshot_ids (복수형)

print(f"이번 실행분 snapshot_id: {snapshot_ids}")


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

silver_table = "dt4_project2_team3_databricks.silver.silver_berth_schedule"

target_table = "dt4_project2_team3_databricks.silver.silver_berth_schedule"

# COMMAND ----------

# MAGIC %md
# MAGIC codex

# COMMAND ----------

current_snapshot_ids = [
    row["snapshot_id"]
    for row in silver_df.select("snapshot_id")
        .where(F.col("snapshot_id").isNotNull())
        .distinct()
        .collect()
]

# COMMAND ----------

print("이번 실행 snapshot_id:", current_snapshot_ids)

# COMMAND ----------

if spark.catalog.tableExists(target_table) and current_snapshot_ids:
    delta_target = DeltaTable.forName(spark, target_table)
    delta_target.delete(F.col("snapshot_id").isin(current_snapshot_ids))
    print(f"기존 중복 snapshot_id 삭제 완료: {len(current_snapshot_ids)}개")
else:
    print("기존 silver 테이블이 없거나 삭제할 snapshot_id가 없습니다.")

(
    silver_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print("silver delta table 저장 완료")
print(f"append 행 수: {silver_df.count()}")