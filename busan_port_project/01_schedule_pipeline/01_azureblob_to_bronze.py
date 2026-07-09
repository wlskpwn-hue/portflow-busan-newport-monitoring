# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Azure blob storage -> pre bronze
# MAGIC Public repository version: replace example values with your own Databricks and storage settings.

# COMMAND ----------

# MAGIC %pip install openpyxl html5lib lxml beautifulsoup4 xlrd fsspec

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import re
import pandas as pd
import json
from io import StringIO

# COMMAND ----------

container = os.getenv("AZURE_STORAGE_CONTAINER", "your-container")
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "yourstorageaccount")
blob_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

files = sorted(dbutils.fs.ls(blob_path), key=lambda x: x.name)
for file_info in files:
    print(file_info.name, file_info.size)

# COMMAND ----------

def copy_blob_file_to_local(blob_file_path):
    workspace_tmp_dir = os.getenv(
        "WORKSPACE_TMP_DIR",
        "/Workspace/Shared/your_project/01_schedule_pipeline/tmp_excel",
    )
    dbutils.fs.mkdirs(workspace_tmp_dir)
    local_path = f"{workspace_tmp_dir}/{os.path.basename(blob_file_path)}"
    dbutils.fs.cp(blob_file_path, f"file:{local_path}")
    return local_path


def extract_terminal_no(file_name: str) -> int:
    matched = re.search(r"terminal_(\d+)_", file_name)
    return int(matched.group(1)) if matched else None


TERMINAL_HEADER_RULES = {
    1: {"mode": "no_header"},
    2: {"mode": "no_header"},
    3: {"mode": "header_row", "header_row": 1},
    4: {"mode": "no_header"},
    5: {"mode": "header_row", "header_row": 0},
    6: {"mode": "xml_custom"},
    7: {"mode": "header_row", "header_row": 0},
}


def read_raw_table(local_path: str) -> pd.DataFrame:
    file_name = os.path.basename(local_path)

    if file_name.endswith(".xlsx"):
        return pd.read_excel(local_path, header=None, engine="openpyxl")
    if file_name.endswith(".xls"):
        with open(local_path, "rb") as file_obj:
            content = file_obj.read()
        content = content.replace(b"udf-8", b"utf-8").replace(b"UDF-8", b"UTF-8")
        try:
            html_text = content.decode("utf-8")
        except UnicodeDecodeError:
            html_text = content.decode("cp949", errors="replace")
        return pd.read_html(StringIO(html_text), header=None)[0]
    if file_name.endswith(".xml"):
        return read_nexacro_xml(local_path)
    raise ValueError(f"Unsupported file extension: {file_name}")


def read_nexacro_xml(local_path: str) -> pd.DataFrame:
    import xml.etree.ElementTree as ET

    tree = ET.parse(local_path)
    root = tree.getroot()

    def clean(tag):
        return tag.split("}")[-1]

    column_order = []
    for elem in root.iter():
        if clean(elem.tag) == "ColumnInfo":
            for col in elem:
                if clean(col.tag) == "Column":
                    col_id = col.attrib.get("id")
                    if col_id:
                        column_order.append(col_id)
            break

    if not column_order:
        raise ValueError("ColumnInfo is missing from the XML dataset.")

    rows = []
    for elem in root.iter():
        if clean(elem.tag) == "Row":
            row = {col_id: None for col_id in column_order}
            for child in elem:
                col_id = child.attrib.get("id")
                if col_id in row:
                    row[col_id] = child.text
            rows.append(row)

    return pd.DataFrame(rows, columns=column_order)


def apply_header_rule(raw_df: pd.DataFrame, terminal_no: int) -> pd.DataFrame:
    rule = TERMINAL_HEADER_RULES.get(terminal_no)
    if rule is None:
        raise ValueError(f"No header rule configured for terminal_{terminal_no}.")

    mode = rule["mode"]
    if mode == "no_header":
        df = raw_df.copy()
        df.columns = [f"col_{index + 1}" for index in range(df.shape[1])]
        return df.reset_index(drop=True)
    if mode == "header_row":
        header_idx = rule["header_row"]
        header = raw_df.iloc[header_idx]
        df = raw_df.iloc[header_idx + 1 :].copy()
        df.columns = header.values
        return df.reset_index(drop=True)
    if mode == "xml_custom":
        return raw_df.reset_index(drop=True)
    raise ValueError(f"Unsupported header mode: {mode}")


def process_one_file(local_path: str) -> dict:
    file_name = os.path.basename(local_path)
    terminal_no = extract_terminal_no(file_name)
    if terminal_no is None:
        raise ValueError(f"Cannot extract terminal number from {file_name}")

    return {
        "file": file_name,
        "terminal_no": terminal_no,
        "df": apply_header_rule(read_raw_table(local_path), terminal_no),
    }


def build_bronze_inputs(files) -> list:
    file_paths = sorted(
        [file_info.path for file_info in files if file_info.name.lower().endswith((".xlsx", ".xls", ".xml"))]
    )
    results = []
    errors = []

    for path in file_paths:
        try:
            results.append(process_one_file(copy_blob_file_to_local(path)))
        except Exception as exc:
            errors.append({"file": os.path.basename(path), "error": str(exc)})

    if errors:
        print(f"[WARN] {len(errors)} files failed during processing:")
        for error in errors:
            print(" -", error)

    return results


BRONZE_STAGING_DIR = os.getenv(
    "BRONZE_STAGING_DIR",
    "/Workspace/Shared/your_project/01_schedule_pipeline/bronze_staging",
)


def save_bronze_inputs_to_staging(bronze_inputs: list, staging_dir: str = BRONZE_STAGING_DIR) -> dict:
    os.makedirs(staging_dir, exist_ok=True)
    manifest_entries = []

    for item in bronze_inputs:
        file_name = item["file"]
        terminal_no = item["terminal_no"]
        df = item["df"].where(pd.notna(item["df"]), None).astype("string")
        parquet_file_name = os.path.splitext(file_name)[0] + ".parquet"
        final_path = f"{staging_dir}/{parquet_file_name}"
        df.to_parquet(final_path, index=False)
        manifest_entries.append(
            {
                "file": file_name,
                "parquet_file": parquet_file_name,
                "terminal_no": terminal_no,
                "rows": item["df"].shape[0],
                "cols": item["df"].shape[1],
                "columns": item["df"].columns.tolist(),
            }
        )

    manifest = {"staging_dir": staging_dir, "file_count": len(manifest_entries), "entries": manifest_entries}
    manifest_path = f"{staging_dir}/manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, ensure_ascii=False, indent=2)

    print(f"Saved {len(manifest_entries)} files to {staging_dir}")
    print(f"manifest.json: {manifest_path}")
    return manifest


bronze_inputs = build_bronze_inputs(files)
manifest = save_bronze_inputs_to_staging(bronze_inputs, staging_dir=BRONZE_STAGING_DIR)

print("Processing complete")
print("file_count:", manifest["file_count"])
