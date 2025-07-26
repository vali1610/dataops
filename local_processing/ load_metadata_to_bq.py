from pyspark.sql import SparkSession
import json
from datetime import datetime
import sys

def normalize_data(data):
    """
    Primește fie un dict (single metadata), fie o listă de dict (multi-metadata).
    Returnează o listă de rânduri normalizate.
    """
    if isinstance(data, dict):  
        return [parse_json_row(data)]
    elif isinstance(data, list):  
        return [parse_json_row(obj) for obj in data]
    else:
        return []

def parse_json_row(data):
    """
    Normalizează obiectul JSON într-un rând tabular.
    """
    return {
        "step": data.get("step", data.get("format", "unknown")),  
        "start_time": data.get("start_time", None),
        "end_time": data.get("end_time", None),
        "duration_sec": data.get("duration_sec"),
        "customer_records": data.get("records", {}).get("customer") if isinstance(data.get("records"), dict) else data.get("row_count"),
        "payment_records": data.get("records", {}).get("payment") if isinstance(data.get("records"), dict) else None,
        "status": data.get("status", "success"),
        "error": data.get("error", None),
        "run_date": datetime.utcnow().date().isoformat()
    }

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LoadMetadata").getOrCreate()
    project_id = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    gcs_paths = sys.argv[4:]

    all_rows = []

    for path in gcs_paths:
        json_df = spark.read.text(path)
        for row in json_df.collect():
            try:
                parsed_json = json.loads(row["value"])
                all_rows.extend(normalize_data(parsed_json))
            except Exception as e:
                print(f"Failed to parse row from {path}: {e}")
                continue

    final_df = spark.createDataFrame(all_rows)
    final_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}:{dataset}.{table}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()