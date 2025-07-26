import sys
import json
from pyspark.sql import SparkSession
from google.cloud import bigquery

project_id = sys.argv[1]
dataset_id = sys.argv[2]
table_id = sys.argv[3]
input_paths = sys.argv[4:]

print("=== DEBUG: Script started ===", file=sys.stderr)
print(f"Project: {project_id}, Dataset: {dataset_id}, Table: {table_id}", file=sys.stderr)
print("Input paths:", input_paths, file=sys.stderr)

spark = SparkSession.builder.appName("LoadMetadataToBQ").getOrCreate()

# Citim fișierele ca text în DataFrame, apoi transformăm în RDD de stringuri
df_raw = spark.read.text(input_paths)
rdd = df_raw.rdd.map(lambda row: row.value)

def parse_json_row(row):
    try:
        data = json.loads(row)
        print("DEBUG: Parsed:", data, file=sys.stderr)

        # Dacă e dict => transform sau ingest
        if isinstance(data, dict):
            return [{
                "step": data.get("step"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "duration_sec": data.get("duration_sec"),
                "records": json.dumps(data.get("records")) if data.get("records") else None,
                "format": None,
                "path": None,
                "status": data.get("status"),
                "row_count": None
            }]

        # Dacă e listă => verify
        elif isinstance(data, list):
            return [{
                "step": "verify",
                "start_time": None,
                "end_time": None,
                "duration_sec": entry.get("duration_sec"),
                "records": None,
                "format": entry.get("format"),
                "path": entry.get("path"),
                "status": entry.get("status"),
                "row_count": entry.get("row_count")
            } for entry in data if isinstance(entry, dict)]

        else:
            print(f"WARN: Unhandled JSON type: {type(data)}", file=sys.stderr)
            return []

    except Exception as e:
        print(f"ERROR: Failed to parse row: {row} | Exception: {e}", file=sys.stderr)
        return []

parsed_rdd = rdd.flatMap(parse_json_row)

# Transformăm în DataFrame
parsed_df = spark.createDataFrame(parsed_rdd)

print("=== DEBUG: Final schema ===", file=sys.stderr)
parsed_df.printSchema()
parsed_df.show(truncate=False)

# Scriem în BigQuery
parsed_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{dataset_id}.{table_id}") \
    .mode("append") \
    .save()

print("Upload complete!", file=sys.stderr)