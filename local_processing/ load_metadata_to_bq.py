import sys
import json
from pyspark.sql import SparkSession
from google.cloud import bigquery

# Argumente CLI
project_id = sys.argv[1]
dataset_id = sys.argv[2]
table_id = sys.argv[3]
input_paths = sys.argv[4:]

print("DEBUG: Script started with arguments:", file=sys.stderr)
print("Project:", project_id, "Dataset:", dataset_id, "Table:", table_id, file=sys.stderr)
print("Input paths:", input_paths, file=sys.stderr)

spark = SparkSession.builder.appName("LoadMetadataToBQ").getOrCreate()

rdd = spark.read.text(input_paths).rdd

def parse_json_row(row):
    try:
        data = json.loads(row)
        print("DEBUG: Parsed row =", data, file=sys.stderr)

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

        elif isinstance(data, list):
            results = []
            for d in data:
                if isinstance(d, dict):
                    results.append({
                        "step": "verify",
                        "start_time": None,
                        "end_time": None,
                        "duration_sec": d.get("duration_sec"),
                        "records": None,
                        "format": d.get("format"),
                        "path": d.get("path"),
                        "status": d.get("status"),
                        "row_count": d.get("row_count")
                    })
            return results
        else:
            print("WARNING: Unexpected JSON type:", type(data), file=sys.stderr)
            return []

    except Exception as e:
        print("ERROR parsing row:", row, "Exception:", e, file=sys.stderr)
        return []

parsed = rdd.flatMap(lambda row: parse_json_row(row["value"]))

df = spark.createDataFrame(parsed)

print("DEBUG: DataFrame schema:", df.printSchema(), file=sys.stderr)
print("DEBUG: Sample data:", df.show(truncate=False), file=sys.stderr)

df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{dataset_id}.{table_id}") \
    .mode("append") \
    .save()

print("✅ Done uploading metadata to BigQuery", file=sys.stderr)