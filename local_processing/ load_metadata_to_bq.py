import sys
import json
from pyspark.sql import SparkSession

project_id = sys.argv[1]
dataset_id = sys.argv[2]
table_id = sys.argv[3]
input_paths = sys.argv[4:]

print("DEBUG: Script started with args:", sys.argv, file=sys.stderr)

def parse_json_row(row):
    try:
        data = json.loads(row)
        print("DEBUG: Parsed row =", data, file=sys.stderr)

        if isinstance(data, dict):
            return {
                "step": data.get("step"),
                "start_time": data.get("start_time"),
                "end_time": data.get("end_time"),
                "duration_sec": data.get("duration_sec"),
                "records": json.dumps(data.get("records")) if data.get("records") else None
            }
        elif isinstance(data, list):
            print("WARNING: Skipping list-type row", file=sys.stderr)
            return None
        else:
            print("WARNING: Unexpected row type", type(data), file=sys.stderr)
            return None
    except Exception as e:
        print("ERROR parsing row:", row, "Exception:", e, file=sys.stderr)
        return None

spark = SparkSession.builder.appName("LoadMetadataToBQ").getOrCreate()

rdd = spark.read.text(input_paths).rdd
parsed = rdd.map(lambda row: parse_json_row(row["value"])).filter(lambda x: x is not None)

df = spark.createDataFrame(parsed)

df.write.format("bigquery") \
    .option("table", f"{project_id}:{dataset_id}.{table_id}") \
    .mode("append") \
    .save()

print("DEBUG: Load to BigQuery complete", file=sys.stderr)