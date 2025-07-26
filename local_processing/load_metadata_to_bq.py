import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

project_id = sys.argv[1]
dataset = sys.argv[2]
table = sys.argv[3]
input_paths = sys.argv[4:]

print(f"Project: {project_id}, Dataset: {dataset}, Table: {table}")
print("Fișiere de procesat:")
for path in input_paths:
    print(f"  - {path}")

spark = SparkSession.builder.appName("LoadMetadataToBigQuery").getOrCreate()

def parse_json_line(line, path):
    try:
        parsed = json.loads(line)
        if isinstance(parsed, dict):
            return [parsed]
        elif isinstance(parsed, list):
            return parsed
        else:
            print(f"Tip necunoscut în fișierul {path}: {type(parsed)}")
            return []
    except Exception as e:
        print(f"Eroare la parsare JSON în {path}: {e}")
        return []

def flatten_record(data, run_date, path):
    record = {
        "run_date": run_date,
        "step": data.get("step"),
        "start_time": data.get("start_time"),
        "end_time": data.get("end_time"),
        "duration_sec": data.get("duration_sec"),
        "customer_records": data.get("records", {}).get("customer") if isinstance(data.get("records"), dict) else None,
        "payment_records": data.get("records", {}).get("payment") if isinstance(data.get("records"), dict) else None,
        "format": data.get("format"),
        "path": data.get("path") or path,
        "is_table": data.get("is_table"),
        "status": data.get("status"),
        "error": data.get("error"),
        "row_count": data.get("row_count")
    }
    return record

all_rows = []
today_str = datetime.utcnow().strftime("%Y-%m-%d")

for path in input_paths:
    print(f"\nProcesare fișier: {path}")
    try:
        df = spark.read.text(path)
        lines = df.collect()
        for row in lines:
            json_records = parse_json_line(row["value"], path)
            for rec in json_records:
                flat = flatten_record(rec, today_str, path)
                all_rows.append(flat)
    except Exception as e:
        print(f"Eroare la citirea fișierului {path}: {e}")

if not all_rows:
    print(" Nu s-au găsit date valide. Ieșire.")
    sys.exit(1)

schema = StructType([
    StructField("run_date", StringType(), True),
    StructField("step", StringType(), True),
    StructField("start_time", FloatType(), True),
    StructField("end_time", FloatType(), True),
    StructField("duration_sec", FloatType(), True),
    StructField("customer_records", IntegerType(), True),
    StructField("payment_records", IntegerType(), True),
    StructField("format", StringType(), True),
    StructField("path", StringType(), True),
    StructField("is_table", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True),
    StructField("row_count", IntegerType(), True),
])

print(f"Total rânduri procesate: {len(all_rows)}")
final_df = spark.createDataFrame(all_rows, schema=schema)

print("Scriere în BigQuery...")
final_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{dataset}.{table}") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()

print("Upload complet cu succes!")

spark.stop()