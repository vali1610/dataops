from pyspark.sql import SparkSession
import json
from datetime import datetime
import sys

def parse_json_row(json_str):
    data = json.loads(json_str)
    row = {
        "step": data.get("step"),
        "start_time": data.get("start_time"),
        "end_time": data.get("end_time"),
        "duration_sec": data.get("duration_sec"),
        "customer_records": data.get("records", {}).get("customer"),
        "payment_records": data.get("records", {}).get("payment"),
        "status": data.get("status", "success"),
        "error": data.get("error", None),
        "run_date": datetime.utcnow().date().isoformat()
    }
    return row

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
            parsed = parse_json_row(row["value"])
            all_rows.append(parsed)

    final_df = spark.createDataFrame(all_rows)
    final_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}:{dataset}.{table}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()