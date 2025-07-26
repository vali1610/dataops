from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
import json
from datetime import datetime
import sys

def parse_json_string(json_str):
    """ Încearcă să parseeze fie un dict, fie o listă de dicturi din string. """
    parsed_rows = []
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            parsed_rows.append(obj)
        elif isinstance(obj, list):
            parsed_rows.extend(obj)
        else:
            print(f"⚠️ Obiect JSON invalid: {obj}")
    except json.JSONDecodeError as e:
        print(f"⚠️ Eroare la parsing JSON: {e}\n➡️ Conținut: {json_str}")
    return parsed_rows

def normalize_row(row_dict):
    """ Normalizează dictul pentru BigQuery: convertește în format tabelar unitar. """
    base = {
        "run_date": datetime.utcnow().date().isoformat(),
        "step": row_dict.get("step", row_dict.get("format", "unknown")),
        "start_time": row_dict.get("start_time"),
        "end_time": row_dict.get("end_time"),
        "duration_sec": row_dict.get("duration_sec"),
        "customer_records": None,
        "payment_records": None,
        "format": row_dict.get("format"),
        "path": row_dict.get("path"),
        "is_table": row_dict.get("is_table"),
        "status": row_dict.get("status", "success"),
        "error": row_dict.get("error"),
        "row_count": row_dict.get("row_count")
    }

    records = row_dict.get("records", {})
    if isinstance(records, dict):
        base["customer_records"] = records.get("customer")
        base["payment_records"] = records.get("payment")

    return base

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LoadMetadataToBQ").getOrCreate()

    project_id = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    input_paths = sys.argv[4:]

    all_rows = []

    for path in input_paths:
        print(f"📄 Procesare fișier: {path}")
        df = spark.read.text(path)
        for row in df.collect():
            line = row["value"]
            for parsed_dict in parse_json_string(line):
                normalized = normalize_row(parsed_dict)
                all_rows.append(normalized)

    if not all_rows:
        print("❌ Nu s-au găsit rânduri valide. Ieșire.")
        sys.exit(1)

    final_df = spark.createDataFrame(all_rows)
    print("✅ Scriere în BigQuery...")

    final_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}:{dataset}.{table}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

    print("✅ Upload complet cu succes!")