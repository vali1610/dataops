import argparse
import time
import json
from pyspark.sql import SparkSession

def init_spark():
    return SparkSession.builder \
        .appName("LoadToBigQuery") \
        .getOrCreate()

def load_table(spark, path, table_name, dataset, project_id):
    metadata = {
        "table": table_name,
        "path": path
    }
    start = time.time()
    try:
        df = spark.read.parquet(path)
        row_count = df.count()

        df.write \
            .format("bigquery") \
            .option("table", f"{project_id}:{dataset}.{table_name}") \
            .option("writeMethod", "direct") \
            .option("writeDisposition", "WRITE_TRUNCATE") \
            .save()

        metadata.update({
            "status": "success",
            "row_count": row_count,
            "duration_sec": round(time.time() - start, 2)
        })
    except Exception as e:
        metadata.update({
            "status": "failed",
            "error": str(e),
            "duration_sec": round(time.time() - start, 2)
        })
    return metadata

def main():
    parser = argparse.ArgumentParser(description="Load Parquet files to BigQuery")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery Dataset name")
    parser.add_argument("--customer_path", required=True, help="GCS path to customer_clean Parquet")
    parser.add_argument("--payment_path", required=True, help="GCS path to payment_clean Parquet")
    parser.add_argument("--output_base", required=False, help="GCS output path for metadata", default=None)

    args = parser.parse_args()
    spark = init_spark()

    results = []
    results.append(load_table(spark, args.customer_path, "customer_clean", args.dataset, args.project_id))
    results.append(load_table(spark, args.payment_path, "payment_clean", args.dataset, args.project_id))

    print("BQ Load Summary:")
    for r in results:
        print(json.dumps(r, indent=2))

    if args.output_base:
        metadata_path = f"{args.output_base}/metadata/load_metadata.json"
        (
            spark
            .createDataFrame([json.dumps(results)], "string")
            .write.mode("overwrite")
            .text(metadata_path)
        )

if __name__ == "__main__":
    main()