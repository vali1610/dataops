import argparse
from pyspark.sql import SparkSession

def init_spark():
    return SparkSession.builder \
        .appName("LoadToBigQuery") \
        .getOrCreate()

def load_to_bq(df, table_name, dataset, project_id, write_disposition="WRITE_TRUNCATE"):
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}:{dataset}.{table_name}") \
        .option("writeMethod", "direct") \
        .option("writeDisposition", write_disposition) \
        .save()

def main():
    parser = argparse.ArgumentParser(description="Load Parquet files to BigQuery")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery Dataset name")
    parser.add_argument("--customer_path", required=True, help="GCS path to customer_clean Parquet")
    parser.add_argument("--payment_path", required=True, help="GCS path to payment_clean Parquet")

    args = parser.parse_args()

    spark = init_spark()

    customer_df = spark.read.parquet(args.customer_path)
    payment_df = spark.read.parquet(args.payment_path)

    load_to_bq(customer_df, "customer_clean", args.dataset, args.project_id)
    load_to_bq(payment_df, "payment_clean", args.dataset, args.project_id)

    print("Loaded both customer_clean and payment_clean to BigQuery")

if __name__ == "__main__":
    main()