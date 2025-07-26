from pyspark.sql import SparkSession
import sys
import time
import json

def init_spark():
    return SparkSession.builder \
        .appName("Transform") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

if __name__ == "__main__":
    spark = init_spark()
    output_base = sys.argv[1]

    results = []

    def verify(fmt, path_or_table, is_table=False):
        print(f"\nVerifying {fmt.upper()} from: {path_or_table}")
        metadata = {"format": fmt, "path": path_or_table, "is_table": is_table}
        start = time.time()

        try:
            if fmt == "iceberg":
                spark.conf.set("spark.sql.catalog.gcs", "org.apache.iceberg.spark.SparkCatalog")
                spark.conf.set("spark.sql.catalog.gcs.type", "hadoop")
                spark.conf.set("spark.sql.catalog.gcs.warehouse", f"{output_base}/iceberg")
                df = spark.read.table(path_or_table)
            elif fmt == "hudi":
                df = spark.read.format("hudi").load(path_or_table)
            elif fmt == "delta":
                df = spark.read.format("delta").load(path_or_table)
            elif fmt == "csv":
                df = spark.read.option("header", True).csv(path_or_table)
            elif fmt == "parquet":
                df = spark.read.parquet(path_or_table)
            else:
                print(f"Unknown format: {fmt}")
                metadata["status"] = "failed"
                metadata["error"] = "Unknown format"
                results.append(metadata)
                return

            df.printSchema()
            df.show(5)

            row_count = df.count()
            duration = round(time.time() - start, 2)

            print(f"Row count: {row_count}")
            metadata.update({
                "status": "success",
                "row_count": row_count,
                "duration_sec": duration
            })

        except Exception as e:
            duration = round(time.time() - start, 2)
            print(f"Failed to verify {fmt}: {e}")
            metadata.update({
                "status": "failed",
                "error": str(e),
                "duration_sec": duration
            })

        results.append(metadata)

    verify("csv", f"{output_base}/csv/customer")
    verify("parquet", f"{output_base}/parquet/customer")
    verify("delta", f"{output_base}/delta/customer")
    verify("hudi", f"{output_base}/hudi/customer")
    verify("iceberg", "gcs.db.customer", is_table=True)

    metadata_path = f"{output_base}/metadata/verify_metadata.json"
    (
        spark
        .createDataFrame([json.dumps(results)], "string")
        .write.mode("overwrite")
        .text(metadata_path)
    )