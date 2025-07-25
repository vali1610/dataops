from pyspark.sql import SparkSession
import sys

def init_spark():
    return SparkSession.builder.appName("Verify").getOrCreate()

if __name__ == "__main__":
    spark = init_spark()
    output_base = sys.argv[1]

    def verify(fmt, path_or_table, is_table=False):
        print(f"\nVerifying {fmt.upper()} from: {path_or_table}")
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
                return
            df.printSchema()
            df.show(5)
            print(f"Row count: {df.count()}")
        except Exception as e:
            print(f"Failed to verify {fmt}: {e}")

    #### Testing 
    verify("csv", f"{output_base}/csv/customer")
    verify("parquet", f"{output_base}/parquet/customer")
    verify("delta", f"{output_base}/delta/customer")
    verify("hudi", f"{output_base}/hudi/customer")
    verify("iceberg", "gcs.db.customer", is_table=True)
