from pyspark.sql import SparkSession
import sys

def init_spark():
    spark = SparkSession.builder \
        .appName("Transform") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = init_spark()

    input_base = sys.argv[1]  # e.g., gs://.../temp/
    output_base = sys.argv[2]  # e.g., gs://.../output/

    customer_df = spark.read.parquet(f"{input_base}/customer_clean")
    payment_df = spark.read.parquet(f"{input_base}/payment_clean")

    customer_df = customer_df.withColumnRenamed("fea_1", "region_code")
    payment_df = payment_df.withColumnRenamed("prod_limit", "credit_limit")

    # Output to Parquet and CSV
    customer_df.write.mode("overwrite").parquet(f"{output_base}/parquet/customer")
    payment_df.write.mode("overwrite").parquet(f"{output_base}/parquet/payment")

    customer_df.write.mode("overwrite").option("header", True).csv(f"{output_base}/csv/customer")
    payment_df.write.mode("overwrite").option("header", True).csv(f"{output_base}/csv/payment")

    # Delta
    customer_df.write.format("delta").mode("overwrite").save(f"{output_base}/delta/customer")
    payment_df.write.format("delta").mode("overwrite").save(f"{output_base}/delta/payment")

    # Hudi
    customer_df.write.format("hudi") \
        .option("hoodie.table.name", "customer_hudi") \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.precombine.field", "id") \
        .option("hoodie.datasource.write.operation", "insert") \
        .mode("overwrite") \
        .save(f"{output_base}/hudi/customer")

    payment_df.write.format("hudi") \
        .option("hoodie.table.name", "payment_hudi") \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.precombine.field", "id") \
        .option("hoodie.datasource.write.operation", "insert") \
        .mode("overwrite") \
        .save(f"{output_base}/hudi/payment")

    # Iceberg
    spark.conf.set("spark.sql.catalog.gcs", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.gcs.type", "hadoop")
    spark.conf.set("spark.sql.catalog.gcs.warehouse", f"{output_base}/iceberg")

    customer_df.writeTo("gcs.db.customer").using("iceberg").createOrReplace()
    payment_df.writeTo("gcs.db.payment").using("iceberg").createOrReplace()
