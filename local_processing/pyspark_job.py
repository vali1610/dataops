import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, to_date

def init_spark():
    spark = SparkSession.builder \
        .appName("Full ETL Job") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    return spark

def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def clean_customer(df):
    df = df.dropDuplicates(['id'])
    df = df.withColumn("fea_2", col("fea_2").cast("double"))
    df = df.filter(col("fea_2").isNotNull() & col("fea_3").isNotNull() & col("fea_4").isNotNull())
    df = df.fillna({"fea_11": df.select(mean(col("fea_11"))).first()[0]})
    df = df.filter(col("fea_4") < 1_000_000)
    for colname in ["fea_7", "fea_8"]:
        df = df.filter(col(colname) != -1)
    return df


def clean_payment(df):
    df = df.dropDuplicates(['id'])

    df = df.withColumn("prod_limit", col("prod_limit").cast("double"))
    df = df.withColumn("new_balance", col("new_balance").cast("double"))
    df = df.withColumn("highest_balance", col("highest_balance").cast("double"))

    df = df.withColumn("update_date", to_date(col("update_date"), "dd/MM/yyyy"))
    df = df.withColumn("report_date", to_date(col("report_date"), "dd/MM/yyyy"))

    df = df.filter(
        col("prod_limit").isNotNull() &
        col("update_date").isNotNull() &
        col("report_date").isNotNull()
    )

    df = df.fillna({"highest_balance": df.select(mean(col("highest_balance"))).first()[0]})
    df = df.filter((col("new_balance") >= 0) & (col("new_balance") < 2_000_000))
    return df



def write_csv_parquet(df, base_name):
    df.write.mode("overwrite").option("header", True).csv(f"converted/csv/{base_name}")
    df.write.mode("overwrite").parquet(f"converted/parquet/{base_name}")

def write_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)

def write_hudi(df, path, table_name):
    path = os.path.abspath(path)  
    df.write.format("hudi") \
        .option("hoodie.table.name", table_name) \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.precombine.field", "id") \
        .option("hoodie.datasource.write.operation", "insert") \
        .mode("overwrite") \
        .save(path)


def write_iceberg(spark, df, db, table):
    spark.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.local.type", "hadoop")
    spark.conf.set("spark.sql.catalog.local.warehouse", "converted/iceberg")
    df.writeTo(f"local.{db}.{table}").using("iceberg").createOrReplace()

def verify_data(spark, fmt, path_or_table, is_table=False):
    print(f"\n🔍 Verifying {fmt.upper()} from: {path_or_table}")
    try:
        if fmt == "iceberg":
            spark.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            spark.conf.set("spark.sql.catalog.local.type", "hadoop")
            spark.conf.set("spark.sql.catalog.local.warehouse", "converted/iceberg")
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

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 spark_etl_job.py <customer_csv_path> <payment_csv_path>")
        sys.exit(1)

    customer_path, payment_path = sys.argv[1], sys.argv[2]

    spark = init_spark()

    # Read input
    customer_df = read_csv(spark, customer_path)
    payment_df = read_csv(spark, payment_path)

    # Clean
    customer_clean = clean_customer(customer_df)
    payment_clean = clean_payment(payment_df)

    # Output dirs
    for folder in ["converted/csv", "converted/parquet", "converted/delta", "converted/hudi", "converted/iceberg"]:
        os.makedirs(folder, exist_ok=True)

    # Write CSV + Parquet
    write_csv_parquet(customer_clean, "customer_clean")
    write_csv_parquet(payment_clean, "payment_clean")

    # Write Delta
    write_delta(customer_clean, "converted/delta/customer_data")
    write_delta(payment_clean, "converted/delta/payment_data")

    # Write Hudi
    write_hudi(customer_clean, "converted/hudi/customer_data", "customer_hudi")
    write_hudi(payment_clean, "converted/hudi/payment_data", "payment_hudi")

    # Write Iceberg
    write_iceberg(spark, customer_clean, "db", "customer_data")
    write_iceberg(spark, payment_clean, "db", "payment_data")

    # Verify outputs
    verify_data(spark, "csv", "converted/csv/customer_clean")
    verify_data(spark, "parquet", "converted/parquet/customer_clean")
    verify_data(spark, "delta", "converted/delta/customer_data")
    verify_data(spark, "hudi", "converted/hudi/customer_data")
    verify_data(spark, "iceberg", "local.db.customer_data", is_table=True)

    print("\nAll ETL steps and checks complete!")

if __name__ == "__main__":
    main()
