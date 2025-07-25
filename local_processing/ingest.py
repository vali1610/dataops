# Data Ingestion Step

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, to_date
import sys

def init_spark():
    return SparkSession.builder.appName("Ingest").getOrCreate()

if __name__ == "__main__":
    spark = init_spark()

    customer_path = sys.argv[1]
    payment_path = sys.argv[2]
    output_base = sys.argv[3]

    def read_and_clean_customer(path):
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        df = df.dropDuplicates(['id']) \
               .withColumn("fea_2", col("fea_2").cast("double")) \
               .filter(col("fea_2").isNotNull() & col("fea_3").isNotNull() & col("fea_4").isNotNull()) \
               .fillna({"fea_11": df.select(mean("fea_11")).first()[0]}) \
               .filter(col("fea_4") < 1_000_000) \
               .filter((col("fea_7") != -1) & (col("fea_8") != -1))
        return df

    def read_and_clean_payment(path):
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        df = df.dropDuplicates(['id']) \
               .withColumn("prod_limit", col("prod_limit").cast("double")) \
               .withColumn("new_balance", col("new_balance").cast("double")) \
               .withColumn("highest_balance", col("highest_balance").cast("double")) \
               .withColumn("update_date", to_date("update_date", "dd/MM/yyyy")) \
               .withColumn("report_date", to_date("report_date", "dd/MM/yyyy")) \
               .filter(col("prod_limit").isNotNull() &
                      col("update_date").isNotNull() &
                      col("report_date").isNotNull()) \
               .fillna({"highest_balance": df.select(mean("highest_balance")).first()[0]}) \
               .filter((col("new_balance") >= 0) & (col("new_balance") < 2_000_000))
        return df

    read_and_clean_customer(customer_path).write.mode("overwrite").parquet(f"{output_base}/temp/customer_clean")
    read_and_clean_payment(payment_path).write.mode("overwrite").parquet(f"{output_base}/temp/payment_clean")
