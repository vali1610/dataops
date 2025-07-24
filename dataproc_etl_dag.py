from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

CUSTOMER_PATH = "gs://vale-dataops-bucket/data/customer_data_dirty.csv"
PAYMENT_PATH = "gs://vale-dataops-bucket/data/payment_data_dirty.csv"

PYSPARK_JOB = {
    "reference": {"project_id": "dataops-466411"},
    "placement": {"cluster_name": "vale-dataproc"},
    "pyspark_job": {
        "main_python_file_uri": "gs://vale-dataops-bucket/jobs/pyspark_job.py",
        "args": [CUSTOMER_PATH, PAYMENT_PATH],
        "jar_file_uris": [
            "gs://vale-dataops-bucket/libs/delta-spark_2.12-3.2.0.jar",
            "gs://vale-dataops-bucket/libs/delta-storage-3.0.0.jar",
            "gs://vale-dataops-bucket/libs/hudi-spark3.5-bundle_2.12-1.0.2.jar",
            "gs://vale-dataops-bucket/libs/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
        ],
        "properties": {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        },
    },
}

with models.DAG(
    "dataproc_etl_pipeline",
    schedule_interval=None,  # only manual run to cut costs 
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc", "etl", "spark"],
) as dag:

    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="submit_spark_etl_job",
        job=PYSPARK_JOB,
        region="europe-west1",
        project_id="dataops-466411",
    )

    run_dataproc_job
