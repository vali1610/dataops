from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID = "dataops-466411"
REGION = "europe-west1"
CLUSTER_NAME = "vale-dataproc"
BUCKET = "vale-dataops-bucket"

PYSPARK_URI = f"gs://{BUCKET}/jobs/pyspark_job.py"
CSV1 = f"gs://{BUCKET}/data/customer_data_dirty.csv"
CSV2 = f"gs://{BUCKET}/data/payment_data_dirty.csv"

JARS = [
    f"gs://{BUCKET}/libs/delta-spark_2.12-3.2.0.jar",
    f"gs://{BUCKET}/libs/delta-storage-3.0.0.jar",
    f"gs://{BUCKET}/libs/hudi-spark3.5-bundle_2.12-1.0.2.jar",
    f"gs://{BUCKET}/libs/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with models.DAG(
    dag_id="dataproc_etl_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "spark", "etl"],
) as dag:

    submit_spark_etl_job = DataprocSubmitJobOperator(
        task_id="submit_spark_etl_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "reference": {"job_id": "spark_job_{{ ts_nodash }}"},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [CSV1, CSV2],
            },
            "jar_file_uris": JARS
        }
    )
