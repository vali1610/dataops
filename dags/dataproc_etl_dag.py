from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 24),
}

with DAG(
    dag_id='dataproc_etl_direct',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id='submit_spark_job_direct',
        bash_command="""
        gcloud auth activate-service-account --key-file=/home/airflow/gcs/data/sa-dataops.json &&
        gcloud dataproc jobs submit pyspark gs://vale-dataops-bucket/jobs/pyspark_job.py \
          --cluster=vale-dataproc \
          --region=europe-west1 \
          --jars=gs://vale-dataops-bucket/libs/delta-spark_2.12-3.2.0.jar,gs://vale-dataops-bucket/libs/delta-storage-3.0.0.jar,gs://vale-dataops-bucket/libs/hudi-spark3.5-bundle_2.12-1.0.2.jar,gs://vale-dataops-bucket/libs/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
          -- gs://vale-dataops-bucket/data/customer_data_dirty.csv gs://vale-dataops-bucket/data/payment_data_dirty.csv
        """,
    )
