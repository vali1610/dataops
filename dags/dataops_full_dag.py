from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
import pendulum

DEFAULT_ARGS = {
    "owner": "valentina",
    "start_date": pendulum.datetime(2025, 7, 25, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

PROJECT_ID = "dataops-466411"
REGION = "europe-west1"
CLUSTER = "vale-dataproc"
BUCKET = "vale-dataops-bucket"
JOB_DIR = f"gs://{BUCKET}/jobs"
DATA_DIR = f"gs://{BUCKET}/data"
OUTPUT_BASE = f"gs://{BUCKET}/output"

with DAG(
    dag_id="dataops_pipeline_full",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    description="ETL orchestration with Dataproc and BigQuery",
    tags=["dataops", "spark", "dag"],
) as dag:

    # Task: Ingest Data
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {JOB_DIR}/ingest.py \
        --cluster={CLUSTER} \
        --region={REGION} \
        -- gs://{BUCKET}/data/customer_data_dirty.csv gs://{BUCKET}/data/payment_data_dirty.csv {OUTPUT_BASE}
        """
    )

    # Task: Transform
    transform = BashOperator(
        task_id="transform_data",
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {JOB_DIR}/transform.py \
        --cluster={CLUSTER} \
        --region={REGION} \
        -- {OUTPUT_BASE}/temp {OUTPUT_BASE}
        """
    )

    # Task: Verify
    verify = BashOperator(
        task_id="verify_outputs",
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {JOB_DIR}/verify.py \
        --cluster={CLUSTER} \
        --region={REGION} \
        -- {OUTPUT_BASE}
        """
    )

    # Task: Load to BigQuery
    load_bq = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {JOB_DIR}/load_to_bq.py \
        --cluster={CLUSTER} \
        --region={REGION} \
        -- \
        --project_id={PROJECT_ID} \
        --dataset=dataops_dataset \
        --customer_path={OUTPUT_BASE}/parquet/customer \
        --payment_path={OUTPUT_BASE}/parquet/payment
        """
    )

    ingest >> transform >> verify >> load_bq