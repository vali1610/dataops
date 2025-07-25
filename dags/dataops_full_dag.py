from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import os

default_args = {
    'owner': 'valentina',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_ID = "dataops-466411"
REGION = "europe-west1"
CLUSTER_NAME = "vale-dataproc"
BUCKET = "vale-dataops-bucket"
JARS = ",".join([
    f"gs://{BUCKET}/libs/delta-spark_2.12-3.2.0.jar",
    f"gs://{BUCKET}/libs/delta-storage-3.0.0.jar",
    f"gs://{BUCKET}/libs/hudi-spark3.5-bundle_2.12-1.0.2.jar",
    f"gs://{BUCKET}/libs/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
])

DATA_PATH = f"gs://{BUCKET}/data"
OUTPUT_PATH = f"gs://{BUCKET}/output"
SCRIPT_PATH = f"gs://{BUCKET}/jobs"

with DAG(
    dag_id='dataproc_pipeline_complex',
    default_args=default_args,
    description='ETL Spark on Dataproc with full pipeline',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'dataproc', 'etl'],
) as dag:

    ingest_task = BashOperator(
        task_id='spark_etl_ingest_task',
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/ingest.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        --jars={JARS} \
        -- \
        {DATA_PATH}/customer_data_dirty.csv \
        {DATA_PATH}/payment_data_dirty.csv \
        {OUTPUT_PATH}
        """
    )

    transform_task = BashOperator(
        task_id='spark_etl_transform_task',
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/transform.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        --jars={JARS} \
        -- \
        {OUTPUT_PATH}/temp \
        {OUTPUT_PATH}
        """
    )

    verify_task = BashOperator(
        task_id='spark_etl_verify_data',
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/verify.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        --jars={JARS} \
        -- \
        {OUTPUT_PATH}
        """
    )

    load_task = BashOperator(
        task_id='spark_etl_load_to_bq',
        bash_command=f"""
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/load_to_bq.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        -- \
        --project_id {PROJECT_ID} \
        --dataset dataops_dataset \
        --customer_path {OUTPUT_PATH}/temp/customer_clean \
        --payment_path {OUTPUT_PATH}/temp/payment_clean
        """
    )
    send_success_email = EmailOperator(
            task_id='send_success_email',
            to='fercal.petru@gmail.com',
            subject='✅ DataOps DAG Successful',
            html_content='The DAG <b>dataops_pipeline_full</b> ran successfully.',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='fercal.petru@gmail.com',
        subject='❌ DataOps DAG Failed',
        html_content='The DAG <b>dataops_pipeline_full</b> failed. Please investigate the logs.',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    ingest_task >> transform_task >> verify_task >> load_task
    [ingest_task, transform_task, verify_task, load_task] >> send_success_email
    [ingest_task, transform_task, verify_task, load_task] >> send_failure_email