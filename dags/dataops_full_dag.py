from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import requests

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
LOG_PATH = f"gs://{BUCKET}/logs/performance_metrics.csv"

def upload_metrics(task_name, **kwargs):
    ts = datetime.utcnow().isoformat()
    duration = kwargs['ti'].xcom_pull(task_ids=task_name)
    status = "warning" if duration and int(duration) > 300 else "success"
    line = f"{ts},{task_name},{duration},{status}\n"
    with open("/tmp/metrics.csv", "a") as f:
        f.write(line)
    os.system(f"gsutil cp /tmp/metrics.csv {LOG_PATH}")

def check_alert():
    os.system(f"gsutil cp {SCRIPT_PATH}/check_alert.sh /tmp/check_alert.sh")
    os.system("chmod +x /tmp/check_alert.sh")
    os.system(f"/tmp/check_alert.sh {BUCKET}")
    with open("/tmp/alert.txt", "r") as f:
        content = f.read().strip()
    return content if content != "NO_ALERT" else None

def notify_slack(**kwargs):
    alert_msg = kwargs['ti'].xcom_pull(task_ids='check_metadata_for_alert')
    conn = BaseHook.get_connection("slack_webhook")
    webhook_url = conn.host + conn.extra_dejson.get("endpoint")

    if alert_msg and alert_msg != "NO_ALERT":
        text = f"🚨 *ALERT from DataOps Pipeline!*\n{alert_msg}"
    else:
        text = "✅ DataOps pipeline ran successfully and no alert was triggered. All good!"

    payload = {"text": text}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise ValueError(f"Slack notification failed: {response.status_code}, {response.text}")

with DAG(
    dag_id='dataproc_pipeline_complex',
    default_args=default_args,
    description='ETL Spark on Dataproc with performance tracking and Slack alerts',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'dataproc', 'monitoring'],
) as dag:

    ingest = BashOperator(
        task_id='ingest',
        bash_command=f"""
        start=$(date +%s)
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/ingest.py \
          --cluster={CLUSTER_NAME} \
          --region={REGION} \
          --jars={JARS} \
          -- {DATA_PATH}/customer_data_dirty.csv {DATA_PATH}/payment_data_dirty.csv {OUTPUT_PATH}
        end=$(date +%s)
        echo $((end-start))
        """,
        do_xcom_push=True,
    )

    log_ingest = PythonOperator(
        task_id='log_ingest',
        python_callable=upload_metrics,
        op_kwargs={'task_name': 'ingest'},
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f"""
        start=$(date +%s)
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/transform.py \
          --cluster={CLUSTER_NAME} \
          --region={REGION} \
          --jars={JARS} \
          -- {OUTPUT_PATH}/temp {OUTPUT_PATH}
        end=$(date +%s)
        echo $((end-start))
        """,
        do_xcom_push=True,
    )

    log_transform = PythonOperator(
        task_id='log_transform',
        python_callable=upload_metrics,
        op_kwargs={'task_name': 'transform'},
    )

    verify = BashOperator(
        task_id='verify',
        bash_command=f"""
        start=$(date +%s)
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/verify.py \
          --cluster={CLUSTER_NAME} \
          --region={REGION} \
          --jars={JARS} \
          -- {OUTPUT_PATH}
        end=$(date +%s)
        echo $((end-start))
        """,
        do_xcom_push=True,
    )

    log_verify = PythonOperator(
        task_id='log_verify',
        python_callable=upload_metrics,
        op_kwargs={'task_name': 'verify'},
    )

    load = BashOperator(
        task_id='load',
        bash_command=f"""
        start=$(date +%s)
        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/load_to_bq.py \
          --cluster={CLUSTER_NAME} \
          --region={REGION} \
          -- \
          --project_id {PROJECT_ID} \
          --dataset dataops_dataset \
          --customer_path {OUTPUT_PATH}/temp/customer_clean \
          --payment_path {OUTPUT_PATH}/temp/payment_clean
        end=$(date +%s)
        echo $((end-start))
        """,
        do_xcom_push=True,
    )

    log_load = PythonOperator(
        task_id='log_load',
        python_callable=upload_metrics,
        op_kwargs={'task_name': 'load'},
    )

    load_metadata = BashOperator(
        task_id='load_metadata',
        bash_command=f"""
        for dir in ingest_metadata.json transform_metadata.json verify_metadata.json load_metadata.json; do
        for file in $(gsutil ls {OUTPUT_PATH}/metadata/$dir/); do
            size=$(gsutil du "$file" | cut -f1)
            if [ "$size" -eq 0 ]; then
                echo "Remove empty file: $file"
                gsutil rm "$file"
            fi
        done
        done

        gcloud dataproc jobs submit pyspark {SCRIPT_PATH}/load_metadata_to_bq.py \
        --cluster={CLUSTER_NAME} \
        --region={REGION} \
        -- \
        {PROJECT_ID} dataops_dataset pipeline_runs \
        {OUTPUT_PATH}/metadata/ingest_metadata.json/* \
        {OUTPUT_PATH}/metadata/transform_metadata.json/* \
        {OUTPUT_PATH}/metadata/verify_metadata.json/* \
        {OUTPUT_PATH}/metadata/load_metadata.json/*
        """,
    )

    check_alert_task = PythonOperator(
        task_id="check_metadata_for_alert",
        python_callable=check_alert,
    )

    notify_slack_task = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack,
        provide_context=True,
        trigger_rule="all_done",
    )

    # DAG FLOW
    ingest >> log_ingest >> transform >> log_transform >> verify >> log_verify >> load >> log_load >> load_metadata
    load_metadata >> check_alert_task >> [notify_slack_task]