from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "valentina",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["valentinaetti@yahoo.com"],
}

dag = DAG(
    dag_id="dataops_pipeline_full",
    default_args=default_args,
    description="Full pipeline: ingest → transform → verify → load → notify",
    schedule_interval="0 8 * * *",  # daily at 08:00
    start_date=datetime(2025, 7, 25),
    catchup=False,
    tags=["spark", "bq", "gcs", "airflow"],
)

BUCKET = "gs://vale-dataops-bucket"
INPUT_CUSTOMER = f"{BUCKET}/data/customer_data_dirty.csv"
INPUT_PAYMENT = f"{BUCKET}/data/payment_data_dirty.csv"
TEMP_OUTPUT = f"{BUCKET}/temp"
FINAL_OUTPUT = f"{BUCKET}/output"
PROJECT_ID = "dataops-466411"
BQ_DATASET = "valentina_dataset"

with dag:

    def push_paths_to_xcom(ti):
        ti.xcom_push(key="customer_input", value=INPUT_CUSTOMER)
        ti.xcom_push(key="payment_input", value=INPUT_PAYMENT)
        ti.xcom_push(key="temp_output", value=TEMP_OUTPUT)
        ti.xcom_push(key="final_output", value=FINAL_OUTPUT)

    prepare = PythonOperator(
        task_id="prepare_xcom_values",
        python_callable=push_paths_to_xcom,
    )

    with TaskGroup("spark_etl") as spark_etl:
        ingest = BashOperator(
            task_id="ingest_data",
            bash_command=(
                f"spark-submit {BUCKET}/jobs/ingest.py "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='customer_input') }} "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='payment_input') }} "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='temp_output') }}"
            ),
        )

        transform = BashOperator(
            task_id="transform_data",
            bash_command=(
                f"spark-submit {BUCKET}/jobs/transform.py "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='temp_output') }} "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='final_output') }}"
            ),
        )

        verify = BashOperator(
            task_id="verify_outputs",
            bash_command=(
                f"spark-submit {BUCKET}/jobs/verify.py "
                "{{ ti.xcom_pull(task_ids='prepare_xcom_values', key='final_output') }}"
            ),
        )

        ingest >> transform >> verify

    load_to_bq = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            f"spark-submit {BUCKET}/jobs/load_to_bq.py "
            "--project_id {{ params.project }} "
            "--dataset {{ params.dataset }} "
            "--customer_path {{ ti.xcom_pull(task_ids='prepare_xcom_values', key='final_output') }}/parquet/customer "
            "--payment_path {{ ti.xcom_pull(task_ids='prepare_xcom_values', key='final_output') }}/parquet/payment"
        ),
        params={"project": PROJECT_ID, "dataset": BQ_DATASET},
    )

    notify_done = EmailOperator(
        task_id="notify_success",
        to={email},
        subject="Airflow Pipeline Executed Successfully",
        html_content="All steps (Ingest, Transform, Verify, Load) completed successfully.",
    )

    prepare >> spark_etl >> load_to_bq >> notify_done