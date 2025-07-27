from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago
import json

def check_metadata():
    # Exemplu simplu: raise alertă dacă metadata e suspectă
    from airflow.exceptions import AirflowSkipException
    # dacă totul e ok:
    # raise AirflowSkipException("No issue detected")
    # dacă vrei să trimiți alerta:
    return 'send_slack_alert'

default_args = {
    'owner': 'valentina',
    'retries': 1,
}

with DAG(
    dag_id='dataops_pipeline_with_slack',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['dataops'],
) as dag:

    load_metadata = BashOperator(
        task_id='load_metadata',
        bash_command='echo "loading metadata..."'
    )

    check_metadata_for_alert = PythonOperator(
        task_id='check_metadata_for_alert',
        python_callable=check_metadata
    )

    send_slack_alert = SimpleHttpOperator(
        task_id='send_slack_alert',
        http_conn_id='slack_webhook',
        method='POST',
        data=json.dumps({
            'text': ':rotating_light: Metadata anomaly detected in DAG *{{ dag.dag_id }}*, task *{{ task_instance.task_id }}*. Please check!'
        }),
        headers={"Content-Type": "application/json"},
    )

    # restul pașilor tăi (exemplu)
    load = BashOperator(
        task_id='load',
        bash_command='echo "loading data..."'
    )

    # chain
    load_metadata >> check_metadata_for_alert >> send_slack_alert >> load