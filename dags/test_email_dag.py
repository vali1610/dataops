from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from airflow.hooks.base import BaseHook

def notify_slack():
    conn = BaseHook.get_connection("slack_webhook")
    url = conn.host + conn.extra_dejson.get("endpoint")
    message = {
        "text": "✅ Slack alert din DAG Airflow – Vale a reușit! 🚀"
    }
    response = requests.post(url, json=message)
    if response.status_code != 200:
        raise ValueError(f"Slack request failed: {response.status_code}, {response.text}")

with DAG(
    dag_id='test_slack_alerting',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:
    
    alert = PythonOperator(
        task_id='notify_slack',
        python_callable=notify_slack
    )