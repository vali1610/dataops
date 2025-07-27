from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import requests
from datetime import datetime

def notify_slack():
    # Obține conexiunea definită în Airflow
    conn = BaseHook.get_connection("slack_webhook")
    webhook_url = conn.host + conn.extra_dejson.get("endpoint")
    
    # Mesajul trimis pe Slack
    message = {
        "text": "✅ Test Slack Webhook din DAG Airflow! Totul funcționează! 🚀"
    }

    response = requests.post(webhook_url, json=message)

    if response.status_code != 200:
        raise ValueError(f"Request to Slack failed: {response.status_code}, {response.text}")

with DAG(
    dag_id="slack_notify_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["slack", "notification"]
) as dag:

    notify_slack_task = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack
    )
