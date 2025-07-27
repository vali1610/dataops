from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def notify_slack():
    webhook_url = "https://hooks.slack.com/services/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"  # înlocuiește dacă ai altul
    message = {
        "text": ":rotating_light: *Alertă Airflow:* A apărut o eroare în pipeline."
    }
    response = requests.post(webhook_url, json=message)
    if response.status_code != 200:
        raise ValueError(f"Request to Slack failed with status {response.status_code}: {response.text}")

with DAG(
    dag_id='slack_alert_fallback',
    start_date=datetime(2025, 7, 27),
    schedule_interval=None,
    catchup=False
) as dag:

    alert_task = PythonOperator(
        task_id='send_slack_alert',
        python_callable=notify_slack
    )