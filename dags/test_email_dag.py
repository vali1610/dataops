from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_gmail_alert",
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=None,
) as dag:

    send_test_email = EmailOperator(
        task_id="send_test_email",
        to="valentinaetti@yahoo.com",
        subject="Test email from Airflow",
        html_content="SMTP connection is working as expected",
    )