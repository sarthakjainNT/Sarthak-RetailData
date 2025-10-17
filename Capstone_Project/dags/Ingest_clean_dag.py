from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta
import os

try:
    email_to = Variable.get("email_to")
except Exception as e:
    raise ValueError(f"Airflow Variable error: {e}")


#this is used for alerting via email about the DAG
def dag_failure_email(context):
    subject = f"OHH! DAG {context['dag'].dag_id} Failed"
    body = f"""
    <h2>Airflow DAG Failure Alert</h2>
    <h3>DAG {context['dag'].dag_id} failed!</h3>
    <p>Task: {context['task_instance'].task_id}</p>
    <p>Execution Date: {context['ts']}</p>
    <p><b>Log URL:</b> <a href="{context['task_instance'].log_url}">View Logs</a></p>
    """
    send_email(to=email_to,subject=subject, html_content=body)

default_args = {
    "owner": "Sarthak",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2025, 10, 8),
    "email_on_failure": True,
    "email_on_retry": False,
    'email': [email_to],
}

# DAG declaration here
@dag(
    dag_id="retail_data_pipelines",
    default_args=default_args,
    description="Retail Data ETL Pipeline with BashOperator, monitoring & alerting",
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    on_failure_callback=dag_failure_email,
    tags=["Our Retail Pipeline"],
) 
#this is the function for the pipeline
def retail_pipeline():
#first task
    ingest_to_bronze_task = BashOperator(
        task_id="ingest_files",
        bash_command=(
        "cd /opt/airflow/src/ingestion && "
        "spark-submit --jars /opt/airflow/jars/mysql-connector-j-9.4.0.jar ingest_to_bronze.py")
    )

#seond task
    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
        "cd /opt/airflow/src/processing && "
        "spark-submit --jars /opt/airflow/jars/mysql-connector-j-9.4.0.jar clean_to_silver.py")
    )
#task dependency by >>
    ingest_to_bronze_task >> bronze_to_silver_task

# Instantiate the DAG
dag = retail_pipeline()