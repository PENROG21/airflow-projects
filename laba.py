from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nationalize_api_dag',
    default_args=default_args,
    description='A DAG to fetch data from Nationalize API',
    schedule_interval=timedelta(days=1),
)

def fetch_nationalize_data():
    url = "https://api.nationalize.io/?name=vadim"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data, indent=2))
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

fetch_data_task = PythonOperator(
    task_id='fetch_nationalize_data',
    python_callable=fetch_nationalize_data,
    dag=dag,
)