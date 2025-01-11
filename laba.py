from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json
from tabulate import tabulate


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'nationalize_api_dag',
    default_args=default_args,
    description='A DAG to fetch data from Nationalize API',
    schedule_interval=timedelta(minutes=1),
)


def fetch_bitcoin_data():
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        eur_data = data['bpi']['EUR']
        table_data = [[key, value] for key, value in eur_data.items()]
        headers = ["Key", "Value"]
        table = tabulate(table_data, headers, tablefmt="fancy_grid")
        print(table)
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")


fetch_data_task = PythonOperator(
    task_id='fetch_nationalize_data',
    python_callable=fetch_bitcoin_data,
    dag=dag,
)