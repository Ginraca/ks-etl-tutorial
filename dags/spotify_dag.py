import sys
sys.path.insert(1, "/home/ginraca/Documents/DatEng/ks-etl-tutorial")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum

from spotify_etl_example import spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.yesterday(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="DAG Example Using Spotify API",
    schedule_interval=timedelta(days=1)
)

def helper():
    print("Hello World!")


run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=spotify_etl,
    dag=dag
)

run_etl