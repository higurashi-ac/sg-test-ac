import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingest_data import ingest_data
from transform_to_parquet import transform_to_parquet
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/payment_etl_dag.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='payment_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for payment data',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 6),
    catchup=False,
    tags=['etl', 'payment'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
    )

    transform_task = PythonOperator(
        task_id='transform_to_parquet',
        python_callable=transform_to_parquet,
    )

    ingest_task >> transform_task