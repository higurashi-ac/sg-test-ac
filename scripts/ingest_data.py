import json
import os
from datetime import datetime
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/ingest_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def ingest_data():
    logger.info("Starting data ingestion process")
    try:
        input_path = '/opt/airflow/data/input/sample_payments.json'
        logger.info(f"Reading local JSON file from {input_path}")
        with open(input_path, 'r') as f:
            data = json.load(f)
        logger.debug(f"Loaded {len(data)} records from JSON file")

        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = os.getenv('S3_BUCKET', 'softgamesbucket')
        date_str = datetime.now().strftime('%Y-%m-%d')
        s3_key = f'payments/raw/{date_str}/payments.json'

        logger.info(f"Uploading data to s3://{bucket}/{s3_key}")
        s3_hook.load_string(
            string_data=json.dumps(data),
            bucket_name=bucket,
            key=s3_key,
            replace=True,
            #extra_args={'ContentType': 'application/json'}
        )
        logger.info(f"Successfully uploaded data to s3://{bucket}/{s3_key}")

    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}", exc_info=True)
        raise