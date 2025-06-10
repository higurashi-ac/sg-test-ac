import json
import os
from datetime import datetime
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests  # Added for making HTTP requests to the Payment Provider API

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
        # Option to fetch data from API instead of local file
        use_api = os.getenv('USE_PAYMENT_API', 'false').lower() == 'true'
        if use_api:
            # Define API endpoint and date range for the report
            api_url = 'https://payment-provider.com/reports'
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Example: previous day
            end_date = datetime.now().strftime('%Y-%m-%d')  # Example: current day
            params = {'start_date': start_date, 'end_date': end_date}
            
            logger.info(f"Fetching data from Payment Provider API: {api_url} with params {params}")
            try:
                # Make API request (assuming authentication via headers, e.g., API key)
                headers = {'Authorization': f"Bearer {os.getenv('PAYMENT_API_KEY')}"}
                response = requests.get(api_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()  # Raise exception for bad status codes
                
                # Parse API response
                data = response.json()
                logger.debug(f"Fetched {len(data)} records from Payment Provider API")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data from API: {str(e)}", exc_info=True)
                raise
        else:
            # Existing local file reading logic
            input_path = '/opt/airflow/data/input/sample_payments.json'
            logger.info(f"Reading local JSON file from {input_path}")
            with open(input_path, 'r') as f:
                data = json.load(f)
            logger.debug(f"Loaded {len(data)} records from JSON file")

        # Existing S3 upload logic
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