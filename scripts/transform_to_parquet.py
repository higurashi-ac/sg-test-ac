from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import shutil
import logging

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/opt/airflow/logs/transform_to_parquet.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def transform_to_parquet():
    logger.info("Starting transform to Parquet process")

    try:
        # Spark configuration
        conf = SparkConf()
        conf.set("spark.driver.host", "127.0.0.1")
        conf.set("spark.driver.bindAddress", "0.0.0.0")

        spark = SparkSession.builder \
            .config(conf=conf) \
            .appName("PaymentETL") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails") \
            .config("spark.eventLog.enabled", "false") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .master("local[*]") \
            .getOrCreate()

        logger.debug(f"Spark version: {spark.version}")
        logger.debug(f"JAVA_HOME: {os.getenv('JAVA_HOME')}")
        logger.debug(f"SPARK_HOME: {os.getenv('SPARK_HOME')}")

        # Schema definition
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("payment_date", DateType(), True),
            StructField("game", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("status", StringType(), True)
        ])

        # S3 download configuration
        s3 = S3Hook(aws_conn_id="aws_default")
        bucket = os.getenv("S3_BUCKET", "softgamesbucket")
        date_str = datetime.now().strftime("%Y-%m-%d")
        s3_key = f"payments/raw/{date_str}/payments.json"
        download_dir = "/opt/airflow/data/output"
        os.makedirs(download_dir, exist_ok=True)

        logger.info(f"Downloading JSON from s3://{bucket}/{s3_key}")
        temp_file_path = s3.download_file(key=s3_key, bucket_name=bucket, local_path=download_dir)

        final_local_path = f"{download_dir}/temp_payments_{date_str}.json"
        shutil.move(temp_file_path, final_local_path)

        logger.info(f"Reading JSON into DataFrame from {final_local_path}")
        df = spark.read.schema(schema).json(final_local_path)
        row_count = df.count()
        logger.debug(f"Loaded DataFrame with {row_count} rows")

        parquet_path = f"/opt/airflow/data/output/parquet/payments_{date_str}"
        logger.info(f"Writing DataFrame to Parquet at {parquet_path}")
        df.write.parquet(parquet_path, mode="overwrite")

        logger.info("Writing data to PostgreSQL table payment_transactions")
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "payment_transactions") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        spark.stop()
        logger.info("Spark session stopped successfully")

    except Exception as err:
        logger.error(f"Error during transformation: {err}", exc_info=True)
        if "spark" in locals():
            spark.stop()
        raise
