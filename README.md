# Payment ETL Project

This project implements an ETL pipeline for processing payment transaction data, storing raw JSON in an S3 bucket, transforming it to Parquet using PySpark, and loading it into PostgreSQL for analysis. The pipeline is orchestrated with Apache Airflow, running in Docker for easy setup. Comprehensive logging is included for debugging.

## Prerequisites
- Docker and Docker Compose installed
- AWS CLI configured with credentials (for S3 access)
- Python 3.9+
- Access to a PostgreSQL database (provided via Docker)

## Project Structure
see Project_tree.md