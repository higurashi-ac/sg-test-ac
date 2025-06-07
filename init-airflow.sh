#!/bin/bash
#init-airflow.sh
set -e

airflow db init
airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com

airflow connections delete aws_default || true
airflow connections add aws_default \
  --conn-type aws \
  --conn-login "${AWS_ACCESS_KEY_ID}" \
  --conn-password "${AWS_SECRET_ACCESS_KEY}" \
  --conn-extra '{"region_name": "eu-north-1"}'

exec airflow webserver