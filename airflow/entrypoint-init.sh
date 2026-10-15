#!/usr/bin/env bash
# One-shot init: migrate the metadata DB, create the API/UI user, seed demo data.
set -euo pipefail
airflow db migrate
airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Beacon --lastname Admin --role Admin --email admin@example.com || true
python /opt/airflow/include/seed_data.py
echo "airflow-init done"
