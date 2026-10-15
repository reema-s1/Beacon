FROM apache/airflow:2.10.5-python3.11
ENV PYTHONPATH=/opt/airflow/include
COPY --chown=airflow:root include/ /opt/airflow/include/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root entrypoint-init.sh /opt/airflow/entrypoint-init.sh
