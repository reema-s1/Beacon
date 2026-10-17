FROM apache/airflow:2.10.5-python3.11
ENV PYTHONPATH=/opt/airflow/include
USER root
# Pre-create mount points so named volumes inherit airflow ownership.
RUN mkdir -p /opt/airflow/data /opt/airflow/reports && chown -R airflow:root /opt/airflow/data /opt/airflow/reports
USER airflow
COPY --chown=airflow:root include/ /opt/airflow/include/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root entrypoint-init.sh /opt/airflow/entrypoint-init.sh
