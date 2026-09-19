FROM apache/airflow:3.3.2-python3.14

COPY requirements/ /requirements/
RUN pip install --no-cache-dir -r /requirements/airflow.txt

USER root
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data
USER airflow
