# Pinned to your exact chosen version
FROM apache/airflow:2.9.3-python3.11


COPY --chown=airflow:root . /opt/airflow/repo
WORKDIR /opt/airflow/repo

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


