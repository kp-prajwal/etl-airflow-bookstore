FROM apache/airflow:3.0.2

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    requests \
    pandas \
    beautifulsoup4
