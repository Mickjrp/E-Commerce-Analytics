from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ecom",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecom_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ecom", "etl"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_to_mongo",
        bash_command=(
            "export MONGO_HOST=mongo MONGO_USER=ecom_admin MONGO_PASS=ecom_admin_pass MONGO_DB=ecom "
            "&& export DATA_DIR=/opt/airflow/data "
            "&& python /opt/airflow/scripts/ingest_to_mongo.py"
        ),
    )

    transform = BashOperator(
        task_id="transform_load_postgres",
        bash_command=(
            "export PG_HOST=postgres PG_USER=ecom_user PG_PASS=ecom_pass PG_DB=ecom_dw PG_SCHEMA=ecom "
            "&& export MONGO_HOST=mongo MONGO_USER=ecom_admin MONGO_PASS=ecom_admin_pass MONGO_DB=ecom "
            "&& export DATA_DIR=/opt/airflow/data "
            "&& python /opt/airflow/scripts/transform_load_postgres.py"
        ),
    )

    data_quality = BashOperator(
        task_id="data_quality_checks",
        bash_command=(
            "export PG_HOST=postgres PG_USER=ecom_user PG_PASS=ecom_pass PG_DB=ecom_dw PG_SCHEMA=ecom "
            "&& export DATA_DIR=/opt/airflow/data "
            "&& python /opt/airflow/scripts/data_quality_checks.py"
        ),
    )

    analytics = BashOperator(
        task_id="analytics_rfm",
        bash_command=(
            "export PG_HOST=postgres PG_USER=ecom_user PG_PASS=ecom_pass PG_DB=ecom_dw PG_SCHEMA=ecom "
            "&& export DATA_DIR=/opt/airflow/data "
            "&& python /opt/airflow/scripts/analytics_rfm.py"
        ),
    )

    ingest >> transform >> data_quality >> analytics
