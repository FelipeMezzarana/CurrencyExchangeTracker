"""Please note that this file is just a simplified demonstration.
It does not follow best practices.

The most recommended approach would be not to use Airflow for processing (as in this example),
just to orchestrate the execution.
A better option would be, for example, to use the docker image to run the pipeline in a servless service,
such as AWS lambda or in a container orchestration service such as AWS ECS or Kubernetes.

This is recommended because otherwise we would have to scale Airflow
according to the job that demands the most from it, which would be extremely inefficient.
"""

# Standard library
from datetime import datetime, timedelta

# First party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

# Local
from ..modules import create_report, update_currency_exchange

default_args = {"owner": "Felipe", "start_date": datetime(2023, 1, 1)}

with DAG(
    "currency_exchange_etl",
    default_args=default_args,
    start_date=datetime(2022, 1, 30),
    schedule_interval="0 12 * * 1",  # At 12:00 every Monday
    catchup=False,
) as dag:
    # Run Unit Tests and save log file
    quality_connection_tests = BashOperator(
        task_id="quality_connection_tests",
        bash_command=r"python3 test_api_input.py",
        execution_timeout=timedelta(minutes=20),
    )

    # run ETL pipeline
    run_etl_pipeline = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=update_currency_exchange.etl_pipeline,
        execution_timeout=timedelta(minutes=20),
    )
    # Generate Excel report
    excel_report = PythonOperator(
        task_id="excel_report",
        python_callable=create_report.report_pipeline,
        execution_timeout=timedelta(minutes=20),
    )

    quality_connection_tests >> run_etl_pipeline >> excel_report
