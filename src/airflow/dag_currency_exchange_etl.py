# Standard library
from datetime import datetime, timedelta

# First party
import create_report
import update_currency_exchange
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
  'owner': 'Felipe',
  'start_date': datetime(2023, 1 ,1)
  }

with DAG(
    "currency_exchange_etl",
    default_args = default_args,
    start_date=datetime(2022, 1 ,30),
    schedule_interval='0 12 * * 1', # At 12:00 every Monday
    catchup=False
    ) as dag:

    # Run Unit Tests and save log file
    quality_connection_tests = BashOperator(
        task_id='quality_connection_tests',
        bash_command=r'python3 test_api_input.py',
        execution_timeout =timedelta(minutes=20)
        )
    
    # run ETL pipeline
    run_etl_pipeline = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable= update_currency_exchange.etl_pipeline,
        execution_timeout =timedelta(minutes=20)
        )
    # Generate Excel report
    excel_report = PythonOperator(
        task_id="excel_report",
        python_callable= create_report.report_pipeline,
        execution_timeout =timedelta(minutes=20)
        )

    quality_connection_tests >> run_etl_pipeline >> excel_report
