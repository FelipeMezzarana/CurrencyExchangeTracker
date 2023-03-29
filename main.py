import subprocess
import update_currency_exchange
import create_report


if __name__ == '__main__':

    # Run the input testes from CLI
    subprocess.run(
        ["python3", "test_api_input.py"],
        check = True) # Raise error if process exits with a non-zero exit code (stop the pipeline)
    # Creates tables(if necessary) and etl data (without duplicate data)
    update_currency_exchange.etl_pipeline(create_tables = False)
    # Generates 3 reports on updated data in the db: null values, invalid data and analytical
    create_report.report_pipeline()

   


