from src.modules import update_currency_exchange
from src.modules import create_report
import logging

# Define global logging config
logging.basicConfig(
    format="%(levelname)s - Line %(lineno)d (%(name)s %(asctime)s) - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

def run():
    # updates the db regardless of the last update date
    update_currency_exchange.etl_pipeline()
    # Generates Excel report
    create_report.report_pipeline()

if __name__ == '__main__':
    run()



   


