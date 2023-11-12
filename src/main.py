# Standard library
import logging

# First party
from src.modules import create_report, update_currency_exchange

# Define global logging config
logging.basicConfig(
    format="%(levelname)s - Line %(lineno)d (%(name)s %(asctime)s) - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)


DB_PATH = "src/database/currency_exchange_db.db"
BASED_CURRENCY_MAPPING = {"usd": "dollar", "eur": "euro"}
REPORT_CURRENCY_LIST = ["dkk", "brl", "jpy", "gbp", "cny"]


def run():
    """Execute all steps.
    * Update currency_exchange_db.db with currency in based_currency_mapping
    New tables with diffetent based currency may be created by adding new item in BASED_CURRENCY_MAPPING
    items should be added in the format: based_currency:table_prefix
    A list of available currency can be checked here:
    https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies.json

    * Generate an excel report comparing the chosen currency with dollar and euro
    New currency can be added in REPORT_CURRENCY_LIST, each currency will add a tab in the report
    """
    # updates the db regardless of the last update date
    update_currency_exchange.etl_pipeline(BASED_CURRENCY_MAPPING, DB_PATH)
    # Generates Excel report
    create_report.report_pipeline(REPORT_CURRENCY_LIST, DB_PATH)


if __name__ == "__main__":
    run()
