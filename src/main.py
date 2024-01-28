# First party
from src.modules import create_report, update_currency_exchange
from src import settings


def run():
    """Execute all steps.
    * Update currency_exchange_db.db with currency in based_currency_mapping (settings.py)
    New tables with diffetent based currency may be created by adding new item in BASED_CURRENCY_MAPPING
    items should be added in the format: based_currency:table_prefix
    A list of available currency can be checked here:
    https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies.json

    * Generate an excel report comparing the chosen currency with dollar and euro
    New currency can be added in REPORT_CURRENCY_LIST (settings.py)
    Each currency will add a tab in the report
    """
    # updates the db regardless of the last update date
    update_currency_exchange.etl_pipeline(settings.BASED_CURRENCY_MAPPING, settings.DB_PATH)
    # Generates Excel report
    create_report.report_pipeline(settings.REPORT_CURRENCY_LIST, settings.DB_PATH)

    return True

if __name__ == "__main__":
    run()
    