# Standard library
import logging

# Define global logging config
logging.basicConfig(
    format="%(levelname)s - Line %(lineno)d (%(name)s %(asctime)s) - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

# SqlLite db path
DB_PATH = "src/database/currency_exchange_db.db"
# Add new tables with different based currency here
# Expected format -> {based_currency:table_prefix}
BASED_CURRENCY_MAPPING = {"usd": "dollar", "eur": "euro"}
# Add new currency to the Excel report here
# Each currency generate a tab in the report
REPORT_CURRENCY_LIST = ["dkk", "brl", "jpy", "gbp", "cny"]
