# Standard library
import os
import unittest
from datetime import datetime, timedelta

# Third party
import requests
import pandas as pd
from src import settings


class TestCurrencyRatesAPI(unittest.TestCase):
    """Runs data quality and connection tests"""

    @classmethod
    def setUpClass(cls):
        """Loads an API request as a Class attribute"""

        # Requests the most recent exchange rate available
        apiVersion = "1"
        endpoint = "currencies/usd.json"
        request_date = "latest"
        url = f"https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@{apiVersion}/{request_date}/{endpoint}"
        cls.resp = requests.get(url)

        # Sample table (empty) of current db
        cls.table_sample = pd.read_csv("tests/unit/sample_data/usd_based_currency_sample.csv")

    def test_check_api_connection(self):
        """Check if API connection is available"""

        self.assertEqual(
            self.resp.status_code,
            200,
            f"Attempt to connect to API failed\nStatus Code: {self.resp.status_code}",
        )

    def test_check_db_connection(self):
        """Check if database connection is available
        * We are working with a SQLite db, so we just need to check if the file exists
        """

        self.assertIn(
            "currency_exchange_db.db",
            os.listdir("src/database"),
            "Attempt to connect to database failed. "
        )

    def test_latest_date(self):
        """latest date must be be same as today
        * Check if api is being updated
        """

        latest_date = self.resp.json().get("date")
        today = datetime.today().strftime(r"%Y-%m-%d")
        today_lag1 = (datetime.today() - timedelta(days=1)).strftime(
            r"%Y-%m-%d"
        )  # Sometimes "latest request" return today -1

        self.assertIn(latest_date, [today, today_lag1])

    def test_currencies(self):
        """Check if latest API call has all currencies used in current DB.
        * Note that we are basing this test on the sample:
            tests/unit/sample_data/usd_based_currency_sample.csv
        If the API adds new currencies, when creating a new db, the sample files must be updated
        as the db will include all currencies available at the time of its creation. 

        * We will accept up to 5 missing exchange rates, 
        as we have a number of virtual currencies that may cease to exist.
        """
 
        currencies_rate = self.resp.json().get("usd")
        available_currencies = list(currencies_rate.keys())
        available_currencies.append("exchange_date") # Column created after API call
        required_currencies = settings.REPORT_CURRENCY_LIST

        for currency in required_currencies:
            self.assertIn(currency,available_currencies)


    def save(self):
        """Save a log file"""

        # Defines the name of the log file (.txt) that will be created
        today = datetime.today().strftime(r"%Y-%m-%d %H.%M")
        log_name = r"log_api_input_" + str(today) + r".txt"
        log_path = r"log/" + str(log_name)
        with open(log_path, "w", encoding="utf-8") as file:
            runner = unittest.TextTestRunner(file)
            unittest.main(testRunner=runner, verbosity=2)


def main():
    STR = TestCurrencyRatesAPI()
    STR.save()


if __name__ == "__main__":
    main()
