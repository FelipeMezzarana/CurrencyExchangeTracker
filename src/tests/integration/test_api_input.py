import unittest
from datetime import datetime,timedelta
import requests
import os

class TestCurrencyRatesAPI(unittest.TestCase):
    """Runs data quality and connection tests
    """

    @classmethod
    def setUpClass(cls):
        """Loads an API request as a Class attribute
        """

        # Requests the most recent exchange rate available
        apiVersion = '1'
        endpoint = f'currencies/usd.json'
        request_date = 'latest'
        url = f'https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@{apiVersion}/{request_date}/{endpoint}'
        cls.resp = requests.get(url)


    def test_check_api_connection(self): 
        """Check if API connection is available
        """

        self.assertEqual(self.resp.status_code,200, (
            'Attempt to connect to API failed'
            f'\nStatus Code: {self.resp.status_code}'))


    def test_check_db_connection(self): 
        """Check if database connection is available
        * We are working with a SQLite db, so we just need to check if the file exists
        """

        self.assertIn('currency_exchange_db.db',os.listdir('Database'), ('Attempt to connect to database failed'
                                         '\nDatabase must be initialized with Docker using the file:'
                                         '\nLinkfire_data_engineer_task/database/volume/docker-compose.yaml'))


    def test_latest_date(self):
        """latest date must be be same as today
        * Check if api is being updated
        """

        latest_date = self.resp.json().get('date')
        today = datetime.today().strftime(r"%Y-%m-%d")
        today_lag1 = (datetime.today() - timedelta(days=1) ).strftime(r"%Y-%m-%d") # Sometimes "latest request" return today -1

        self.assertIn(latest_date,[today,today_lag1])


    def test_qty_currencies(self):
        """ Check quantity of currencies the API returned 
        """

        qty_currencies = len(self.resp.json().get('usd'))
        self.assertEqual(qty_currencies,272,('the db contains columns for 272 currencies,'
                                             ' if more are added in the API, adaptations in the code may be required'))   


    def save(self):
        """Save a log file
        """

        # Defines the name of the log file (.txt) that will be created
        today = datetime.today().strftime(r"%Y-%m-%d %H.%M")
        log_name =  r'log_api_input_' + str(today) + r'.txt'
        log_path = r'log/' + str(log_name)
        with open(log_path , 'w', encoding='utf-8') as file:
            runner = unittest.TextTestRunner(file)
            unittest.main(testRunner = runner, verbosity = 2)


def main():
    STR = TestCurrencyRatesAPI()  
    STR.save()


if __name__ == '__main__':
    main()