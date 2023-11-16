import unittest
from src.modules import update_currency_exchange  
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime,timedelta
from typing import Optional
import json


class MockRequests:
    """Mock requests.get(url)
    """
    def __init__(self,status_code:int,json_file:Optional[dict]=None) -> None:
        self.status_code = status_code
        self.json_file = json_file

    def json(self):
        return self.json_file

    

class TestUodateCurrencyExchange(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Mock libs used several times.
        * sqlite3 connection.
        * pandas (for sql operations)
        """

        cls.mock_sqlite3_conn= Mock()
        cls.mock_sqlite3= Mock()
        cls.mock_sqlite3.connect.return_value = cls.mock_sqlite3_conn
        cls.mock_sqlite3_conn.close.return_value = None

        cls.pandas_sql_mock = Mock()
        cls.pandas_sql_mock.to_sql.return_value= None
        
    def test_create_table_currency_exchange(self)-> None:
        """Test create_table_currency_exchange func.
        """
        
        with (patch("src.modules.update_currency_exchange.sqlite3", self.mock_sqlite3),
            patch("src.modules.update_currency_exchange.pd", self.pandas_sql_mock)):
            update_currency_exchange.create_table_currency_exchange("db_path","table_name")

    def test_last_exchange_date(self)-> None:
        """Test last_exchange_date func.
        """

        with patch("src.modules.update_currency_exchange.sqlite3", self.mock_sqlite3):
            last_update_date = update_currency_exchange.last_exchange_date("db_path","table_name")
        
        # Without mocking pd.read_sql_query the query will fail and should return last date as 1 year ago.
        expected_last_update_date = datetime.today() - timedelta(days=365)
        self.assertEqual(
            last_update_date.strftime("%Y-%m-%d"), # Removing millisecond granularity
            expected_last_update_date.strftime("%Y-%m-%d")
            )

        expected_last_update_date = (datetime.today() - timedelta(days=10))
        mock_last_date = expected_last_update_date.strftime("%Y-%m-%d")
        
        self.pandas_sql_mock.read_sql_query.return_value = pd.DataFrame({"last_update_date":[mock_last_date]})

        with (patch("src.modules.update_currency_exchange.sqlite3", self.mock_sqlite3),
              patch("src.modules.update_currency_exchange.pd", self.pandas_sql_mock)):
            last_update_date = update_currency_exchange.last_exchange_date("db_path","table_name")

        self.assertEqual(
            last_update_date.strftime("%Y-%m-%d"), # Removing millisecond granularity
            expected_last_update_date.strftime("%Y-%m-%d"))
        

    def test_get_currency_exchange(self)->None:
        """Test get_currency_exchange func.
        """

        mock_requests= Mock()

        request_sample_json = open("tests/unit/sample_data/currencies_request_sample.json", "r")
        request_sample = json.load(request_sample_json)
        mock_requests.get.return_value = MockRequests(status_code = 200,json_file = request_sample)
        
        mock_check_table = Mock()
        table_sample = pd.read_csv("tests/unit/sample_data/usd_based_currency_sample.csv")
        mock_check_table.return_value = table_sample.drop(table_sample.index) # Drop data, keep structure

        # Ususal case
        with (patch("src.modules.update_currency_exchange.requests", mock_requests),
              patch("src.modules.update_currency_exchange.check_table", mock_check_table)):
            currency_df = update_currency_exchange.get_currency_exchange(
                db_path= "db_path",
                table_name = "table_name",
                based_currency = "usd",
                since_date = (datetime.today() - timedelta(days=1))
                )
            self.assertEqual(currency_df.columns.tolist(), table_sample.columns.tolist())
    
        # Requesting with db alredy updated
        currency_df = update_currency_exchange.get_currency_exchange(
                db_path= "db_path",
                table_name = "table_name",
                based_currency = "usd",
                since_date = datetime.today()
                )
        self.assertTrue(currency_df.empty)

        # API missing requested day.
        # It's a free API, delays or even lost days may occur
        # In this case, we will take the data from the previous day.
        # We will raise an error only if we have two missing days.
        mock_requests.get.return_value = MockRequests(status_code = 503,json_file = None)
        with (patch("src.modules.update_currency_exchange.requests", mock_requests),
                patch("src.modules.update_currency_exchange.check_table", mock_check_table)):
            self.assertRaises(
                Exception,
                update_currency_exchange.get_currency_exchange,
                "db_path",
                "table_name",
                "usd",
                (datetime.today() - timedelta(days=1))
                )












