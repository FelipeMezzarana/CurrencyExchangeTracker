import unittest
from src import main  
from unittest.mock import Mock, patch

        
class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        """
        pass    

    @patch("src.main.update_currency_exchange.etl_pipeline")
    @patch("src.main.create_report.report_pipeline")
    def test_create_table_currency_exchange(self,m1,m2)-> None:
        """Test main.run.
        """
        is_successful = main.run()
        self.assertTrue(is_successful)

        




