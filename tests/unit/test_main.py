import unittest
from src import main  
from unittest.mock import Mock, patch

        
class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        """
        pass    

    def test_create_table_currency_exchange(self)-> None:
        """Test main.run.
        """
        
        mock_etl_pipeline = Mock()
        mock_report_pipeline = Mock()

        with (patch("src.main.update_currency_exchange.etl_pipeline", mock_etl_pipeline),
            patch("src.main.create_report.report_pipeline", mock_report_pipeline)):
            main.run()

        




