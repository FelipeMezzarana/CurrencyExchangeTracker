# Standard library
import os
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

# Third party
import pandas as pd

# First party
from src.modules import create_report


class TestCreateReport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        """
        cls.dollar_based_table = pd.read_csv(
            "tests/unit/sample_data/dollar_based_currency_full_table.csv"
            ) 
        cls.euro_based_table = pd.read_csv(
            "tests/unit/sample_data/euro_based_currency_full_table.csv"
            ) 
        
    @patch("src.modules.create_report.complete_table_df")
    @patch("src.modules.create_report.generate_excel_report")
    def test_report_pipelinee(self,m1,m2)-> None:
        """Mock everything just to ensure that new features will be tested,
        * Mocked funcs will be tested individually later.
        """
        
        result = create_report.report_pipeline('mock_currency_list', 'mock_db_path')
        self.assertTrue(result)

    def test_generate_excel_report(self)-> None:
        """Test Excel report generation, generate_excel_report().
        """
        
        create_report.generate_excel_report(self.dollar_based_table,self.euro_based_table,["dkk", "brl"])

        file_path = "Exchange Rate Report " + datetime.today().strftime("%Y-%d-%m") + ".xlsx"
        self.assertIn(file_path,os.listdir())

    def test_specific_info_df(self)-> None:
        """
        """

        infos_df, currency_name = create_report.specific_info_df(
            self.dollar_based_table,
            self.euro_based_table,
            "dkk"
            )

        self.assertEqual(currency_name,"Dkk")
        self.assertEqual(infos_df["Dollar Based Rate"]["Last Year Range"],"6.62 - 7.25")
