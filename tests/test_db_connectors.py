import os
import sys
import unittest
from unittest.mock import MagicMock

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')) 
if path not in sys.path: 
    sys.path.append(path)

from settings import (
    DB_MULTIPLIER_COLUMN,
    DB_TABLE_NAME,
    DB_NAME_COLUMN
)
from db_connectors import SQLQueryManager


class TestSQLQueryManager(unittest.TestCase):

    def setUp(self):
        self.db_connector_mock = MagicMock()
        self.sql_handler = SQLQueryManager(self.db_connector_mock)

    def test_get_multiplier(self):
        instrument = "example_instrument"
        expected_query = 'SELECT {} FROM {} where {} = "{}"'.format(
            DB_MULTIPLIER_COLUMN,
            DB_TABLE_NAME,
            DB_NAME_COLUMN,
            instrument
        )

        expected_result = 1.5
        self.db_connector_mock.fetchone.return_value = (expected_result,)
        result = self.sql_handler.get_multiplier(instrument)
        self.db_connector_mock.fetchone.assert_called_once_with(expected_query)
        
        self.assertEqual(result, expected_result)

    def test_get_multiplier_no_result(self):
        instrument = "nonexistent_instrument"
        expected_query = 'SELECT {} FROM {} where {} = "{}"'.format(
            DB_MULTIPLIER_COLUMN,
            DB_TABLE_NAME,
            DB_NAME_COLUMN,
            instrument
        )
        expected_result = 1
        self.db_connector_mock.fetchone.return_value = None
        result = self.sql_handler.get_multiplier(instrument)
        self.db_connector_mock.fetchone.assert_called_once_with(expected_query)
        
        self.assertEqual(result, expected_result)

    def test_close(self):
        self.sql_handler.close()
        self.db_connector_mock.close_connection.assert_called_once()


if __name__ == '__main__':
    unittest.main()