import os
import unittest
import sys

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')) 
if path not in sys.path: 
    sys.path.append(path)

import pandas as pd
from pandas import Timestamp

from utils import get_dataset_instruments_path
from file_loaders import FileLoader


class TestFileLoader(unittest.TestCase):

    def setUp(self):
        self.file_loader = FileLoader()

    def test_load_instrument_dataframe(self):
        instrument = 'INSTRUMENT1'
        expected_file = os.path.join(get_dataset_instruments_path(), f'{instrument}.csv')

        with open(expected_file, 'w') as f:
            f.write('instrument1,2022-01-01,10\n')
            f.write('instrument1,2022-01-02,20\n')

        expected_df = pd.DataFrame({
            'instrument_name': ['instrument1', 'instrument1'],
            'date': [Timestamp('2022-01-01'), Timestamp('2022-01-02')],
            'value': [10, 20]
        })

        df_instrument = self.file_loader.load_instrument_dataframe(instrument)
        self.assertTrue(isinstance(df_instrument, pd.DataFrame))
        self.assertEqual(df_instrument.to_dict(), expected_df.to_dict())

        os.remove(expected_file)


if __name__ == '__main__':
    unittest.main()