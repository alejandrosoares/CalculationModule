import os
import unittest
import sys


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')) 
if path not in sys.path: 
    sys.path.append(path)


from utils import (
    get_dataset_instruments_path,
    get_full_dataset_path
)
from settings import (
    DATASET_PATH,
    DATASET_INSTRUMENTS_FOLDER,
    DATASET_FILE_NAME
)


class TestUtils(unittest.TestCase):
    
    def test_get_full_dataset_path(self):
        expected_path = os.path.join(DATASET_PATH, DATASET_FILE_NAME)
        result = get_full_dataset_path()
        self.assertEqual(result, expected_path)

    def test_get_dataset_instruments_path(self):
        expected_path = os.path.join(DATASET_PATH, DATASET_INSTRUMENTS_FOLDER)
        result = get_dataset_instruments_path()
        self.assertEqual(result, expected_path)



if __name__ == '__main__':
    unittest.main()