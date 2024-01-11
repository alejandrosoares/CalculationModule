import os
from abc import ABC, abstractmethod

import pandas as pd
from pandas.core.frame import DataFrame

from settings import DATASET_INSTRUMENTS_DATE_FORMAT
from utils import get_dataset_instruments_path
from exceptions import InstrumentDoesNotExistsException


__all__ = ['IFileLoader', 'FileLoader']


class IFileLoader(ABC):
    
    @abstractmethod
    def load_instrument_dataframe(self, instrument: str) -> DataFrame:
        pass


class FileLoader:
    """
    Loads an instrument data from a file. \n
    If the file of the instrument does not exists in the dataset, raises an InstrumentDoesNotExistsException.
    """

    def load_instrument_dataframe(self, instrument: str) -> DataFrame:
        file = os.path.join(get_dataset_instruments_path(), f'{instrument}.csv')
        try:
            df = pd.read_csv(file, header=None, names=['instrument_name', 'date', 'value'])
        except FileNotFoundError:
            raise InstrumentDoesNotExistsException(
                f'Instrument {instrument} does not exists in dataset.'
            )
        
        df['date'] = pd.to_datetime(df['date'], format=DATASET_INSTRUMENTS_DATE_FORMAT) 
        return df
    