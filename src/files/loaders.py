from abc import ABC, abstractmethod

import pandas as pd
from pandas.core.frame import DataFrame

from settings import (
    DATASET_INSTRUMENTS_DATE_FORMAT, 
    DF_CLEANED_COLS,
    DF_COL_DATE
)
from utils import get_full_instrument_path
from exceptions import InstrumentDoesNotExistsException


__all__ = ['IFileLoader', 'FileLoader']


class IDataframeLoader(ABC):
    
    @abstractmethod
    def load_dataframe(self, instrument: str) -> DataFrame:
        pass


class DataframeLoader(IDataframeLoader):
    """
    Loads an instrument data from a file. \n
    If the file of the instrument does not exists in the dataset, raises an InstrumentDoesNotExistsException.
    """

    def load_dataframe(self, instrument: str) -> DataFrame:
        file = get_full_instrument_path(instrument)
        try:
            df = pd.read_csv(file, header=None, names=DF_CLEANED_COLS)
        except FileNotFoundError:
            raise InstrumentDoesNotExistsException(
                f'Instrument {instrument} does not exists in dataset.'
            )
        
        df[DF_COL_DATE] = pd.to_datetime(df[DF_COL_DATE], format=DATASET_INSTRUMENTS_DATE_FORMAT) 
        return df
    