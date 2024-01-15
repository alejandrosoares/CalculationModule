from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from pandas.core.series import Series
from pandas.core.frame import DataFrame

from settings import (
    DF_COL_DATE,
    DF_COL_VALUE
)
from recorders import IFileDataRecorder
from db_connectors import ISQLQueryManager
from exceptions import InvalidInstrumentException
from files.loaders import IDataframeLoader


__all__ = ['ICalculationEngine', 'CalculationEngine']


class ICalculationEngine(ABC):
    
    @abstractmethod
    def get_mean_of_instrument(self, instrument: str) -> int:
        pass

    @abstractmethod
    def get_mean_of_instrument_between_dates(
        self, 
        start: datetime,
        end: datetime
    ) -> int:
        pass
    
    @abstractmethod
    def get_sum_of_newest(self, instrument: str, items: int) -> float:
        pass

    @abstractmethod
    def get_final_price_by_instrument_and_date(
        self, 
        instrument: str, 
        date: datetime
    ) -> float:
        pass
    
    @abstractmethod
    def get_statistical_of_instrument(self) -> dict:
        pass
    


class CalculationEngine(ICalculationEngine):
    """
    Makes the statistical calculations. \n
    If the instrument does not exists, raises an InstrumentDoesNotExistsException. \n
    If the operation is not valid for the instrument, raises an InvalidInstrumentException.
    """
    def __init__(self, 
        sql_handler: ISQLQueryManager,
        dataframe_loader: IDataframeLoader,
        data_recorder: IFileDataRecorder = None
    ):
        self.sql_handler = sql_handler
        self.dataframe_loader = dataframe_loader
        self.data_recorder = data_recorder

    def get_mean_of_instrument(self, instrument: str = 'INSTRUMENT1') -> int:
        df = self._load_dataframe(instrument)
        mean = self._calculate_mean(df)
        return mean
    
    def get_mean_of_instrument_between_dates(
        self, 
        instrument: str = 'INSTRUMENT2', 
        start: datetime = datetime(2014, 11 , 1),
        end: datetime = datetime(2014, 11 , 30) 
    ) -> int:
        df = self._load_dataframe(instrument)
        df_between_period = self._filter_by_period(df, start, end)
        mean = self._calculate_mean(df_between_period)
        return mean
    
    def get_statistical_of_instrument(self) -> dict:
        """
        Returns the statistical calculations of the data while the data is being loaded.
        """
        if self.data_recorder:
            return self.data_recorder.get_statistics()
        return {}

    def get_sum_of_newest(self, instrument: str, items: int = 10) -> float:
        invalids = ['INSTRUMENT1', 'INSTRUMENT2', 'INSTRUMENT3']
        if instrument in invalids:
            raise InvalidInstrumentException(
                f'Instrument "{instrument}" is not valid for this operation.'
            )
        
        df = self._load_dataframe(instrument)
        df_sorted = self._sort_dataframe(df)
        total = df_sorted[DF_COL_VALUE].head(items).sum()
        return total
    
    def get_final_price_by_instrument_and_date(self, instrument: str, date: datetime) -> Optional[float]:
        """
        Returns the final price by date and instrument name.
        If the instrument does not exist, it returns None. \n
        The final price is calculated by multiplying the value (price) of the instrument by the multiplier.
        If the multiplier does not exist in the database, it will be considered as 1.
        """
        df = self._load_dataframe(instrument)
        _instrument = self._filter_by_date(df, date)
        if _instrument is None:
            return None

        multiplier = self.sql_handler.get_multiplier(instrument)
        return _instrument.value * multiplier
    
    def _load_dataframe(self, instrument: str) -> DataFrame:
        df = self.dataframe_loader.load_dataframe(instrument)
        return df

    def _calculate_mean(self, df: DataFrame) -> float:
        mean = df[DF_COL_VALUE].mean()
        return mean
    
    def _filter_by_date(self, df: DataFrame, date: datetime) -> Optional[Series]:
        instrument = df[df[DF_COL_DATE] == date]
        if instrument.empty:
            return None
        return instrument.iloc[0]
    
    def _filter_by_period(self, df: DataFrame, start: datetime, end: datetime) -> DataFrame:
        df_between_period = df[(df[DF_COL_DATE] >= start) & (df[DF_COL_DATE] <= end)]
        return df_between_period
    
    def _sort_dataframe(self, df: DataFrame) -> DataFrame:
        df_sorted = df.sort_values(by=[DF_COL_DATE], ascending=False)
        return df_sorted