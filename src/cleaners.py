from abc import ABC, abstractmethod

from dask.dataframe.core import DataFrame
from settings import (
    DF_COL_DATE,
    DF_COL_WEEKDAY
)


__all__ = ['IPreprocessor', 'Preprocessor']


class IDataCleaner(ABC):

    @abstractmethod
    def clean(self, df: DataFrame) -> DataFrame:
        pass


class DataCleaner(IDataCleaner):
    """
    Cleans the unwanted data.
    """

    def clean(self, df: DataFrame) -> DataFrame:
        df = self._create_auxiliar_cols(df)
        df = self._remove_non_business_days(df)
        df = self._remove_auxiliar_cols(df)
        return df
    
    def _create_auxiliar_cols(self, df):
        df[DF_COL_WEEKDAY] = df[DF_COL_DATE].dt.weekday
        return df
    
    def _remove_non_business_days(self, df: DataFrame) -> DataFrame:
        sunday = 6
        saturday = 5
        df = df[(df.weekday != sunday) | (df.weekday != saturday)]
        return df
    
    def _remove_auxiliar_cols(self, df: DataFrame) -> DataFrame:
        df = df.drop(DF_COL_WEEKDAY, axis=1)
        return df