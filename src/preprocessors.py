from abc import ABC, abstractmethod

import dask.dataframe as dd
from dask.dataframe.core import DataFrame
from settings import (
    DATASET_DATE_FORMAT,
    DF_COL_DATE,
    DF_COL_WEEKDAY
)


__all__ = ['IPreprocessor', 'Preprocessor']


class IPreprocessor(ABC):

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        pass


class Preprocessor:
    """
    Format the data and clean the unwanted data.
    """

    def process(self, df: DataFrame) -> DataFrame:
        df = self._format_date(df)
        df = self._create_auxiliar_cols(df)
        df = self._remove_non_business_days(df)
        df = self._remove_auxiliar_cols(df)
        return df

    def _format_date(self, df: DataFrame) -> DataFrame:
        df[DF_COL_DATE] = dd.to_datetime(df[DF_COL_DATE], format=DATASET_DATE_FORMAT)
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