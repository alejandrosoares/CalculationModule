from abc import ABC, abstractmethod

import dask.dataframe as dd
from dask.dataframe.core import DataFrame
from settings import DATASET_DATE_FORMAT


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
        df['date'] = dd.to_datetime(df['date'], format=DATASET_DATE_FORMAT)
        return df
    
    def _create_auxiliar_cols(self, df):
        df['weekday'] = df['date'].dt.weekday
        return df
    
    def _remove_non_business_days(self, df: DataFrame) -> DataFrame:
        sunday = 6
        saturday = 5
        df = df[(df.weekday != sunday) | (df.weekday != saturday)]
        return df
    
    def _remove_auxiliar_cols(self, df: DataFrame) -> DataFrame:
        df = df.drop('weekday', axis=1)
        return df