from abc import ABC, abstractmethod

from dask.dataframe.core import DataFrame
from settings import (
    DF_COL_NAME,
    DF_COL_VALUE
)

__all__ = ['IFileDataRecorder', 'FileDataRecorder']


class IFileDataRecorder(ABC):

    @abstractmethod
    def record_data(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def get_statistics(self) -> dict:
        pass


class FileDataRecorder:
    """
    Record statistics of the data while the data is being loaded.
    """

    def __init__(self):
        self.statistics = {'INSTRUMENT3': {'mean': 0}}

    def record_data(self, df: DataFrame) -> DataFrame:
        self._record_mean_of_instrument3(df)

    def _record_mean_of_instrument3(self, df: DataFrame) -> None:
        instrument_3 = df[df[DF_COL_NAME] == 'INSTRUMENT3']
        mean = instrument_3[DF_COL_VALUE].mean().compute()
        self.statistics['INSTRUMENT3']['mean'] = mean

    def get_statistics(self) -> dict:
        return self.statistics

    