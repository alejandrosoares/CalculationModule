from abc import ABC, abstractmethod

from dask.dataframe.core import DataFrame


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
        instrument_3 = df[df['instrument_name'] == 'INSTRUMENT3']
        mean = instrument_3['value'].mean().compute()
        self.statistics['INSTRUMENT3']['mean'] = mean

    def get_statistics(self) -> dict:
        return self.statistics

    