from abc import ABC, abstractmethod

import dask.dataframe as dd
from settings import (
    DASK_MEMORY_SIZE,
    DF_COLS,
)
from utils import get_full_dataset_path, get_converted_dataset_path


from time import time


__all__ = ['IFileExtensionConverter', 'CsvToParquetConverter']


class IFileExtensionConverter(ABC):
    
    @abstractmethod
    def convert(self) -> None:
        pass


class CsvToParquetConverter(IFileExtensionConverter):
    """
    Converts a csv file to parquet format in chunks.
    """
    def __init__(self, 
        memory: str = DASK_MEMORY_SIZE,
    ):
        self.memory = memory

    def convert(self) -> None:
        start = time()
        file = get_full_dataset_path()
        converted_file = get_converted_dataset_path()
        df = dd.read_csv(
            file,
            blocksize=self.memory,
            header=None,
            names=DF_COLS
        )
        df.to_parquet(converted_file)
        end = time()
        print(f'File converted. Time {end - start} seconds')