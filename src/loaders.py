import os

import dask.dataframe as dd
from dask.dataframe.core import DataFrame

from settings import (
    DASK_MEMORY_SIZE,
    DF_COLS,
    DF_COL_NAME,
    DF_CLEANED_COLS
)
from utils import get_dataset_instruments_path, get_full_dataset_path
from preprocessors import IPreprocessor
from recorders import IFileDataRecorder


class Loader:
    """
    Loads an input file in chunks, preprocesses the data using a Preprocessor class
    and saves each instrument to a separated file.  
    """
    def __init__(self, 
        preprocessor: IPreprocessor, 
        memory: str = DASK_MEMORY_SIZE,
        recorder: IFileDataRecorder = None
    ):
        self.memory = memory
        self.preprocessor = preprocessor
        self.recorder = recorder

    def init(self) -> None:
        df = self._read_file()
        df = self.preprocessor.process(df)
        self._record_data(df)
        self._save_by_instrument(df)

    def _read_file(self) -> None:
        dataset_path = get_full_dataset_path()
        df = dd.read_csv(
            dataset_path,
            blocksize=self.memory,
            header=None,
            names=DF_COLS
        )
        return df

    def _record_data(self, df: DataFrame) -> None:
        if not self.recorder:
            return
        self.recorder.record_data(df) 

    def _save_by_instrument(self, df:DataFrame) -> None:
        instruments = df[DF_COL_NAME].unique().compute().tolist()
        for instrument in instruments:
            df_instrument = df[df[DF_COL_NAME] == instrument]
            self._save_file(df_instrument, instrument)
    
    def _save_file(self, df: DataFrame, instrument: str) -> None:
        file = os.path.join(get_dataset_instruments_path(), f'{instrument}.csv')
        df.to_csv(
            file,
            index=False, 
            header=False,
            mode='at',
            columns=DF_CLEANED_COLS
        )

    