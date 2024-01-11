import os

from settings import (
    DATASET_PATH,
    DATASET_FILE_NAME,
    DATASET_INSTRUMENTS_FOLDER
)


def get_full_dataset_path() -> str:
    return os.path.join(DATASET_PATH, DATASET_FILE_NAME)


def get_dataset_instruments_path() -> str:
    return os.path.join(DATASET_PATH, DATASET_INSTRUMENTS_FOLDER)
