DB_NAME = 'db.sqlite3'
DB_TABLE_NAME = 'INSTRUMENT_PRICE_MODIFIER'
DB_MULTIPLIER_COLUMN = 'multiplier'
DB_NAME_COLUMN = 'name'

CACHE_TIMEOUT = 5
CACHE_MAXSIZE = 10_000

DATASET_DATE_FORMAT = '%d-%b-%Y'
DATASET_PATH = 'datasets'
DATASET_FILE_NAME = 'example_input.csv'
DATASET_INSTRUMENTS_FOLDER = 'instruments'
DATASET_INSTRUMENTS_DATE_FORMAT = '%Y-%m-%d'

DASK_MEMORY_SIZE="100MB"

DF_COL_NAME = 'instrument_name'
DF_COL_DATE = 'date'
DF_COL_VALUE = 'value'
DF_COL_WEEKDAY = 'weekday'
DF_COLS = [
    DF_COL_NAME,
    DF_COL_DATE,
    DF_COL_VALUE
]

