from datetime import datetime

from preprocessors import Preprocessor
from recorders import FileDataRecorder
from calculators import CalculationEngine
from db_connectors import SQLQueryManager, SQLiteConnector
from files.loaders import DataframeLoader
from files.processors import ParquetChunkProcessor
from files.converters import CsvToParquetConverter

import time


def main():

    print('Converting data')
    converter = CsvToParquetConverter()
    converter.convert()
    print('Converting finished')


    print('\nProcessing data...')
    start = time.time()
    data_recorder = FileDataRecorder()
    preprocessor = Preprocessor()
    processor = ParquetChunkProcessor(preprocessor, recorder=data_recorder)
    processor.init()
    end = time.time()
    print(f'\nProcessing finished. Time: {end - start} seconds')


    print('\nLoading finished... Ready to use.')
    start = time.time()
    db = SQLiteConnector()
    sql_handler = SQLQueryManager(db)
    dataframe_loader = DataframeLoader()
    calculator = CalculationEngine(sql_handler, dataframe_loader, data_recorder)


    mean_instrument_one = calculator.get_mean_of_instrument()
    mean_two_between_dates = calculator.get_mean_of_instrument_between_dates()
    sum_newest = calculator.get_sum_of_newest('INSTRUMENT4', 3)
    date = datetime(2014, 12, 5) #05-Dec-2014
    final_price = calculator.get_final_price_by_instrument_and_date('INSTRUMENT3', date)
    statistics = calculator.get_statistical_of_instrument()

    print('\nRESULTS:')
    print(f'Mean of INSTRUMENT1: {mean_instrument_one}')
    print(f'Mean of INSTRUMENT2 between 01-Nov-2014 and 30-Nov-2014: {mean_two_between_dates}')
    print(f'Sum of INSTRUMENT4 newest 3 items: {sum_newest}')
    print(f'Final price of INSTRUMENT3 on 05-Dec-2014: {final_price}')
    print(f'Statistical of INSTRUMENT3: {statistics}')
    
    sql_handler.close()
    end = time.time()
    print(f'\nTotal time: {end - start} seconds')


if __name__ == '__main__':
    main()