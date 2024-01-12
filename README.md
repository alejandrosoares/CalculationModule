# Calculation Module

Module for statistical calculations for different financial instruments

From a data file it calculates:
1. the mean of INSTRUMENT1.
2. the mean of INSTRUMENT2 for November 2014.
3. the mean of INSTRUMENT3, it is calculated as the data is loaded.
4. sum of newest 10 items for any instrument except those mentioned above. 
5. calculates the final price for a specific instrument and date. The price of the record is multiplied by multipliers stored in the database.


**The input file must have a CVS extension.** 

For more information about configurations you can see the src/settings.py file.


## How it works
A suitable solution was to split the large file into smaller files by instruments.

subsequent calculations can be accelerated because they are carried out with smaller files. At the same time, the total storage space can be reduced (by about 35%), since only the date and value can be stored in the datasets.

![Schema](schema.png)

1. The **Loader** loads the file by chunk size and sends the chunk to the **PreProcesor** for formatting and cleaning. It also sends the chunk of data to the **Recorder** to perform calculations "on-the-fly".
Finally, the **Loader** splits and saves files by instrument to speed up the time of future calculations.

2. When some operation is selected (for example: mean of any instrument), the **Calculator** asks the **FileLoader** for the data, and the **FileLoader** loads the data of the instrument from a small file.

3. When a final price is necessary to calculate, the **Calculator** asks for the multiplier to the **SQLQueryManager** to retrieve the information from the database.

## Features:

1. To load and transform the original dataset, the **Dask** library was used for memory control and to allow parallel processing.

2. However, the use of **Dask** increases the complexity, so **Pandas** was used to manipulate the smaller datasets. Because the number of records is small. Approximately:
(newest year - oldest year) * record per day = (2014 - 1996) * 365 = 6570 records.

3. To retrieve the multipliers from the database, the data was cached (with a timeout of 5 seconds) and an index was created by instrument_name to speed up the queries.

## Installation
1. Create the virtual enviroment and activate it:
```
    python3 -m venv venv
```
For linux
```
    source venv/bin/activate
```
2. Install requirements:
```
    pip install -r requirements.txt
```
3. Install sqlite3
```
sudo apt install sqlite3
```
4. Run:
```
    python3 src/main.py
```

## Test
1. Once the virtual environment is activated, run:
```
    python3 -m unittest discover
```
