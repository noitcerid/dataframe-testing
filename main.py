import duckdb
import pandas as pd
import polars as pl
from datetime import datetime

file_path = '/home/chris/US_Accidents_March23.csv'


def import_duckdb():
    # read a CSV file into a Relation
    duckdb.read_csv(file_path)

    # directly query a CSV file
    results = duckdb.sql(f'SELECT * FROM "{file_path}"').df()

    return results


def import_pandas():
    # Run same routine as duckdb test import
    # read CSV
    results = pd.read_csv(file_path)

    return results


def import_pyarrow():
    results = pd.read_csv(file_path, engine='pyarrow')

    return results


def import_polars():
    results = pl.read_csv(file_path)

    return results


def append_column_to_df_duckdb():
    duckdb.read_csv(file_path)
    etl_batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = duckdb.sql(f"SELECT *, '{etl_batch_id}' AS _ETL_BATCH_ID FROM '{file_path}'").df()


def append_column_to_df_pandas():
    results = pd.read_csv(file_path)
    results['_ETL_BATCH_ID'] = datetime.now().strftime("%Y%m%d_%H%M%S")


def append_column_to_df_pyarrow():
    results = pd.read_csv(file_path, engine='pyarrow')
    results['_ETL_BATCH_ID'] = datetime.now().strftime("%Y%m%d_%H%M%S")


def append_column_to_df_polars():
    results = pl.read_csv(file_path)
    etl_batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = results.with_columns(pl.lit(etl_batch_id).alias('_ETL_BATCH_ID'))


print('Starting DuckDB test...')
duck_start = datetime.now()
import_duckdb()
duck_end = datetime.now()

print('Starting pandas test...')
pandas_start = datetime.now()
import_pandas()
pandas_end = datetime.now()

print('Starting pyarrow test...')
pyarrow_start = datetime.now()
import_pyarrow()
pyarrow_end = datetime.now()

print('Starting polars test...')
polars_start = datetime.now()
import_polars()
polars_end = datetime.now()

print('Starting DuckDB column append...')
duck_append_start = datetime.now()
append_column_to_df_duckdb()
duck_append_end = datetime.now()

print('Starting pandas column append...')
pandas_append_start = datetime.now()
append_column_to_df_pandas()
pandas_append_end = datetime.now()

print('Starting pyarrow column append...')
pyarrow_append_start = datetime.now()
append_column_to_df_pyarrow()
pyarrow_append_end = datetime.now()

print('Starting polars column append...')
polars_append_start = datetime.now()
append_column_to_df_polars()
polars_append_end = datetime.now()

print(f'Results:\n'
      f'\n--------------\n'
      f'\nDuckDB\n--------------\n'
      f'\tImport time: {(duck_end - duck_start).total_seconds()} seconds\n'
      f'\tAppend time: {(duck_append_end - duck_append_start).total_seconds()} seconds\n'
      f'\n--------------\n'
      f'\npandas\n--------------\n'
      f'\tImport time: {(pandas_end - pandas_start).total_seconds()} seconds\n'
      f'\tAppend time: {(pandas_append_end - pandas_append_start).total_seconds()} seconds\n'
      f'\n--------------\n'
      f'\nPyArrow\n--------------\n'
      f'\tImport time: {(pyarrow_end - pyarrow_start).total_seconds()} seconds\n'
      f'\tAppend time: {(pyarrow_append_end - pyarrow_append_start).total_seconds()} seconds\n'
      f'\n--------------\n'
      f'\npolars\n--------------\n'
      f'\tImport time: {(polars_end - polars_start).total_seconds()} seconds\n'
      f'\tAppend time: {(polars_append_end - polars_append_start).total_seconds()} seconds'
      )
