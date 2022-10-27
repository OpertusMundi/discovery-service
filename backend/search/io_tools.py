import dask.dataframe as dd
import pandas as pd
import os

from pathlib import Path

# Typing
from typing import List


def root_path():
    return Path(os.environ["DATA_ROOT_PATH"])


def table_exists(bucket: str, table_path: str) -> bool:
    """
    Checks whether the table exists as object in the given bucket at the given path.
    """
    path = root_path() / bucket / table_path
    return path.exists()


def get_tables(bucket: str) -> List[str]:
    """
    Gets all objects in the given bucket as a list of paths.
    """
    path = root_path() / bucket
    return [p.name for p in path.glob("**/*.csv")]


def bucket_exists(bucket: str) -> bool:
    """
    Checks whether the given bucket exists.
    """
    path = root_path() / bucket
    return path.exists()


def get_df(bucket: str, table_path: str, rows=None) -> pd.DataFrame:
    """
    Gets a pandas dataframe from the given bucket/table_path combination.

    The amount of rows can be limited with the 'rows' keyword.
    """
    path = root_path() / bucket / table_path

    df = pd.read_csv(
        path,
        header=0,
        engine="python",
        encoding="utf8",
        quotechar='"',
        escapechar='\\',
        nrows=rows
    )

    return df


def get_ddf(bucket: str, table_path: str) -> dd.DataFrame:
    """
    Gets a dask dataframe from the given bucket/table_path combination.
    """
    path = root_path() / bucket / table_path

    ddf = dd.read_csv(
        path,
        sample_rows=1000,  # Sample 1000 rows to auto-determine dtypes
        blocksize=25e6,  # 25MB per block
        header=0,
        engine="python",
        encoding="utf8",
        quotechar='"',
        escapechar='\\',
        # on_bad_lines='warn', # For some reason Dask doesn't like this keyword parameter all of a sudden, even though it is supported!
    )

    return ddf
