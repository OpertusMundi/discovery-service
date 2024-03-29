import dask.dataframe as dd
import pandas as pd
import os

from pathlib import Path

# Typing
from typing import List


def root_path() -> Path:
    return Path(os.environ["DATA_ROOT_PATH"])


def table_exists(table_path: str) -> bool:
    """
    Checks whether the table exists at the given path.
    """
    path = root_path() / table_path
    return path.exists()


def get_tables() -> List[str]:
    """
    Gets all tables as a list of paths.
    """
    root = root_path()
    tables = []
    for p in root.iterdir():
        tables += [str(p.relative_to(root)).strip("/")
                   for p in (p / "resources").glob("**/*.csv")]
    return tables


def get_table_path_from_asset_id(asset_id: str) -> str:
    root = root_path()
    asset_path = root / asset_id
    for p in (asset_path / "resources").glob("**/*.csv"):
        return str(p.relative_to(root)).strip("/")
    return ""


def get_df(table_path: str, rows=None) -> pd.DataFrame:
    """
    Gets a pandas dataframe from the given table_path.

    The amount of rows can be limited with the 'rows' keyword.
    """
    path = root_path() / table_path

    df = pd.read_csv(
        path,
        header=0,
        engine="python",
        # encoding="utf8",
        quotechar='"',
        escapechar='\\',
        nrows=rows,
        sep=None,
        on_bad_lines='skip',
    )

    return df


def get_ddf(table_path: str) -> dd.DataFrame:
    """
    Gets a dask dataframe from the given table_path.
    """
    path = root_path() / table_path

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
