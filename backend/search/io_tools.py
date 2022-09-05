import dask.dataframe as dd
import pandas as pd

from io import StringIO

from minio.error import NoSuchKey
# Typing
from typing import List, Dict, Any

from ..clients import minio
from ..clients import dask



def table_exists(bucket: str, table_path: str) -> bool:
    """
    Checks whether the table exists as object in the given bucket at the given path.
    """ 
    try:
        minio.minio_client.stat_object(bucket, path)
        return True
    except NoSuchKey:
        return False


def get_tables(bucket: str) -> List[str]:
    """
    Gets all objects in the given bucket as a list of paths.
    """ 
    objects = minio.minio_client.list_objects(bucket, recursive=True)
    return [o.object_name for o in objects]


def bucket_exists(bucket: str) -> bool:
    """
    Checks whether the given bucket exists.
    """ 
    return minio.minio_client.bucket_exists(bucket)


def get_df(bucket: str, table_path: str, rows=None) -> pd.DataFrame:
    """
    Gets a pandas dataframe from the given bucket/table_path combination.

    The amount of rows can be limited with the 'rows' keyword.
    """ 
    res = minio.minio_client.get_object(bucket, table_path)
    csv_string = res.data.decode("utf-8")
    res.close()
    res.release_conn()

    df = pd.read_csv(
        StringIO(csv_string), 
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
    minio_path = f"s3://{bucket}/{table_path}"

    ddf = dd.read_csv(
        minio_path,
        sample_rows=1000,  # Sample 1000 rows to auto-determine dtypes
        blocksize=25e6,  # 25MB per block
        header=0,
        engine="python",
        encoding="utf8",
        quotechar='"',
        escapechar='\\',
        # on_bad_lines='warn', # For some reason Dask doesn't like this keyword parameter all of a sudden, even though it is supported!
        storage_options=dask.get_s3_settings()
    )

    return ddf


def get_unique_values(ddf):
    ddf = ddf.select_dtypes(exclude=['number'])  # Drop numerics, no need to search these in ES

    # This might still cause memory issues for very large DFs
    # TODO: Dump to disk and read from there
    get_unique = lambda s: s.unique().compute()

    futures = dask.get_client().map(get_unique, [ddf[col] for col in ddf.columns])
    results = dask.get_client().gather(futures)

    return dict(zip(ddf.columns, results))
