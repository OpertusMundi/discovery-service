from io import StringIO
# Typing
from typing import List

import dask.dataframe as dd
import pandas as pd
from minio.error import S3Error

from ..clients import dask
from ..clients import minio


def table_exists(bucket: str, table_path: str) -> bool:
    """
    Checks whether the table exists as object in the given bucket at the given path.
    """
    try:
        minio.minio_client.stat_object(bucket, table_path)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        else:
            raise e


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
