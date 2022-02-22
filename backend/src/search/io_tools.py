import dask.dataframe as dd
from ..clients import minio
from ..clients import dask


def get_tables():
    objects = minio.get_client().list_objects(minio.get_s3_settings()['default_bucket'], recursive=True)
    return [o.object_name for o in objects]


def get_ddf(path):
    settings = minio.get_s3_settings()

    minio_path = f"s3://{settings['default_bucket']}/{path}"
    del settings['default_bucket']

    ddf = dd.read_csv(
        minio_path, 
        sample_rows=1000, # Sample 1000 rows to auto-determine dtypes
        blocksize=25e6, # 25MB per block
        header=0, 
        engine="python", 
        encoding="utf8", 
        quotechar='"', 
        escapechar='\\', 
        # on_bad_lines='warn', # For some reason Dask doesn't like this keyword parameter all of a sudden, even though it is supported!
        storage_options=settings, # Use s3 style paths for minio
    ) 

    return ddf


def get_unique_values(ddf):
    ddf = ddf.select_dtypes(exclude=['number']) # Drop numerics, no need to search these in ES

    # This might still cause memory issues for very large DFs
    # TODO: Dump to disk and read from there
    func = lambda s: s.unique().compute()

    futures = dask.get_client().map(func, [ddf[col] for col in ddf.columns])
    results = dask.get_client().gather(futures)

    return dict(zip(ddf.columns, results))
