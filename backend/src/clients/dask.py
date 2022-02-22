from dask.distributed import Client

dask_client = None

def get_client():
    global dask_client
    if dask_client==None:
        dask_client = Client(threads_per_worker=4, n_workers=8) # TODO: make configurable
    return dask_client
