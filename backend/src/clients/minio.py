import os

from minio import Minio


minio_client = None

def get_client():
    global minio_client
    if minio_client==None:
        settings = get_s3_settings()
        minio_client = Minio(
            settings['client_kwargs']['endpoint_url'].split('//')[-1],
            access_key=settings['key'],
            secret_key=settings['secret'],
            secure=False
        )
    return minio_client

def get_s3_settings():
    return {
      "key": os.environ["MINIO_ROOT_USER"],
      "secret": os.environ["MINIO_ROOT_PASSWORD"],
      "client_kwargs": {"endpoint_url":f"http://{os.environ['MINIO_ADDRESS']}"},
      "default_bucket": os.environ['MINIO_DEFAULT_BUCKET']
    }