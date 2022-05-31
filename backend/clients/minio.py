import os

from minio import Minio


minio_client: Minio = Minio(f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}",
                            access_key=os.environ['MINIO_ACCESS_KEY'],
                            secret_key=os.environ['MINIO_SECRET_KEY'],
                            secure=False)
