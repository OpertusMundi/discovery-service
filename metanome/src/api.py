from flask import Flask
import os
import logging
import object_methods as mn
from pathlib import Path
import requests
import time

logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)


# Flask init
app = Flask(__name__)
app.debug = True
app.secret_key = os.urandom(24)


MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_DEFAULT_BUCKET = os.environ["MINIO_DEFAULT_BUCKET"]
METANOME_ADDRESS = os.environ["METANOME_ADDRESS"]


@app.route('/')
def index():
    return "HELLO"


@app.route("/run_binder")
def run_binder():
    minio_connection = mn.create_minio_connection(
        f"http://{MINIO_ADDRESS}", 
        MINIO_ROOT_USER, 
        MINIO_ROOT_PASSWORD
    )
    res_minio_connection = requests.post(f"http://{METANOME_ADDRESS}/api/minio-connections/store", json=minio_connection).json()

    # Need to make sure to delete existing inputs
    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/minio-inputs").json()
    for inp in inputs:
        requests.delete(f"http://{METANOME_ADDRESS}/api/minio-inputs/delete/{inp['id']}")

    # This one makes a whole bunch of inputs, doesn't return anything useful, so performing a get is better
    minio_input = mn.create_minio_input("", MINIO_DEFAULT_BUCKET, res_minio_connection)
    requests.post(f"http://{METANOME_ADDRESS}/api/minio-inputs/bucket", json=minio_input)
    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/minio-inputs").json()


    binder_execution = mn.create_binder_execution([mn.convert_minio_input_to_execution(inp) for inp in inputs])
    requests.post(f"http://{METANOME_ADDRESS}/api/algorithm-execution", json=binder_execution)

    # Watch the results dir
    result_path = Path("/metanome/results")
    path = None
    while path == None:
        time.sleep(1)
        paths = list(result_path.iterdir())
        if len(paths) > 0:
            path = paths[0]

    results = ""
    with open(path, 'r') as file:
        results = file.read()

    for path in result_path.iterdir():
        path.unlink()

    return results


app.run(host='0.0.0.0', port=443)
