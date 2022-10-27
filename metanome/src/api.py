from flask import Flask
import os
import logging
import object_methods as mn
from pathlib import Path
import requests
import time
import json

logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)


# Flask init
app = Flask(__name__)
app.debug = True
app.secret_key = os.urandom(24)


METANOME_ADDRESS = os.environ["METANOME_ADDRESS"]
METANOME_DATA_PATH = os.environ["METANOME_DATA_PATH"]



@app.route('/')
def index():
    return "HELLO"


@app.route("/run_binder/<path:bucket>")
def run_binder(bucket):
    # Need to make sure to delete existing inputs
    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/file-inputs").json()
    for inp in inputs:
        requests.delete(f"http://{METANOME_ADDRESS}/api/file-inputs/delete/{inp['id']}")

    path = Path(METANOME_DATA_PATH) / bucket
    requests.post(f"http://{METANOME_ADDRESS}/api/file-inputs/get-directory-files", json=mn.create_file_input(str(path)))  # This one makes a whole bunch of inputs, doesn't return anything useful, so performing a get is better


    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/file-inputs").json()


    binder_execution = mn.create_binder_execution([mn.convert_file_input_to_execution(inp) for inp in inputs])
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
