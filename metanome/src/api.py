from flask import Flask, Response
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


METANOME_ADDRESS = os.environ["METANOME_ADDRESS"]
METANOME_DATA_PATH = os.environ["METANOME_DATA_PATH"]
METANOME_TIMEOUT = int(os.environ["METANOME_TIMEOUT"])


@app.route('/')
def index():
    return "HELLO"


@app.route("/run_binder")
def run_binder():
    # Need to make sure to delete existing inputs
    logging.info("Cleaning existing inputs")
    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/file-inputs").json()
    for inp in inputs:
        requests.delete(
            f"http://{METANOME_ADDRESS}/api/file-inputs/delete/{inp['id']}")

    root = Path(METANOME_DATA_PATH)
    for p in root.iterdir():
        if p.is_dir():
            resource_path = p / 'resources'
            logging.info(f"Adding {resource_path} to Metanome's DB")
            # This one makes a whole bunch of inputs, doesn't return anything useful, so performing a get is better
            requests.post(f"http://{METANOME_ADDRESS}/api/file-inputs/get-directory-files", json=mn.create_file_input(str(resource_path)))

    inputs = requests.get(f"http://{METANOME_ADDRESS}/api/file-inputs").json()
    logging.info("Available input files:")
    for inp in inputs:
        logging.info(inp)

    binder_execution = mn.create_binder_execution(
        [mn.convert_file_input_to_execution(inp) for inp in inputs])
    requests.post(
        f"http://{METANOME_ADDRESS}/api/algorithm-execution", json=binder_execution)

    # Watch the results dir
    counter = 0
    result_path = Path("/metanome/results")
    path = None
    logging.info("Waiting for Binder results...")
    while path is None and counter <= METANOME_TIMEOUT:
        time.sleep(1)
        counter += 1
        paths = list(result_path.iterdir())
        if len(paths) > 0:
            path = paths[0]

    results = ""
    if counter <= METANOME_TIMEOUT:
        with open(path, 'r') as file:
            results = file.read()

        for path in result_path.iterdir():
            path.unlink()

        return results
    else:
        logging.warning("Timeout exceeded for Binder results file")
        return Response("Metanome result file timeout", status=500)


app.run(host='0.0.0.0', port=443)
