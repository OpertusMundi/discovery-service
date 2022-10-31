import json
import uuid
from pathlib import Path


def load_json_template(name):
    script_dir = Path(__file__).parent
    json_path = (script_dir / "json" / f"{name}.json").resolve()
    with open(json_path, 'r') as json_file:
        template = json_file.read()
        return json.loads(template)


def create_minio_connection(address, key, secret_key):
    obj = load_json_template("minio_connection")
    obj["url"] = address
    obj["key"] = key
    obj["secretKey"] = secret_key
    return obj


def create_minio_input(object_name, bucket_name, minio_connection):
    obj = load_json_template("minio_input")
    obj["objectName"] = object_name
    obj["bucketName"] = bucket_name
    obj["minIOConnection"] = minio_connection
    return obj


def create_file_input(path):
    obj = load_json_template("file_input")
    obj["fileName"] = path
    obj["name"] = path
    return obj


def create_binder_execution(input_list, memory=1000):
    obj = load_json_template("binder_execution")
    obj["executionIdentifier"] = str(uuid.uuid4())
    for requirement in obj["requirements"]:
        if requirement["type"] == "ConfigurationRequirementRelationalInput":
            requirement["settings"] = input_list
    obj["memory"] = str(memory)
    return obj


def convert_minio_input_to_execution(obj_minio_input):
    exec_minio_input = load_json_template("execution_minio_input")
    exec_minio_connection = load_json_template("execution_minio_connection")

    obj_minio_connection = obj_minio_input["minIOConnection"]

    exec_minio_connection["url"] = obj_minio_connection["url"]
    exec_minio_connection["key"] = obj_minio_connection["key"]
    exec_minio_connection["secretKey"] = obj_minio_connection["secretKey"]

    exec_minio_input["object"] = obj_minio_input["objectName"]
    exec_minio_input["bucket"] = obj_minio_input["bucketName"]
    exec_minio_input["minIOConnection"] = exec_minio_connection

    return exec_minio_input


def convert_file_input_to_execution(obj_file_input):
    exec_file_input = load_json_template("execution_file_input")
    exec_file_input["fileName"] = obj_file_input["fileName"]
    return exec_file_input
