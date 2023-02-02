import json
import logging
import sys

import pika
import requests

logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.ERROR)

sys.path.append("..")


def callback(ch, method, properties, body):
    # Expects JSON --> convenient if we need to add additional stuff
    data = json.loads(body)
    logging.info("GOT SOMETHING: ", data)

    try:
        table_name = "/".join(data["Key"].split("/")[1:])
        requests.get(f"http://localhost:443/addtable/{table_name}")
        logging.info(f"Made GET request to API for table {table_name}")
    except KeyError as e:
        pass
        logging.error(f"Queue object does not conform to expected MinIO `put` event format: {e}")


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="rabbitmq", port="5672")
)
channel = connection.channel()
queue_name = "ingestion_queue"
exchange_name = "bucketevents"
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange=exchange_name, queue=queue_name)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
logging.info('Queue listener is up!')
channel.start_consuming()
