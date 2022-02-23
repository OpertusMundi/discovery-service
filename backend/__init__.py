import os

from flask import Flask
from celery import Celery
from flask_cors import CORS


# Flask configuration
app = Flask(__name__)
app.debug = True
app.secret_key = os.urandom(24)

# Celery configuration
celery = Celery(app.name)
celery.conf.update(broker_url=f"amqp://{os.environ['RABBITMQ_DEFAULT_USER']}:"
                                  f"{os.environ['RABBITMQ_DEFAULT_PASS']}@"
                                  f"{os.environ['RABBITMQ_HOST']}:"
                                  f"{os.environ['RABBITMQ_PORT']}/")

CORS(app)

