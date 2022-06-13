import os

from celery import Celery
from flask import Flask
from flask_cors import CORS
from flask_restx import Api

# Flask configuration
app = Flask(__name__)
app.debug = True
app.secret_key = os.urandom(24)

# Swagger configuration
api = Api(app)

# Celery configuration
celery = Celery(app.name, broker_url=f"amqp://{os.environ['RABBITMQ_DEFAULT_USER']}:"
                                     f"{os.environ['RABBITMQ_DEFAULT_PASS']}@"
                                     f"{os.environ['RABBITMQ_HOST']}:"
                                     f"{os.environ['RABBITMQ_PORT']}/",
                backend=f"redis://:{os.environ['REDIS_PASSWORD']}@"
                        f"{os.environ['REDIS_HOST']}:"
                        f"{os.environ['REDIS_PORT']}/0",
                include=['backend.utility.celery_tasks'])

CORS(app)
