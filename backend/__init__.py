import os

from celery import Celery
from flask import Flask
from flask_cors import CORS
from flask_restx import Api

# Flask configuration
app = Flask(__name__)
app.debug = False if os.environ["DAISY_PRODUCTION"] == "true" else True
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
                include=['backend.utility.celery_tasks'],
                task_track_started=True,  # Makes sure task status changes to "STARTED"
                result_extended=True  # Also stores args, task name, task children, etc into backend
                )

# Important to set, otherwise weird race-conditions with backend retrieval will occur
# See: https://stackoverflow.com/questions/26527214/why-celery-current-app-refers-the-default-instance-inside-flask-view-functions
# And: https://stackoverflow.com/questions/54205149/django-celery-asyncresult-attributeerror-disabledbackend-object-has-no-attrib
celery.set_default()

CORS(app)
