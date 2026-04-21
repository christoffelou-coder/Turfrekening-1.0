import sys
import os

# Voeg project root toe aan Python path zodat Flask app.py vindbaar is
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import serverless_wsgi
from app import app as flask_app


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
