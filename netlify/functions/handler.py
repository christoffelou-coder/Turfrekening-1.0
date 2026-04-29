import sys
import os

# site-packages zitten naast dit bestand (geïnstalleerd door build command)
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, 'site-packages'))

# app.py, models.py etc. komen via included_files mee in de bundle
sys.path.insert(0, '/var/task')

import serverless_wsgi
from app import app as flask_app


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
