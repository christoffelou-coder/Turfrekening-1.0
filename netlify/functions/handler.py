import sys
import os

# Packages worden door de build geïnstalleerd in dezelfde map als dit bestand
# (pip install -t netlify/functions -r requirements.txt)
# Python vindt ze automatisch via de function directory in sys.path

import serverless_wsgi
from app import app as flask_app


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
