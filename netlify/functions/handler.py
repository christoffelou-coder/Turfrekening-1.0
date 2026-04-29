import sys
import os
import importlib.util

# Zoek site-packages op meerdere plekken (Lambda pad is niet altijd hetzelfde)
_base = os.path.dirname(os.path.abspath(__file__))
for _path in [
    os.path.join(_base, 'site-packages'),
    os.path.join(_base, '..', '..', 'site-packages'),
    '/var/task/site-packages',
    '/var/task/netlify/functions/site-packages',
]:
    if os.path.exists(_path):
        sys.path.insert(0, _path)

# Voeg project root toe voor app.py, models.py etc.
for _root in [
    os.path.join(_base, '..', '..'),
    '/var/task',
]:
    if os.path.exists(os.path.join(_root, 'app.py')):
        sys.path.insert(0, _root)
        break

import serverless_wsgi
from app import app as flask_app


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
