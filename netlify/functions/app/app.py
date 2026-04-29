import sys
import os
import importlib.util

# Voeg project root toe aan path voor models, calculations, sheets_sync
root_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, os.path.abspath(root_dir))

# Laad root app.py via importlib om naming conflict te vermijden
# (dit bestand heet ook app.py, dus gewone import zou zichzelf importeren)
_root_app_path = os.path.join(os.path.abspath(root_dir), 'app.py')
_spec = importlib.util.spec_from_file_location("flask_root_app", _root_app_path)
_module = importlib.util.module_from_spec(_spec)
sys.modules['flask_root_app'] = _module
_spec.loader.exec_module(_module)

flask_app = _module.app

import serverless_wsgi


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
