import sys
import os
import importlib.util

# Vendor dir bevat pip-geïnstalleerde packages (Flask, SQLAlchemy, etc.)
vendor_dir = os.path.join(os.path.dirname(__file__), 'vendor')
if os.path.exists(vendor_dir):
    sys.path.insert(0, vendor_dir)

# Project root toevoegen voor models.py, calculations.py, sheets_sync.py
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, root_dir)

# Laad root app.py via importlib om naming conflict te vermijden
_root_app_path = os.path.join(root_dir, 'app.py')
_spec = importlib.util.spec_from_file_location("flask_root_app", _root_app_path)
_module = importlib.util.module_from_spec(_spec)
sys.modules['flask_root_app'] = _module
_spec.loader.exec_module(_module)

flask_app = _module.app

import serverless_wsgi


def handler(event, context):
    return serverless_wsgi.handle_request(flask_app, event, context)
