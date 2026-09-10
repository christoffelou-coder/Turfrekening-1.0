import sys
import os

# Python path setup voor Netlify
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, 'site-packages'))
sys.path.insert(0, '/var/task')

# Import Flask app
try:
    import serverless_wsgi
    from app import app as flask_app
    
    def handler(event, context):
        """Netlify serverless function handler"""
        return serverless_wsgi.handle_request(flask_app, event, context)
    
except Exception as e:
    print(f"Error importing Flask app: {e}")
    import traceback
    traceback.print_exc()
    
    def handler(event, context):
        return {
            'statusCode': 500,
            'body': f'Server error: {str(e)}'
        }
