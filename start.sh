#!/bin/bash
cd "$(dirname "$0")"
echo "🍺 Turfrekening starten..."
export FLASK_DEBUG=1
python3 -m flask --app app db upgrade && python3 app.py
