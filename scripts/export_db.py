"""Alleen-lezen export van alle tabellen naar backups/turfrekening_export_<tijd>.json.
Gebruik: .venv/bin/python scripts/export_db.py   (leest DATABASE_URL uit .env)"""
import datetime
import json
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))

url = os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1)
engine = create_engine(url)
insp = inspect(engine)
out = {}
with engine.connect() as conn:
    for table in insp.get_table_names():
        rows = [dict(r._mapping) for r in conn.execute(text(f'select * from "{table}"'))]
        out[table] = rows
        print(f"{table}: {len(rows)} rijen")

os.makedirs(os.path.join(ROOT, "backups"), exist_ok=True)
path = os.path.join(ROOT, "backups", "turfrekening_export_%s.json" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
json.dump(out, open(path, "w"), default=str)
print("export:", path, os.path.getsize(path), "bytes")
