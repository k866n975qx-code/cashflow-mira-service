# api/db.py
import os, psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set; create .env or export it")

# tolerate SQLAlchemy-style scheme
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

def get_conn():
    return psycopg.connect(DB_URL)