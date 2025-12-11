# Cashflow Minimal Service

Small FastAPI service that ingests Lunch Money data (read-only) and provides local cashflow controls.

## Quickstart (macOS)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

cp .env.example .env
# edit .env with your DB url and LM token

# create DB schema
psql "postgresql://cashflow:cashflow@127.0.0.1:5432/cashflow" -f db/schema.sql

# run API
uvicorn api.main:app --host 127.0.0.1 --port 8040
```

### First sync
```bash
curl -X POST "http://127.0.0.1:8040/sync?days=90"
```

### Health
```bash
curl http://127.0.0.1:8040/healthz
```

### OpenAPI (for GPT Action)
Host `openapi.json` in the repo and point your Action to the raw URL, or serve `/openapi.json` if you wire it to FastAPI.
