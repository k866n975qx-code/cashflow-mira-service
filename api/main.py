from fastapi import FastAPI, Request
import hashlib
from api.routers.sync import router as sync_router
from api.routers.transactions import router as txn_router
from api.routers.categories import router as cat_router
from api.routers.budgets import router as budget_router
from api.routers.cashflow import router as cashflow_router
from api.routers.bills import router as bills_router
from api.routers.balances import router as balances_router
from api.routers.ef import router as ef_router
from api.routers.breakdown import router as breakdown_router
from api.routers.snapshot import router as snapshot_router

app = FastAPI(title="Cashflow Minimal")

app.include_router(sync_router)
app.include_router(txn_router)
app.include_router(cat_router)
app.include_router(budget_router)
app.include_router(cashflow_router)
app.include_router(bills_router)
app.include_router(balances_router)
app.include_router(ef_router)
app.include_router(breakdown_router)
app.include_router(snapshot_router)

@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    """
    Add deterministic cache headers for all successful GET responses.
    ETag is a weak hash of (path + query), stable for the same request.
    """
    response = await call_next(request)

    # Only cache GETs with 200 OK
    if request.method == "GET" and response.status_code == 200:
        # Don't override if something already set headers explicitly
        if "cache-control" not in (k.lower() for k in response.headers.keys()):
            response.headers["Cache-Control"] = "public, max-age=3600"

        if "etag" not in (k.lower() for k in response.headers.keys()):
            cache_key = f"{request.url.path}?{request.url.query}"
            etag = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
            # Weak ETag since it's not content-derived
            response.headers["ETag"] = f'W/"{etag}"'

    return response
