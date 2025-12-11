from fastapi import APIRouter, HTTPException, Query
from datetime import date, timedelta, datetime
import os, httpx
from api.db import get_conn

router = APIRouter(prefix="", tags=["sync"])

LM_BASE = "https://dev.lunchmoney.app/v1"
LM_TOKEN = os.environ.get("LM_API_TOKEN")

@router.post("/sync")
def sync(days: int = Query(1, ge=1),  # use ?days=90 once for initial backfill; timer can call days=1
         start: date | None = None,
         end: date | None = None):
    if not LM_TOKEN:
        raise HTTPException(500, "LM_API_TOKEN not set")

    if not start or not end:
        end = date.today()
        start = end - timedelta(days=days)

    headers = {"Authorization": f"Bearer {LM_TOKEN}"}
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}

    with httpx.Client(timeout=30) as client:
        r = client.get(f"{LM_BASE}/transactions", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        items = data.get("transactions") or data.get("data") or []

        # also pull accounts for balances (plaid + assets)
        try:
            r_acc = client.get(f"{LM_BASE}/plaid_accounts", headers=headers)
            r_acc.raise_for_status()
            plaid_accounts = r_acc.json().get("plaid_accounts") or r_acc.json().get("data") or []
        except Exception:
            plaid_accounts = []
        try:
            r_assets = client.get(f"{LM_BASE}/assets", headers=headers)
            r_assets.raise_for_status()
            assets = r_assets.json().get("assets") or r_assets.json().get("data") or []
        except Exception:
            assets = []

    inserted_lm = 0
    inserted_local = 0
    upserted_accounts = 0
    snapshots = 0

    sql_upsert_lm = """
    INSERT INTO lm_transactions (lm_id, date_posted, amount, currency, payee, note, account_id, plaid_account_id)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (lm_id) DO UPDATE SET
      date_posted=EXCLUDED.date_posted,
      amount=EXCLUDED.amount,
      currency=EXCLUDED.currency,
      payee=EXCLUDED.payee,
      note=EXCLUDED.note,
      account_id=EXCLUDED.account_id,
      plaid_account_id=EXCLUDED.plaid_account_id;
    """

    sql_insert_local = """
    INSERT INTO transactions (lm_id, date_posted, amount, currency, payee, note)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (lm_id) DO NOTHING;
    """

    sql_upsert_account = """
    INSERT INTO accounts (id, name, provider, type, subtype, is_liquid)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE SET
      name=EXCLUDED.name,
      provider=EXCLUDED.provider,
      type=EXCLUDED.type,
      subtype=EXCLUDED.subtype
    """
    sql_insert_balance = """
    INSERT INTO account_balances (account_id, balance, currency)
    VALUES (%s,%s,%s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            for tx in items:
                lm_id = tx.get("id") or tx.get("transaction_id")
                if lm_id is None:
                    continue
                dt = (tx.get("date_posted") or tx.get("date") or "")[:10]
                amt = float(tx.get("amount"))
                ccy = (tx.get("currency") or "USD")[:3].upper()
                payee = tx.get("payee") or tx.get("merchant")
                note = tx.get("notes") or tx.get("note")
                account_id = str(tx.get("asset_id") or tx.get("account_id") or "")
                plaid_account_id = str(tx.get("plaid_account_id") or "")

                cur.execute(sql_upsert_lm, (lm_id, dt, amt, ccy, payee, note, account_id, plaid_account_id))
                # rowcount is 1 for INSERT, 0 for UPDATE in psycopg3's text protocol; we don't rely on it here
                inserted_lm += 1

                cur.execute(sql_insert_local, (lm_id, dt, amt, ccy, payee, note))
                if cur.rowcount == 1:
                    inserted_local += 1

            # Upsert Plaid accounts (checking/savings/credit), mark liquid only for depository checking/savings
            for a in plaid_accounts:
                acc_id = str(a.get("plaid_account_id") or a.get("id") or a.get("account_id") or "")
                name = a.get("name") or a.get("official_name") or a.get("mask") or "Plaid Account"
                atype = (a.get("type") or "").lower()         # e.g., 'depository', 'credit', 'investment'
                subtype = (a.get("subtype") or "").lower()    # e.g., 'checking', 'savings'
                is_liquid = bool(atype == "depository" and subtype in ("checking","savings"))
                cur.execute(sql_upsert_account, (acc_id, name, "plaid", atype, subtype, is_liquid))
                upserted_accounts += 1
                # snapshot balance if present
                bal = a.get("balance_current") or a.get("current_balance") or a.get("balance")
                if bal is not None:
                    cur.execute(sql_insert_balance, (acc_id, float(bal), (a.get("currency") or "USD")[:3].upper()))
                    snapshots += 1

            # Upsert manual assets (non-liquid by default)
            for a in assets:
                acc_id = str(a.get("id") or a.get("asset_id") or "")
                if not acc_id:
                    continue
                name = a.get("name") or a.get("display_name") or "Asset"
                atype = (a.get("type_name") or a.get("type") or "").lower()   # LM may use type_name
                subtype = (a.get("subtype") or "")[:50].lower()
                is_liquid = False  # per your rule: manual/asset entries not liquid by default
                cur.execute(sql_upsert_account, (acc_id, name, "asset", atype, subtype, is_liquid))
                upserted_accounts += 1
                bal = a.get("balance") or a.get("value")
                if bal is not None:
                    cur.execute(sql_insert_balance, (acc_id, float(bal), (a.get("currency") or "USD")[:3].upper()))
                    snapshots += 1

        conn.commit()

    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "fetched": len(items),
        "upserted_into_lm_transactions": len(items),
        "inserted_into_transactions": inserted_local,
        "accounts_processed": upserted_accounts,
        "balance_snapshots": snapshots
    }
