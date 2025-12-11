from fastapi import APIRouter
from api.db import get_conn

router = APIRouter(prefix="/balances", tags=["balances"])

@router.get("/snapshot")
def snapshot():
    """
    Totals + per-account latest balances.
    """
    with get_conn() as conn, conn.cursor() as cur:
        # latest row per account using DISTINCT ON
        cur.execute("""
          SELECT a.id, a.name, a.provider, a.type, a.subtype, a.is_liquid,
                 ab.balance, ab.currency, ab.as_of
          FROM accounts a
          JOIN (
            SELECT DISTINCT ON (account_id) account_id, balance, currency, as_of
            FROM account_balances
            ORDER BY account_id, as_of DESC
          ) ab ON ab.account_id = a.id
          ORDER BY a.name
        """)
        rows = cur.fetchall()

    accounts = []
    liquid_total = 0.0
    non_liquid_total = 0.0
    for (aid, name, provider, atype, subtype, is_liquid, bal, ccy, as_of) in rows:
        bal = float(bal)
        if is_liquid:
            liquid_total += bal
        else:
            non_liquid_total += bal
        accounts.append({
            "id": aid,
            "name": name,
            "provider": provider,
            "type": atype,
            "subtype": subtype,
            "is_liquid": bool(is_liquid),
            "balance": bal,
            "currency": ccy,
            "as_of": as_of.isoformat()
        })

    return {
        "liquid_total": round(liquid_total, 2),
        "non_liquid_total": round(non_liquid_total, 2),
        "accounts": accounts
    }
