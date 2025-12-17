from fastapi import APIRouter, Query
from datetime import date, datetime, timedelta
from typing import List, Dict
import math
from api.db import get_conn
# reuse bill helpers
from api.routers.bills import (
    _row_to_bill,
    _weekly_due_dates,
    _monthly_due_dates,
    _yearly_due_dates,
    _period_start_for,
    _ledger_sums,
)

router = APIRouter(prefix="/snapshot", tags=["snapshot"])
DEFAULT_LATEST_DAYS = 7


def _safe_amount(val: float) -> float:
    if val is None or not math.isfinite(val):
        return 0.0
    return val

def _money(value) -> float:
    """
    Normalize any numeric to a 2-decimal-place float for currency.
    """
    try:
        return round(float(value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0

def _extract_net_cashflow(snapshot: dict) -> float:
    """
    Extracts net cashflow from the actual schema:
    snapshot["cashflow"]["net"]

    Falls back to 0.0 if missing or null.
    """
    cf = (snapshot or {}).get("cashflow") or {}
    try:
        return float(cf.get("net") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _extract_ef_amount(snapshot: dict) -> float:
    """
    Safely extract EF amount from a snapshot.
    Adjust keys here if your EF schema differs.
    """
    ef = (snapshot or {}).get("ef") or {}
    for key in ("current_amount", "balance", "amount", "liquid"):
        if key in ef and ef[key] is not None:
            try:
                return float(ef[key])
            except (TypeError, ValueError):
                continue
    return 0.0


def _pct_change(curr: float, prev: float) -> float | None:
    """
    Percentage change from prev -> curr, rounded to 2 decimals.
    Returns None when prev is 0 to avoid division issues.
    """
    if prev == 0:
        return None
    return round(((curr - prev) / abs(prev)) * 100.0, 2)

def _bill_status(due: date, paid: bool) -> str:
    """
    Derive a simple status enum for a bill.

    active   = upcoming or due today, not paid yet
    overdue  = due date in the past, not paid
    paid     = marked as paid
    (archived is reserved for future use)
    """
    today = date.today()
    if paid:
        return "paid"
    if due < today:
        return "overdue"
    return "active"

def _category_type(is_income: bool, is_transfer: bool) -> str:
    """
    Map category flags to a type enum usable by GPT/clients.
    """
    if is_transfer:
        return "transfer"
    if is_income:
        return "income"
    return "expense"

@router.get("")
def full_snapshot(
    date_from: date = Query(..., alias="from"),
    date_to:   date = Query(..., alias="to"),
):
    """
    Expanded snapshot for a period [from..to]:
      - cashflow totals + by-category (affects_cashflow=true) + top outflow payees
      - budgets vs actual (only if from/to are same month)
      - bill occurrences within range (with progress)
      - balances (latest) and EF overview (global)
    """
    # --- CASHFLOW (totals + by-category + top payees) ---
    with get_conn() as conn, conn.cursor() as cur:
        # totals (respect ignored + category flags); treat expenses that LM encodes as positive as outflow too
        cur.execute("""
          WITH src AS (
            SELECT t.amount,
                   COALESCE(c.affects_cashflow, true) AS affects_cashflow,
                   COALESCE(c.is_income, false) AS is_income
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.ignored = false
              AND t.date_posted >= %s AND t.date_posted <= %s
          )
          SELECT
            -- inflow prefers income categories; if none flagged, fallback to all positives
            COALESCE(
              NULLIF(SUM(CASE WHEN is_income AND amount > 0 THEN amount ELSE 0 END), 0),
              SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),
              0
            ) AS inflow,
            -- outflow as positive magnitude:
            COALESCE(SUM(CASE
              WHEN affects_cashflow AND amount < 0 THEN -amount
              WHEN affects_cashflow AND amount > 0 AND NOT is_income THEN amount
              ELSE 0 END), 0) AS outflow_mag
          FROM src
        """, (date_from, date_to))
        inflow_val, outflow_mag_val = cur.fetchone()
        inflow = _safe_amount(float(inflow_val or 0.0))
        outflow_mag = _safe_amount(float(outflow_mag_val or 0.0))  # positive magnitude
        net = _safe_amount(inflow - outflow_mag)

        # by-category (only affects_cashflow=true)
        cur.execute("""
          SELECT COALESCE(c.id, '_uncategorized') AS category_id,
                 COALESCE(c.name, 'Uncategorized') AS category_name,
                 COALESCE(c.is_income, false) AS is_income,
                 false AS is_transfer,
                 COALESCE(SUM(t.amount), 0) AS total
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          WHERE t.ignored = false
            AND (c.affects_cashflow IS NULL OR c.affects_cashflow = true)
            AND t.date_posted >= %s AND t.date_posted <= %s
          GROUP BY COALESCE(c.id, '_uncategorized'),
                   COALESCE(c.name, 'Uncategorized'),
                   COALESCE(c.is_income, false)
          ORDER BY COALESCE(c.name, 'Uncategorized')
        """, (date_from, date_to))
        by_cat = []
        for (cid, cname, is_income, is_transfer, total) in cur.fetchall():
            cat_type = _category_type(bool(is_income), bool(is_transfer))
            by_cat.append({
                "category_id": cid,
                "category_name": cname,
                "type": cat_type,
                "total": _money(total),
            })

        # top outflow payees (affects_cashflow=true, negatives only)
        cur.execute("""
          SELECT COALESCE(t.payee, 'Unknown') AS payee,
                 SUM(CASE
                      WHEN (c.affects_cashflow IS NULL OR c.affects_cashflow = true) AND t.amount < 0 THEN -t.amount
                      WHEN (c.affects_cashflow IS NULL OR c.affects_cashflow = true) AND t.amount > 0 AND COALESCE(c.is_income,false) = false THEN t.amount
                      ELSE 0 END) AS spend
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          WHERE t.ignored = false
            AND t.date_posted >= %s AND t.date_posted <= %s
          GROUP BY payee
          HAVING SUM(CASE
                      WHEN (c.affects_cashflow IS NULL OR c.affects_cashflow = true) AND t.amount < 0 THEN -t.amount
                      WHEN (c.affects_cashflow IS NULL OR c.affects_cashflow = true) AND t.amount > 0 AND COALESCE(c.is_income,false) = false THEN t.amount
                      ELSE 0 END) > 0
          ORDER BY spend DESC
          LIMIT 5
        """, (date_from, date_to))
        top_payees = [{"payee": r[0], "spend": float(r[1] or 0)} for r in cur.fetchall()]

    cashflow = {
        "inflow": _money(inflow),
        "outflow": _money(outflow_mag),   # positive magnitude of spend
        "net": _money(net),               # inflow - outflow
        "by_category": by_cat,
        "top_outflow_payees": top_payees,
    }

    # --- BUDGETS vs ACTUAL (only if same month) ---
    budgets = None
    if date_from.year == date_to.year and date_from.month == date_to.month:
        year, month = date_from.year, date_from.month
        month_start = date(year, month, 1)
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
          WITH elig AS (
            SELECT id, name, COALESCE(is_income, false) AS is_income, false AS is_transfer
            FROM categories
            WHERE affects_cashflow = true AND budgetable = true
          ),
          b AS (
            SELECT category_id, amount
            FROM budgets
            WHERE year = %s AND month = %s
          ),
          a AS (
            SELECT t.category_id AS category_id,
                   COALESCE(SUM(CASE
                     WHEN t.amount < 0 THEN -t.amount
                     WHEN t.amount > 0 AND COALESCE(c.is_income,false) = false THEN t.amount
                     ELSE 0 END), 0) AS actual_spend
            FROM transactions t
            JOIN elig e ON e.id = t.category_id
            JOIN categories c ON c.id = t.category_id
            WHERE t.ignored = false
              AND t.date_posted >= %s
              AND t.date_posted < %s
            GROUP BY t.category_id
          )
          SELECT e.id, e.name, e.is_income, e.is_transfer, COALESCE(b.amount, 0) AS budgeted, COALESCE(a.actual_spend, 0) AS actual
          FROM b
          JOIN elig e ON e.id = b.category_id
          LEFT JOIN a ON a.category_id = b.category_id
          ORDER BY e.name
        """, (year, month, month_start, next_month))
            rows = cur.fetchall()
            budgets = [{
                "category_id": r[0],
                "category_name": r[1],
                "type": _category_type(bool(r[2]), bool(r[3])),
                "budgeted": _money(r[4]),
                "actual": _money(r[5]),
                "variance": _money(_money(r[4]) - _money(r[5])),
            } for r in rows]

    # --- BILLS occurrences in range (with progress) ---
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date FROM bills")
        bills = [_row_to_bill(r) for r in cur.fetchall()]

    bill_items: List[Dict] = []
    for b in bills:
        # clamp bill active window to requested range
        s = max(date_from, b["start_date"]) if b["start_date"] else date_from
        e = min(date_to, b["end_date"]) if b["end_date"] else date_to
        if s > e:
            continue
        # expand by frequency
        if b["frequency"] == "weekly":
            if b["weekday"] is None:
                continue
            dates = _weekly_due_dates(b["weekday"], s, e)
        elif b["frequency"] == "monthly":
            if b["day_of_month"] is None:
                continue
            dates = _monthly_due_dates(b["day_of_month"], s, e)
        else:  # yearly
            if not b["start_date"]:
                continue
            dates = _yearly_due_dates(b["start_date"], s, e)

        for due in dates:
            period_start = _period_start_for(b, due)
            contrib, paid_sum = _ledger_sums(b["id"], period_start, due)
            amt = float(b["amount"])
            paid = paid_sum >= amt - 1e-6
            progress = 1.0 if paid else (0.0 if amt <= 0 else min(1.0, contrib / amt))
            paid_flag = bool(paid)
            bill_items.append({
                "bill_id": b["id"],
                "name": b["name"],
                "due": due.isoformat(),
                "amount": _money(amt),
                "progress_pct": round(progress * 100, 2),
                "paid": paid_flag,
                "contrib_sum": _money(contrib),
                "paid_sum": _money(paid_sum),
                "status": _bill_status(due, paid_flag),
            })

    # --- BALANCES (latest + change %) ---
    with get_conn() as conn, conn.cursor() as cur:
        # latest row per account + previous balance (for change %)
        cur.execute("""
          WITH ranked AS (
            SELECT
              account_id,
              balance,
              currency,
              as_of,
              ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY as_of DESC) AS rn,
              LAG(balance) OVER (PARTITION BY account_id ORDER BY as_of DESC) AS prev_balance
            FROM account_balances
          )
          SELECT
            a.id,
            a.name,
            a.provider,
            a.type,
            a.subtype,
            a.is_liquid,
            r.balance,
            r.currency,
            r.as_of,
            r.prev_balance
          FROM accounts a
          JOIN ranked r ON r.account_id = a.id
          WHERE r.rn = 1
          ORDER BY a.name
        """)
        rows = cur.fetchall()

    accounts = []
    liquid_total = 0.0
    non_liquid_total = 0.0
    for (aid, name, provider, atype, subtype, is_liquid, bal, ccy, as_of, prev_bal) in rows:
        bal = float(bal)
        prev_val = float(prev_bal) if prev_bal is not None else None

        if is_liquid:
            liquid_total += bal
        else:
            non_liquid_total += bal

        change_pct = None
        if prev_val is not None and prev_val != 0:
            change_pct = round(((bal - prev_val) / abs(prev_val)) * 100.0, 2)

        accounts.append({
            "id": aid,
            "name": name,
            "provider": provider,
            "type": atype,
            "subtype": subtype,
            "is_liquid": bool(is_liquid),
            "balance": _money(bal),
            "currency": ccy,
            "as_of": as_of.isoformat(),
            "balance_change_pct": change_pct,
        })

    balances = {
        "liquid_total": _money(liquid_total),
        "non_liquid_total": _money(non_liquid_total),
        "accounts": accounts,
    }

    # --- EF (global; not tied to period) ---
    # reuse same queries as /ef to avoid import cycle
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          WITH t AS (
            SELECT t.amount, COALESCE(c.affects_cashflow, true) AS affects_cashflow
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.ignored = false
              AND t.date_posted >= (date_trunc('month', CURRENT_DATE) - INTERVAL '3 months')
              AND t.date_posted <  date_trunc('month', CURRENT_DATE)
          )
          SELECT COALESCE(SUM(CASE WHEN t.affects_cashflow AND t.amount < 0 THEN -t.amount ELSE 0 END), 0)
          FROM t
        """)
        outflow_3m = float(cur.fetchone()[0] or 0.0)
        baseline = outflow_3m / 3.0
        target = baseline * 3.0
        cur.execute("""
          SELECT SUM(x.balance) FROM (
            SELECT DISTINCT ON (a.id) a.id, a.is_liquid, ab.balance
            FROM accounts a
            JOIN account_balances ab ON ab.account_id = a.id
            WHERE a.is_liquid = true
            ORDER BY a.id, ab.as_of DESC
          ) x
        """)
        liquid = float(cur.fetchone()[0] or 0.0)
        cur.execute("""
          WITH src AS (
            SELECT t.amount, COALESCE(c.affects_cashflow, true) AS affects_cashflow,
                   COALESCE(c.is_income, false) AS is_income
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.ignored = false
              AND t.date_posted >= date_trunc('month', CURRENT_DATE)
              AND t.date_posted <  (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')
          )
          SELECT
            COALESCE(NULLIF(SUM(CASE WHEN is_income AND affects_cashflow AND amount > 0 THEN amount ELSE 0 END), 0),
                     SUM(CASE WHEN affects_cashflow AND amount > 0 THEN amount ELSE 0 END)) AS monthly_income
          FROM src
        """)
        monthly_income = float(cur.fetchone()[0] or 0.0)
    ef = {
        "baseline_monthly_outflow": round(baseline, 2),
        "target": round(target, 2),
        "liquid": round(liquid, 2),
        "funded_pct": round(100.0 if target <= 0 else min(100.0, (liquid/target)*100.0), 2),
        "monthly_income": round(monthly_income, 2),
    }

    payload = {
        "range": { "from": date_from.isoformat(), "to": date_to.isoformat() },
        "cashflow": cashflow,
        "budgets": budgets,
        "bills": bill_items,
        "balances": balances,
        "ef": ef,
    }

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    payload["meta"] = {
        "served_from": "db",
        "cache_age_seconds": 0,
        "snapshot_created_at": now_iso,
    }
    return payload


@router.get("/latest")
def latest_snapshot(
    days: int = Query(
        DEFAULT_LATEST_DAYS,
        ge=1,
        le=365,
        description="Number of trailing days to include (inclusive of today)",
    ),
):
    """
    Convenience endpoint: latest full snapshot for the last N days (default 7).
    """
    today = date.today()
    date_to = today
    date_from = today - timedelta(days=days - 1)
    snapshot = full_snapshot(date_from=date_from, date_to=date_to)

    # attach meta
    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    snapshot["meta"] = {
        "served_from": "db",
        "cache_age_seconds": 0,
        "snapshot_created_at": now_iso,
    }
    return snapshot


@router.get("/full")
def full_snapshot_full(
    date_from: date = Query(..., alias="from"),
    date_to:   date = Query(..., alias="to"),
):
    """
    Explicit alias for the full snapshot endpoint.
    Returns the same shape as GET /snapshot, with a meta block attached.
    """
    snapshot = full_snapshot(date_from=date_from, date_to=date_to)
    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    snapshot["meta"] = {
        "served_from": "db",
        "cache_age_seconds": 0,
        "snapshot_created_at": now_iso,
    }
    return snapshot


@router.get("/diff")
def snapshot_diff(
    date_from: date = Query(..., alias="from"),
    date_to:   date = Query(..., alias="to"),
):
    """
    Compare the current period [from, to] against the previous period
    of equal length. Returns net cashflow + EF deltas.
    """
    if date_to < date_from:
        raise ValueError("to must be on or after from")

    current = full_snapshot(date_from=date_from, date_to=date_to)

    days_span = (date_to - date_from).days + 1
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=days_span - 1)

    previous = full_snapshot(date_from=prev_from, date_to=prev_to)

    curr_net = _extract_net_cashflow(current)
    prev_net = _extract_net_cashflow(previous)

    curr_ef = _extract_ef_amount(current)
    prev_ef = _extract_ef_amount(previous)

    net_diff_abs = _money(curr_net - prev_net)
    ef_diff_abs = _money(curr_ef - prev_ef)

    net_diff_pct = _pct_change(curr_net, prev_net)
    ef_diff_pct = _pct_change(curr_ef, prev_ef)

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    return {
        "current": {
            "range": {
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
            },
            "net_cashflow": curr_net,
            "ef_amount": curr_ef,
        },
        "previous": {
            "range": {
                "from": prev_from.isoformat(),
                "to": prev_to.isoformat(),
            },
            "net_cashflow": prev_net,
            "ef_amount": prev_ef,
        },
        "diff": {
            "net_cashflow": {
                "absolute": net_diff_abs,
                "pct": net_diff_pct,
            },
            "ef_amount": {
                "absolute": ef_diff_abs,
                "pct": ef_diff_pct,
            },
        },
        "meta": {
            "served_from": "db",
            "cache_age_seconds": 0,
            "snapshot_created_at": now_iso,
        },
    }
