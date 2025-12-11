from fastapi import APIRouter
from api.db import get_conn
from datetime import date

router = APIRouter(prefix="/ef", tags=["ef"])

@router.get("")
def ef_overview():
    """
    Emergency fund: baseline (avg last 3 months outflow), target = 3x baseline,
    liquid balance (sum latest liquid accounts), funded %, and recommended contribution
    capped at 5% of monthly income.
    """
    with get_conn() as conn, conn.cursor() as cur:
        # Sum negative amounts (outflow) last 3 months, respecting category flags & ignored
        cur.execute("""
          WITH t AS (
            SELECT t.amount, t.date_posted, COALESCE(c.affects_cashflow, true) AS affects_cashflow
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category
            WHERE t.ignored = false
              AND t.date_posted >= (date_trunc('month', CURRENT_DATE) - INTERVAL '3 months')
              AND t.date_posted <  date_trunc('month', CURRENT_DATE)
          )
          SELECT
            COALESCE(SUM(CASE WHEN t.affects_cashflow AND t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS outflow_3m
          FROM t
        """)
        outflow_3m = float(cur.fetchone()[0] or 0.0)

        baseline = outflow_3m / 3.0  # average per month
        target = baseline * 3.0

        # liquid balances: latest snapshot per liquid account
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

        # monthly income inflow (current calendar month)
        # prefer categories where is_income=true; fallback to all positives
        cur.execute("""
          WITH src AS (
            SELECT t.amount, COALESCE(c.affects_cashflow, true) AS affects_cashflow,
                   COALESCE(c.is_income, false) AS is_income
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category
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

    need = max(0.0, target - liquid)
    cap = monthly_income * 0.05
    recommended = min(cap, need)
    funded_pct = 100.0 if target <= 0 else min(100.0, (liquid / target) * 100.0)

    return {
        "baseline_monthly_outflow": round(baseline, 2),
        "target": round(target, 2),
        "liquid": round(liquid, 2),
        "funded_pct": round(funded_pct, 2),
        "monthly_income": round(monthly_income, 2),
        "recommended_contribution": round(recommended, 2)
    }
