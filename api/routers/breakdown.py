from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal, List, Dict
from datetime import date, datetime, timedelta
from api.db import get_conn
from api.routers.bills import _row_to_bill, _next_due_date, _previous_due_date, _ledger_sums  # reuse helpers

router = APIRouter(prefix="/breakdown", tags=["breakdown"])

class BreakdownBody(BaseModel):
    amount: float = Field(..., ge=0, description="New inflow to allocate")
    reserve_cushion: float = Field(200, ge=0)
    due_soon_days: int = Field(14, ge=1, le=60)
    year: Optional[int] = None   # budgets year (default: current)
    month: Optional[int] = None  # budgets month (1-12; default: current)

@router.post("")
def plan_breakdown(body: BreakdownBody):
    """
    Allocate new inflow in this order:
      1) Bills due within <due_soon_days>, sorted by urgency & low progress
      2) Budgets (month): fund gaps for categories (affects_cashflow & budgetable)
      3) Emergency Fund: up to need, capped at 5% of monthly income
      4) Leftover (plus reserve cushion kept aside)
    """
    # guard and setup
    total = float(body.amount or 0)
    reserve = min(float(body.reserve_cushion or 0), total)
    remaining = max(0.0, total - reserve)
    today = date.today()
    y = body.year or today.year
    m = body.month or today.month
    first_day = date(y, m, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    window_end = today + timedelta(days=body.due_soon_days)

    allocations_bills: List[Dict] = []
    allocations_budgets: List[Dict] = []
    alloc_ef = 0.0

    # ---------- 1) Bills due soon ----------
    bills = []
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date FROM bills")
        bills = [_row_to_bill(r) for r in cur.fetchall()]
    scored = []
    for b in bills:
        due = _next_due_date(b, today)
        if due < today or due > window_end:
            continue
        period_start = _previous_due_date(b, due)
        contrib, paid_sum = _ledger_sums(b["id"], period_start, due)
        amt = float(b["amount"])
        paid = paid_sum >= amt - 1e-6
        if paid:
            continue
        need = max(0.0, amt - contrib)
        days_to_due = (due - today).days
        soon_weight = max(0.0, 1.0 - days_to_due / body.due_soon_days)
        progress = 0.0 if amt <= 0 else min(1.0, contrib / amt)
        score = 0.6 * soon_weight + 0.4 * (1.0 - progress)
        if need > 0:
            scored.append({
                "bill_id": b["id"], "name": b["name"], "due": due, "need": need, "score": score
            })
    scored.sort(key=lambda x: x["score"], reverse=True)
    for item in scored:
        if remaining <= 0:
            break
        take = min(item["need"], remaining)
        if take <= 0:
            continue
        allocations_bills.append({
            "to": f"bill:{item['bill_id']}",
            "name": item["name"],
            "due": item["due"].isoformat(),
            "amount": round(take, 2)
        })
        remaining -= take

    # ---------- 2) Budgets (fund gaps) ----------
    with get_conn() as conn, conn.cursor() as cur:
        # Pull budgets for the month (only eligible categories)
        cur.execute("""
          WITH elig AS (
            SELECT id, name FROM categories
            WHERE affects_cashflow = true AND budgetable = true
          ),
          b AS (
            SELECT category_id, amount
            FROM budgets
            WHERE year = %s AND month = %s
          ),
          a AS (
            SELECT t.category AS category_id,
                   COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS actual_spend
            FROM transactions t
            JOIN elig e ON e.id = t.category
            WHERE t.ignored = false
              AND t.date_posted >= %s
              AND t.date_posted < %s
            GROUP BY t.category
          )
          SELECT e.id, e.name, COALESCE(b.amount, 0) AS budgeted, COALESCE(a.actual_spend, 0) AS actual
          FROM b
          JOIN elig e ON e.id = b.category_id
          LEFT JOIN a ON a.category_id = b.category_id
          ORDER BY e.name
        """, (y, m, first_day, next_month))
        rows = cur.fetchall()
    budget_needs = []
    for cid, cname, budgeted, actual in rows:
        need = max(0.0, float(budgeted) - float(actual))
        if need > 0:
            budget_needs.append({"category_id": cid, "name": cname, "need": need})
    # simple biggest-need-first
    budget_needs.sort(key=lambda x: x["need"], reverse=True)
    for item in budget_needs:
        if remaining <= 0:
            break
        take = min(item["need"], remaining)
        allocations_budgets.append({
            "to": f"budget:{item['category_id']}",
            "name": item["name"],
            "amount": round(take, 2)
        })
        remaining -= take

    # ---------- 3) EF (cap at 5% of monthly income) ----------
    with get_conn() as conn, conn.cursor() as cur:
        # baseline/target
        cur.execute("""
          WITH t AS (
            SELECT t.amount, COALESCE(c.affects_cashflow, true) AS affects_cashflow
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category
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

        # liquid balances
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
        need = max(0.0, target - liquid)

        # monthly income (prefer categories marked is_income)
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

    cap = monthly_income * 0.05
    ef_want = min(cap, need)
    if remaining > 0 and ef_want > 0:
        alloc_ef = round(min(remaining, ef_want), 2)
        remaining -= alloc_ef

    # ---------- response ----------
    to_bills = round(sum(x["amount"] for x in allocations_bills), 2)
    to_budgets = round(sum(x["amount"] for x in allocations_budgets), 2)
    return {
        "input_amount": round(total, 2),
        "reserve_cushion": round(reserve, 2),
        "allocated": {
            "bills": allocations_bills,
            "budgets": allocations_budgets,
            "ef": alloc_ef
        },
        "summary": {
            "to_bills": to_bills,
            "to_budgets": to_budgets,
            "to_ef": alloc_ef,
            "unallocated": round(remaining + 0.0, 2)
        }
    }
