from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from api.db import get_conn

router = APIRouter(prefix="/budgets", tags=["budgets"])

class BudgetUpsert(BaseModel):
    amount: float

def budget_row(r):
    cols = ["category_id", "category_name", "year", "month", "amount"]
    return dict(zip(cols, r))

@router.get("")
def list_budgets(year: int = Query(..., ge=2000, le=2100),
                 month: int = Query(..., ge=1, le=12)):
    sql = """
      SELECT b.category_id, c.name, b.year, b.month, b.amount
      FROM budgets b
      JOIN categories c ON c.id=b.category_id
      WHERE b.year=%s AND b.month=%s
      ORDER BY c.name
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (year, month))
        return [budget_row(r) for r in cur.fetchall()]

@router.put("/{category_id}/{year}-{month}")
def upsert_budget(category_id: str, year: int, month: int, body: BudgetUpsert):
    # enforce category exists and is eligible (affects_cashflow && budgetable)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT affects_cashflow, budgetable FROM categories WHERE id=%s", (category_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(400, "category does not exist")
        affects_cashflow, budgetable = row
        if not (affects_cashflow and budgetable):
            raise HTTPException(400, "category not eligible for budgets (flags)")

        cur.execute("""
          INSERT INTO budgets (category_id, year, month, amount)
          VALUES (%s,%s,%s,%s)
          ON CONFLICT (category_id, year, month) DO UPDATE SET amount=EXCLUDED.amount
        """, (category_id, year, month, body.amount))
        conn.commit()
    return {"ok": True, "category_id": category_id, "year": year, "month": month, "amount": body.amount}