from fastapi import APIRouter, HTTPException, Query
from api.db import get_conn

router = APIRouter(prefix="/cashflow", tags=["cashflow"])

@router.get("/budget-vs-actual")
def budget_vs_actual(year: int = Query(..., ge=2000, le=2100),
                     month: int = Query(..., ge=1, le=12)):
    """
    Only includes categories that HAVE a budget row for the given month.
    Enforces category flags: affects_cashflow=true AND budgetable=true.
    Actuals sum transactions.ignored=false AND transactions.category = categories.id.
    """
    sql = """
    WITH b AS (
      SELECT category_id, amount
      FROM budgets
      WHERE year=%s AND month=%s
    ),
    t AS (
      SELECT t.category AS category_id,
             COALESCE(SUM(CASE
               WHEN t.amount < 0 THEN -t.amount
               WHEN t.amount > 0 AND COALESCE(c.is_income,false) = false THEN t.amount
               ELSE 0 END), 0) AS actual
      FROM transactions t
      JOIN categories c ON c.id = t.category
      WHERE EXTRACT(YEAR FROM t.date_posted) = %s
        AND EXTRACT(MONTH FROM t.date_posted) = %s
        AND t.ignored = false
        AND c.affects_cashflow = true
        AND c.budgetable = true
      GROUP BY t.category
    )
    SELECT b.category_id,
           c.name,
           b.amount AS budgeted,
           COALESCE(t.actual, 0) AS actual,
           (b.amount - COALESCE(t.actual, 0)) AS variance
    FROM b
    JOIN categories c ON c.id = b.category_id
    LEFT JOIN t ON t.category_id = b.category_id
    WHERE c.affects_cashflow = true AND c.budgetable = true
    ORDER BY c.name;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (year, month, year, month))
        rows = cur.fetchall()
    return [
        {
            "category_id": r[0],
            "category_name": r[1],
            "budgeted": float(r[2]),
            "actual": float(r[3]),
            "variance": float(r[4]),
        }
        for r in rows
    ]
