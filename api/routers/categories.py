from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from api.db import get_conn

router = APIRouter(prefix="/categories", tags=["categories"])

class CategoryCreate(BaseModel):
    id: str = Field(..., description="Your id, e.g. 'groceries'")
    name: str
    affects_cashflow: bool = True
    budgetable: bool = True
    is_income: bool = False

class CategoryPatch(BaseModel):
    name: Optional[str] = None
    affects_cashflow: Optional[bool] = None
    budgetable: Optional[bool] = None
    is_income: Optional[bool] = None

def row_to_dict(row):
    cols = ["id","name","affects_cashflow","budgetable","is_income"]
    return dict(zip(cols, row))

@router.get("", response_model=List[dict])
def list_categories():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,affects_cashflow,budgetable,is_income FROM categories ORDER BY name")
        return [row_to_dict(r) for r in cur.fetchall()]

@router.post("", status_code=201)
def create_category(body: CategoryCreate):
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO categories (id,name,affects_cashflow,budgetable,is_income) VALUES (%s,%s,%s,%s,%s)",
                (body.id, body.name, body.affects_cashflow, body.budgetable, body.is_income)
            )
        except Exception as e:
            conn.rollback()
            raise HTTPException(400, f"insert failed: {e}")
        conn.commit()
    return {"ok": True, "id": body.id}

@router.patch("/{id}")
def patch_category(id: str, body: CategoryPatch):
    fields = []
    args = []
    for k in ["name","affects_cashflow","budgetable","is_income"]:
        v = getattr(body, k)
        if v is not None:
            fields.append(f"{k}=%s")
            args.append(v)
    if not fields:
        return {"ok": True}
    args.append(id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE categories SET {', '.join(fields)} WHERE id=%s", args)
        if cur.rowcount == 0:
            raise HTTPException(404, "not found")
        conn.commit()
    return {"ok": True}

@router.delete("/{id}", status_code=204)
def delete_category(id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM categories WHERE id=%s", (id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "not found")
        conn.commit()
    return