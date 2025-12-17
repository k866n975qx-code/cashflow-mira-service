from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import date
from pydantic import BaseModel
from api.db import get_conn

router = APIRouter(prefix="/transactions", tags=["transactions"])

# -------- models --------
class TxnPatch(BaseModel):
    category: Optional[str] = None
    ignored: Optional[bool] = None
    note: Optional[str] = None
    payee: Optional[str] = None


class TxnOut(BaseModel):
    lm_id: int
    date_posted: date
    amount: float
    currency: str
    payee: Optional[str] = None
    note: Optional[str] = None
    category: Optional[str] = None
    ignored: bool


class BatchTxnPatchItem(TxnPatch):
    lm_id: int


class BatchTxnPatchRequest(BaseModel):
    operations: list[BatchTxnPatchItem]

# -------- helpers --------
def row_to_dict(row):
    cols = ["lm_id","date_posted","amount","currency","payee","note","category","ignored"]
    data = dict(zip(cols, row))
    amt = data.get("amount")
    data["amount"] = float(amt) if amt is not None else None
    return data

# -------- endpoints --------
IGNORED_EXPR = "COALESCE(CASE WHEN c.affects_cashflow IS NOT NULL THEN NOT c.affects_cashflow END, t.ignored)"

@router.get("", response_model=list[TxnOut])
def list_transactions(
    date_from: Optional[date] = Query(None, alias="from"),
    date_to: Optional[date] = Query(None, alias="to"),
    ignored: Optional[bool] = None,
    category: Optional[str] = None,
    uncategorized: Optional[bool] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[TxnOut]:
    sql = f"""SELECT t.lm_id, t.date_posted, t.amount::float, t.currency, t.payee, t.note, t.category,
                     {IGNORED_EXPR} AS ignored
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category
              WHERE 1=1"""
    args = []
    if date_from:
        sql += " AND t.date_posted >= %s"; args.append(date_from)
    if date_to:
        sql += " AND t.date_posted <= %s"; args.append(date_to)
    if ignored is not None:
        sql += f" AND {IGNORED_EXPR} = %s"; args.append(ignored)
    cat_norm = category.lower() if isinstance(category, str) else None
    if uncategorized is True or cat_norm in ("null", "_uncategorized"):
        sql += " AND (t.category IS NULL OR t.category = '')"
    elif uncategorized is False:
        sql += " AND (t.category IS NOT NULL AND t.category <> '')"
    elif category is not None:
        sql += " AND LOWER(t.category) = %s"; args.append(cat_norm or "")
    sql += " ORDER BY t.date_posted DESC, t.lm_id DESC LIMIT %s OFFSET %s"
    args.extend([limit, offset])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
    return [TxnOut(**row_to_dict(r)) for r in rows] if rows else []


@router.get("/uncategorized", response_model=list[TxnOut], summary="List uncategorized transactions")
def list_uncategorized_transactions(
    ignored: Optional[bool] = None,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[TxnOut]:
    """
    Convenience endpoint to fetch only uncategorized transactions (category is NULL/empty).
    Mirrors list_transactions semantics, including ignored filtering.
    """
    sql = f"""SELECT t.lm_id, t.date_posted, t.amount::float, t.currency, t.payee, t.note, t.category,
                     {IGNORED_EXPR} AS ignored
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category
              WHERE (t.category IS NULL OR t.category = '')"""
    args = []
    if ignored is not None:
        sql += f" AND {IGNORED_EXPR} = %s"; args.append(ignored)
    sql += " ORDER BY t.date_posted DESC, t.lm_id DESC LIMIT %s OFFSET %s"
    args.extend([limit, offset])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
    return [TxnOut(**row_to_dict(r)) for r in rows] if rows else []

@router.get("/{lm_id}")
def get_transaction(lm_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT t.lm_id, t.date_posted, t.amount::float, t.currency, t.payee, t.note, t.category,
                        {IGNORED_EXPR} AS ignored
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category
               WHERE t.lm_id = %s""",
            (lm_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Not found")
    return row_to_dict(row)

@router.patch("/{lm_id}")
def patch_transaction(lm_id: int, body: TxnPatch):
    # fetch current
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT lm_id, date_posted, amount::float, currency, payee, note, category, ignored
                       FROM transactions WHERE lm_id = %s""", (lm_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        current = row_to_dict(row)

        updates = {}
        category_override_ignored = None

        # category drives ignored: affects_cashflow=false => ignored=true; affects_cashflow=true => ignored=false
        if body.category is not None and body.category != current.get("category"):
            updates["category"] = body.category
            cur.execute("SELECT affects_cashflow FROM categories WHERE id=%s", (body.category,))
            cat_row = cur.fetchone()
            affects_cashflow = cat_row[0] if cat_row is not None else True
            category_override_ignored = not affects_cashflow

        for field in ("ignored","note","payee"):
            val = getattr(body, field)
            if val is not None and val != current.get(field):
                updates[field] = val

        if category_override_ignored is not None and category_override_ignored != current.get("ignored"):
            updates["ignored"] = category_override_ignored

        if not updates:
            return current  # nothing to change

        # build update
        set_sql = ", ".join([f"{k} = %s" for k in updates.keys()])
        args = list(updates.values()) + [lm_id]
        cur.execute(f"UPDATE transactions SET {set_sql} WHERE lm_id = %s", args)

        # write minimal change log
        for k, new_val in updates.items():
            old_val = current.get(k)
            cur.execute(
                "INSERT INTO transaction_changes (lm_id, field, old_value, new_value) VALUES (%s,%s,%s,%s)",
                (lm_id, k, str(old_val) if old_val is not None else None, str(new_val) if new_val is not None else None)
            )

        conn.commit()

        # return updated row
        cur.execute(
            f"""SELECT t.lm_id, t.date_posted, t.amount::float, t.currency, t.payee, t.note, t.category,
                        {IGNORED_EXPR} AS ignored
                 FROM transactions t
                 LEFT JOIN categories c ON c.id = t.category
                WHERE t.lm_id = %s""",
            (lm_id,),
        )
        return row_to_dict(cur.fetchone())


@router.post("/batch", summary="Batch patch transactions (category, ignored, note, payee)")
def batch_patch_transactions(body: BatchTxnPatchRequest):
    """
    Apply category / ignored / note / payee updates to many transactions at once.
    Mirrors the single-transaction patch semantics, including transaction_changes logging.
    """
    cols = ["lm_id", "date_posted", "amount", "currency", "payee", "note", "category", "ignored"]
    results = []

    with get_conn() as conn, conn.cursor() as cur:
        for op in body.operations:
            lm_id = op.lm_id

            # load current row
            cur.execute(
                """SELECT lm_id, date_posted, amount::float, currency, payee, note, category, ignored
                   FROM transactions WHERE lm_id = %s""",
                (lm_id,),
            )
            row = cur.fetchone()
            if not row:
                # skip missing; could accumulate errors if desired
                continue

            current = row_to_dict(row)

            # determine changes
            updates = {}
            category_override_ignored = None

            if op.category is not None and op.category != current.get("category"):
                updates["category"] = op.category
                cur.execute("SELECT affects_cashflow FROM categories WHERE id=%s", (op.category,))
                cat_row = cur.fetchone()
                affects_cashflow = cat_row[0] if cat_row is not None else True
                category_override_ignored = not affects_cashflow

            for field in ("ignored", "note", "payee"):
                new_val = getattr(op, field)
                if new_val is not None and new_val != current.get(field):
                    updates[field] = new_val

            if category_override_ignored is not None and category_override_ignored != current.get("ignored"):
                updates["ignored"] = category_override_ignored

            if not updates:
                results.append(current)
                continue

            # apply update
            set_sql = ", ".join(f"{k} = %s" for k in updates.keys())
            args = list(updates.values()) + [lm_id]
            cur.execute(f"UPDATE transactions SET {set_sql} WHERE lm_id = %s", args)

            # log changes
            for k, new_val in updates.items():
                old_val = current.get(k)
                cur.execute(
                    "INSERT INTO transaction_changes (lm_id, field, old_value, new_value) VALUES (%s,%s,%s,%s)",
                    (
                        lm_id,
                        k,
                        str(old_val) if old_val is not None else None,
                        str(new_val) if new_val is not None else None,
                    ),
                )

            # fetch updated row
            cur.execute(
                f"""SELECT t.lm_id, t.date_posted, t.amount::float, t.currency, t.payee, t.note, t.category,
                            {IGNORED_EXPR} AS ignored
                       FROM transactions t
                       LEFT JOIN categories c ON c.id = t.category
                      WHERE t.lm_id = %s""",
                (lm_id,),
            )
            updated = cur.fetchone()
            results.append(row_to_dict(updated))

        conn.commit()

    return results
