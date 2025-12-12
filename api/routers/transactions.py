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


class BatchTxnPatchItem(TxnPatch):
    lm_id: int


class BatchTxnPatchRequest(BaseModel):
    operations: list[BatchTxnPatchItem]

# -------- helpers --------
def row_to_dict(row):
    cols = ["lm_id","date_posted","amount","currency","payee","note","category","ignored"]
    return dict(zip(cols, row))

# -------- endpoints --------
@router.get("")
def list_transactions(
    date_from: Optional[date] = Query(None, alias="from"),
    date_to: Optional[date] = Query(None, alias="to"),
    ignored: Optional[bool] = None,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    sql = """SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
             FROM transactions WHERE 1=1"""
    args = []
    if date_from:
        sql += " AND date_posted >= %s"; args.append(date_from)
    if date_to:
        sql += " AND date_posted <= %s"; args.append(date_to)
    if ignored is not None:
        sql += " AND ignored = %s"; args.append(ignored)
    sql += " ORDER BY date_posted DESC, lm_id DESC LIMIT %s OFFSET %s"
    args.extend([limit, offset])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
    return [row_to_dict(r) for r in rows]

@router.get("/{lm_id}")
def get_transaction(lm_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
                       FROM transactions WHERE lm_id = %s""", (lm_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Not found")
    return row_to_dict(row)

@router.patch("/{lm_id}")
def patch_transaction(lm_id: int, body: TxnPatch):
    # fetch current
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
                       FROM transactions WHERE lm_id = %s""", (lm_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        current = row_to_dict(row)

        updates = {}
        for field in ("category","ignored","note","payee"):
            val = getattr(body, field)
            if val is not None and val != current.get(field):
                updates[field] = val

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
        cur.execute("""SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
                       FROM transactions WHERE lm_id = %s""", (lm_id,))
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
                """SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
                   FROM transactions WHERE lm_id = %s""",
                (lm_id,),
            )
            row = cur.fetchone()
            if not row:
                # skip missing; could accumulate errors if desired
                continue

            current = dict(zip(cols, row))

            # determine changes
            updates = {}
            for field in ("category", "ignored", "note", "payee"):
                new_val = getattr(op, field)
                if new_val is not None and new_val != current.get(field):
                    updates[field] = new_val

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
                """SELECT lm_id, date_posted, amount, currency, payee, note, category, ignored
                   FROM transactions WHERE lm_id = %s""",
                (lm_id,),
            )
            updated = cur.fetchone()
            results.append(dict(zip(cols, updated)))

        conn.commit()

    return results
