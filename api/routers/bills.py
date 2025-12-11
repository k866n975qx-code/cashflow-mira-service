from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import date, datetime, timedelta
import calendar
from api.db import get_conn

router = APIRouter(prefix="/bills", tags=["bills"])

Freq = Literal["weekly","monthly","yearly"]

class BillCreate(BaseModel):
    id: str = Field(..., description="your id, e.g. 'rent'")
    name: str
    amount: float
    currency: str = "USD"
    frequency: Freq
    weekday: Optional[int] = Field(None, ge=0, le=6, description="0=Mon..6=Sun for weekly")
    day_of_month: Optional[int] = Field(None, ge=1, le=31, description="for monthly/yearly")
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class BillPatch(BaseModel):
    name: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    frequency: Optional[Freq] = None
    weekday: Optional[int] = Field(None, ge=0, le=6)
    day_of_month: Optional[int] = Field(None, ge=1, le=31)
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class ContribBody(BaseModel):
    amount: float
    note: Optional[str] = None

# ---------- helpers ----------
def _row_to_bill(r):
    cols = ["id","name","amount","currency","frequency","weekday","day_of_month","start_date","end_date"]
    d = dict(zip(cols, r))
    # convert dates
    if d["start_date"] and isinstance(d["start_date"], datetime):
        d["start_date"] = d["start_date"].date()
    if d["end_date"] and isinstance(d["end_date"], datetime):
        d["end_date"] = d["end_date"].date()
    return d

def _last_dom(y, m):
    return calendar.monthrange(y, m)[1]

def _prev_month(y: int, m: int):
    # wrap correctly: if January (1), go to previous year December (12)
    return (y - 1, 12) if m == 1 else (y, m - 1)

def _previous_due_date(bill, due: date) -> date:
    """
    Previous scheduled due date BEFORE `due`:
    - weekly: due - 7 days
    - monthly: same day_of_month in previous month (clamped to last DOM)
    - yearly: same month/day in previous year (clamped)
    Respects start_date if it lands after the computed previous due.
    """
    if bill["frequency"] == "weekly":
        return due - timedelta(days=7)

    if bill["frequency"] == "monthly":
        day = bill.get("day_of_month")
        if not day:
            return due.replace(day=1)  # fallback
        py, pm = _prev_month(due.year, due.month)
        dom = min(day, _last_dom(py, pm))
        prev_due = date(py, pm, dom)
        # respect start_date if set and later than prev_due
        if bill.get("start_date") and bill["start_date"] > prev_due:
            return bill["start_date"]
        return prev_due

    # yearly
    anchor = bill.get("start_date") or due.replace(month=1, day=1)
    py = due.year - 1
    dom = min(anchor.day, _last_dom(py, anchor.month))
    prev_due = date(py, anchor.month, dom)
    if bill.get("start_date") and bill["start_date"] > prev_due:
        return bill["start_date"]
    return prev_due

def _next_due_date(bill, ref: date) -> date:
    """
    Compute the NEXT scheduled due date ON/AFTER `ref`.
    """
    if bill["frequency"] == "weekly":
        # next occurrence of weekday at or after ref
        wd = bill.get("weekday")
        if wd is None:
            return ref
        delta = (wd - ref.weekday()) % 7
        return ref + timedelta(days=delta)

    if bill["frequency"] == "monthly":
        dom = bill.get("day_of_month") or 1
        # same month if DOM >= today, else next month
        y, m = ref.year, ref.month
        last = _last_dom(y, m)
        d = date(y, m, min(dom, last))
        if d < ref:
            # move to next month
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
            d = date(y, m, min(dom, _last_dom(y, m)))
        # respect end_date if set
        if bill.get("end_date") and d > bill["end_date"]:
            return bill["end_date"]
        # respect start_date if set and ref is before it
        if bill.get("start_date") and d < bill["start_date"]:
            return bill["start_date"]
        return d

    # yearly
    anchor = bill.get("start_date") or date(ref.year, 1, 1)
    # try this year on same month/day (clamped)
    last = _last_dom(ref.year, anchor.month)
    d = date(ref.year, anchor.month, min(anchor.day, last))
    if d < ref:
        # next year
        ny = ref.year + 1
        d = date(ny, anchor.month, min(anchor.day, _last_dom(ny, anchor.month)))
    if bill.get("end_date") and d > bill["end_date"]:
        return bill["end_date"]
    if bill.get("start_date") and d < bill["start_date"]:
        return bill["start_date"]
    return d

def _monthly_due_dates(day: int, start: date, end: date):
    y,m = start.year, start.month
    while date(y,m,1) <= end:
        dom = min(day, _last_dom(y,m))
        d = date(y,m,dom)
        if d >= start and d <= end:
            yield d
        # next month
        if m == 12: y, m = y+1, 1
        else: m += 1

def _weekly_due_dates(weekday: int, start: date, end: date):
    # weekday: 0=Mon..6=Sun
    days_ahead = (weekday - start.weekday()) % 7
    first = start + timedelta(days=days_ahead)
    d = first
    while d <= end:
        yield d
        d += timedelta(days=7)

def _yearly_due_dates(anchor: date, start: date, end: date):
    # anchor month/day defines due; yearly on the anniversary
    y = max(start.year, anchor.year)
    while True:
        try:
            d = date(y, anchor.month, anchor.day)
        except ValueError:
            # handle Feb 29 → Feb 28 in non-leap years
            d = date(y, anchor.month, min(anchor.day, _last_dom(y, anchor.month)))
        if d > end: break
        if d >= start: yield d
        y += 1

def _period_start_for(bill, due: date) -> date:
    # Period starts at the PREVIOUS due date, so contributions between
    # previous_due (inclusive) and this due (inclusive) count toward progress.
    return _previous_due_date(bill, due)

def _ledger_sums(bill_id: str, start: date, end: date):
    """
    Return (contributions_sum, paid_sum_as_positive) within [start..end].
    Contributions are ONLY positives; payments are counted as positive magnitudes of negatives.
    """
    start_ts = datetime.combine(start, datetime.min.time())
    end_ts = datetime.combine(end, datetime.max.time())
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS contrib,
              COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0) AS paid
            FROM bill_ledger
            WHERE bill_id=%s AND occurred_at >= %s AND occurred_at <= %s
            """,
            (bill_id, start_ts, end_ts)
        )
        contrib, paid = cur.fetchone()
    return float(contrib or 0), float(paid or 0)

# ---------- CRUD ----------
@router.get("")
def list_bills():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date FROM bills ORDER BY name")
        return [_row_to_bill(r) for r in cur.fetchall()]

@router.post("", status_code=201)
def create_bill(body: BillCreate):
    if body.frequency == "weekly" and body.weekday is None:
        raise HTTPException(400, "weekday required for weekly")
    if body.frequency == "monthly" and body.day_of_month is None:
        raise HTTPException(400, "day_of_month required for monthly")
    if body.frequency == "yearly" and body.start_date is None:
        raise HTTPException(400, "start_date required for yearly")

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO bills (id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT (id) DO NOTHING
        """, (body.id, body.name, body.amount, body.currency, body.frequency, body.weekday, body.day_of_month, body.start_date, body.end_date))
        created = (cur.rowcount == 1)
        conn.commit()
    return {"ok": True, "id": body.id, "created": created}

@router.patch("/{id}")
def patch_bill(id: str, body: BillPatch):
    fields, args = [], []
    for k in ["name","amount","currency","frequency","weekday","day_of_month","start_date","end_date"]:
        v = getattr(body, k)
        if v is not None:
            fields.append(f"{k}=%s"); args.append(v)
    if not fields:
        return {"ok": True}
    args.append(id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE bills SET {', '.join(fields)} WHERE id=%s", args)
        if cur.rowcount == 0:
            raise HTTPException(404, "not found")
        conn.commit()
    return {"ok": True}

@router.delete("/{id}", status_code=204)
def delete_bill(id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bills WHERE id=%s", (id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "not found")
        conn.commit()
    return

# ---------- Ledger ops ----------
@router.post("/{id}/contribute")
def contribute(id: str, body: ContribBody):
    if body.amount <= 0:
        raise HTTPException(400, "amount must be > 0")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO bill_ledger (bill_id, amount, note) VALUES (%s,%s,%s)", (id, body.amount, body.note))
        conn.commit()
    return {"ok": True}

@router.post("/{id}/mark-paid")
def mark_paid(id: str):
    """
    Idempotent: if already paid for the current period, do nothing and report already_paid=true.
    Current period is [previous_due .. next_due].
    """
    today = date.today()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date FROM bills WHERE id=%s", (id,))
        r = cur.fetchone()
        if not r:
            raise HTTPException(404, "bill not found")
        bill = _row_to_bill(r)
        amt = float(bill["amount"])

        next_due = _next_due_date(bill, today)
        period_start = _previous_due_date(bill, next_due)
        contrib, paid_sum = _ledger_sums(id, period_start, next_due)

        if paid_sum >= amt - 1e-6:
            return {"ok": True, "already_paid": True, "period_start": period_start.isoformat(), "due": next_due.isoformat()}

        # insert a negative amount equal to bill amount
        cur.execute("INSERT INTO bill_ledger (bill_id, amount, note) VALUES (%s,%s,%s)", (id, -amt, "paid"))
        conn.commit()
    return {"ok": True, "already_paid": False, "period_start": period_start.isoformat(), "due": next_due.isoformat()}

# ---------- Occurrences ----------
@router.get("/occurrences")
def occurrences(
    date_from: date = Query(..., alias="from"),
    date_to:   date = Query(..., alias="to"),
    due_soon_days: int = Query(14, ge=1, le=60)
):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id,name,amount,currency,frequency,weekday,day_of_month,start_date,end_date FROM bills")
        bills = [_row_to_bill(r) for r in cur.fetchall()]

    out = []
    for b in bills:
        # restrict by bill start/end window
        s = max(date_from, b["start_date"]) if b["start_date"] else date_from
        e = min(date_to, b["end_date"]) if b["end_date"] else date_to
        if s > e:
            continue

        if b["frequency"] == "weekly":
            if b["weekday"] is None: continue
            dates = _weekly_due_dates(b["weekday"], s, e)
        elif b["frequency"] == "monthly":
            if b["day_of_month"] is None: continue
            dates = _monthly_due_dates(b["day_of_month"], s, e)
        else:  # yearly
            if not b["start_date"]: continue
            anchor = b["start_date"]
            dates = _yearly_due_dates(anchor, s, e)

        for due in dates:
            period_start = _period_start_for(b, due)
            contrib, paid_amt = _ledger_sums(b["id"], period_start, due)
            amount = float(b["amount"])
            paid = paid_amt >= amount - 1e-6
            # progress is full if paid entry exists; otherwise based on contributions only
            progress = 1.0 if paid else max(0.0, min(1.0, contrib / amount))
            days_to_due = (due - date.today()).days
            soon_weight = 0.0
            if days_to_due <= due_soon_days:
                soon_weight = max(0.0, 1.0 - max(days_to_due,0) / due_soon_days)
            score = 0.6 * soon_weight + 0.4 * (1.0 - progress)

            out.append({
                "bill_id": b["id"],
                "name": b["name"],
                "due": due.isoformat(),
                "amount": amount,
                "progress_pct": round(progress * 100, 2),
                "paid": paid,
                "saved_so_far": round(contrib, 2),
                "contrib_sum": round(contrib, 2),
                "paid_sum": round(paid_amt, 2),
                "days_to_due": days_to_due,
                "due_soon": days_to_due <= due_soon_days,
                "priority_score": round(score, 4),
            })

    # sort: highest priority first (due soon + low progress)
    out.sort(key=lambda x: (x["due_soon"], x["priority_score"]), reverse=True)
    return out

@router.get("/{id}/ledger")
def bill_ledger(id: str,
                date_from: date = Query(..., alias="from"),
                date_to: date = Query(..., alias="to")):
    """
    Inspect raw ledger rows for a bill and see summed contributions vs payments.
    """
    start_ts = datetime.combine(date_from, datetime.min.time())
    end_ts   = datetime.combine(date_to, datetime.max.time())
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT occurred_at, amount, COALESCE(note,'')
            FROM bill_ledger
            WHERE bill_id=%s AND occurred_at >= %s AND occurred_at <= %s
            ORDER BY occurred_at
            """,
            (id, start_ts, end_ts)
        )
        rows = cur.fetchall()
    contrib_sum = sum(float(a) for (_, a, _) in rows if float(a) > 0)
    paid_sum    = sum(-float(a) for (_, a, _) in rows if float(a) < 0)
    return {
        "bill_id": id,
        "from": date_from.isoformat(),
        "to": date_to.isoformat(),
        "contrib_sum": round(contrib_sum, 2),
        "paid_sum": round(paid_sum, 2),
        "entries": [
            {"occurred_at": ts.isoformat(), "amount": float(a), "note": n} for (ts, a, n) in rows
        ]
    }
