# backend/stocktake.py
import csv
import io
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Body
from starlette.responses import StreamingResponse

from db import get_conn
from bin_import import parse_bin_locations_csv

router = APIRouter(prefix="/stocktake", tags=["stocktake"])


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_stocktake_tables():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stocktake_sessions (
                session_id TEXT PRIMARY KEY,
                label TEXT,
                created_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stocktake_items (
                session_id TEXT,
                product_code TEXT,
                description TEXT,
                barcode TEXT,
                quantity REAL,
                updated_by TEXT,
                updated_at TEXT,
                PRIMARY KEY (session_id, product_code)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bin_products (
                bin_code TEXT,
                product_code TEXT,
                description TEXT,
                baseline_qty REAL,
                is_main INTEGER DEFAULT 1,
                alt_index INTEGER DEFAULT 0,
                PRIMARY KEY (bin_code, product_code, is_main, alt_index)
            )
            """
        )

        conn.commit()
    finally:
        conn.close()


@router.get("/bins")
def list_bins():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT bin_code
            FROM bin_products
            WHERE bin_code IS NOT NULL AND TRIM(bin_code) <> ''
            ORDER BY bin_code
            """
        )
        bins = [r["bin_code"] for r in cur.fetchall()]

        # Also include existing session_ids from stocktake_sessions (in case bins not uploaded)
        cur.execute(
            """
            SELECT session_id FROM stocktake_sessions
            WHERE session_id IS NOT NULL AND TRIM(session_id) <> ''
            ORDER BY session_id
            """
        )
        sessions = [r["session_id"] for r in cur.fetchall()]

        merged = sorted(set(bins + sessions))
        return merged
    finally:
        conn.close()

@router.get("/bin_products")
def get_bin_products(bin_code: str = Query(...)):
    bin_code = (bin_code or "").strip()
    if not bin_code:
        raise HTTPException(status_code=400, detail="bin_code is required")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                bin_code,
                product_code,
                COALESCE(description, '') AS description,
                COALESCE(baseline_qty, 0) AS baseline_qty,
                COALESCE(is_main, 1) AS is_main,
                COALESCE(alt_index, 0) AS alt_index
            FROM bin_products
            WHERE bin_code = ?
            ORDER BY CAST(product_code AS INTEGER), product_code
            """,
            (bin_code,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

@router.post("/bins/upload")
async def upload_bins(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Upload a CSV file (.csv) for bin locations")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    records = parse_bin_locations_csv(content)
    if not records:
        raise HTTPException(status_code=400, detail="No bin records found in CSV (check headers/format)")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM bin_products")

        cur.executemany(
            """
            INSERT OR REPLACE INTO bin_products
            (bin_code, product_code, description, baseline_qty, is_main, alt_index)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["bin_code"],
                    r["product_code"],
                    r.get("description") or "",
                    r.get("baseline_qty"),
                    int(r.get("is_main", 1)),
                    int(r.get("alt_index", 0)),
                )
                for r in records
            ],
        )

        conn.commit()
        return {"ok": True, "rows": len(records)}
    finally:
        conn.close()


def _resolve_product(barcode: Optional[str], product_code: Optional[str]):
    barcode = (barcode or "").strip().replace(" ", "")
    product_code = (product_code or "").strip()

    conn = get_conn()
    cur = conn.cursor()
    try:
        if barcode:
            cur.execute("SELECT * FROM products WHERE barcode = ? LIMIT 1", (barcode,))
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r.get("barcode")

        if product_code:
            cur.execute("SELECT * FROM products WHERE product_code = ? LIMIT 1", (product_code,))
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r.get("barcode")

        if barcode.isdigit():
            cur.execute("SELECT * FROM products WHERE product_code = ? LIMIT 1", (barcode,))
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r.get("barcode")

        return (product_code or barcode or ""), "UNKNOWN", (barcode or None)
    finally:
        conn.close()


@router.post("/item")
def add_or_update_item(payload: dict = Body(...)):
    session_id = (payload.get("session_id") or "").strip()
    barcode = payload.get("barcode")
    product_code = payload.get("product_code")
    updated_by = (payload.get("updated_by") or "").strip() or None

    try:
        quantity = float(payload.get("quantity") or 0)
    except:
        quantity = 0.0

    if not session_id:
        raise HTTPException(status_code=400, detail="session_id is required")

    resolved_code, resolved_desc, resolved_barcode = _resolve_product(barcode=barcode, product_code=product_code)
    if not resolved_code:
        raise HTTPException(status_code=400, detail="Could not resolve product code")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO stocktake_sessions (session_id, label, created_at)
            VALUES (?, ?, ?)
            """,
            (session_id, session_id, now_iso()),
        )

        cur.execute(
            """
            INSERT INTO stocktake_items
              (session_id, product_code, description, barcode, quantity, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, product_code)
            DO UPDATE SET
              description=excluded.description,
              barcode=excluded.barcode,
              quantity=excluded.quantity,
              updated_by=excluded.updated_by,
              updated_at=excluded.updated_at
            """,
            (
                session_id,
                resolved_code,
                resolved_desc,
                resolved_barcode,
                quantity,
                updated_by,
                now_iso(),
            ),
        )

        conn.commit()
        return {
            "session_id": session_id,
            "product_code": resolved_code,
            "description": resolved_desc,
            "barcode": resolved_barcode,
            "quantity": quantity,
            "updated_by": updated_by,
            "updated_at": now_iso(),
        }
    finally:
        conn.close()


@router.get("/items")
def list_items(session_id: str = Query(...)):
    session_id = (session_id or "").strip()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT session_id, product_code, description, barcode, quantity, updated_by, updated_at
            FROM stocktake_items
            WHERE session_id = ?
            ORDER BY product_code
            """,
            (session_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@router.delete("/items")
def clear_items(session_id: str = Query(...)):
    session_id = (session_id or "").strip()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM stocktake_items WHERE session_id = ?", (session_id,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/move")
def move_item(payload: dict = Body(...)):
    """
    Move a product from one bin(session) to another.
    Keeps qty/desc/barcode, updates timestamps.
    """
    from_session_id = (payload.get("from_session_id") or "").strip()
    to_session_id = (payload.get("to_session_id") or "").strip()
    product_code = (payload.get("product_code") or "").strip()

    if not from_session_id or not to_session_id or not product_code:
        raise HTTPException(status_code=400, detail="from_session_id, to_session_id, product_code required")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT session_id, product_code, description, barcode, quantity, updated_by
            FROM stocktake_items
            WHERE session_id = ? AND product_code = ?
            LIMIT 1
            """,
            (from_session_id, product_code),
        )
        r = cur.fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Item not found in source bin")

        # ensure destination session exists
        cur.execute(
            """
            INSERT OR IGNORE INTO stocktake_sessions (session_id, label, created_at)
            VALUES (?, ?, ?)
            """,
            (to_session_id, to_session_id, now_iso()),
        )

        # upsert into destination
        cur.execute(
            """
            INSERT INTO stocktake_items
              (session_id, product_code, description, barcode, quantity, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, product_code)
            DO UPDATE SET
              description=excluded.description,
              barcode=excluded.barcode,
              quantity=excluded.quantity,
              updated_by=excluded.updated_by,
              updated_at=excluded.updated_at
            """,
            (
                to_session_id,
                r["product_code"],
                r["description"],
                r["barcode"],
                r["quantity"],
                r["updated_by"],
                now_iso(),
            ),
        )

        # delete from source
        cur.execute(
            "DELETE FROM stocktake_items WHERE session_id = ? AND product_code = ?",
            (from_session_id, product_code),
        )

        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/export")
def export_session(session_id: str = Query(...)):
    """
    Export ONE bin/session.
    Includes 'bin' column because Kerridge wants it even for individual sessions.
    """
    session_id = (session_id or "").strip()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT session_id, product_code, description, barcode, quantity, updated_by, updated_at
            FROM stocktake_items
            WHERE session_id = ?
            ORDER BY product_code
            """,
            (session_id,),
        )
        rows = cur.fetchall()

        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["bin", "product_code", "description", "barcode", "quantity", "updated_by", "updated_at"])
        for r in rows:
            w.writerow([
                r["session_id"],
                r["product_code"],
                r["description"],
                r["barcode"] or "",
                r["quantity"],
                r["updated_by"] or "",
                r["updated_at"] or "",
            ])

        data = output.getvalue().encode("utf-8")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="stocktake_{session_id}.csv"'},
        )
    finally:
        conn.close()


@router.get("/export_all_bins")
def export_all_bins():
    """
    Export ALL bins in ONE file.
    Sorted by bin then product_code (Kerridge-friendly).
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT session_id, product_code, description, barcode, quantity, updated_by, updated_at
            FROM stocktake_items
            ORDER BY session_id, product_code
            """
        )
        rows = cur.fetchall()

        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["bin", "product_code", "description", "barcode", "quantity", "updated_by", "updated_at"])
        for r in rows:
            w.writerow([
                r["session_id"],
                r["product_code"],
                r["description"],
                r["barcode"] or "",
                r["quantity"],
                r["updated_by"] or "",
                r["updated_at"] or "",
            ])

        data = output.getvalue().encode("utf-8")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="stocktake_ALL_bins.csv"'},
        )
    finally:
        conn.close()


@router.get("/export_all_merged")
def export_all_merged():
    """
    Export ALL bins merged into totals per product_code.
    (No bin column — this is a grand total report.)
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT product_code,
                   MAX(description) as description,
                   SUM(quantity) as quantity
            FROM stocktake_items
            GROUP BY product_code
            ORDER BY product_code
            """
        )
        rows = cur.fetchall()

        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["product_code", "description", "quantity"])
        for r in rows:
            w.writerow([r["product_code"], r["description"], r["quantity"]])

        data = output.getvalue().encode("utf-8")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="stocktake_ALL_merged.csv"'},
        )
    finally:
        conn.close()