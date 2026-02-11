# backend/stocktake.py
import csv
import io
import os
from datetime import datetime
from typing import Optional, Tuple

from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Body, Header, Depends
from starlette.responses import StreamingResponse

from db import get_conn
from bin_import import parse_bin_locations_csv

router = APIRouter(prefix="/stocktake", tags=["stocktake"])


def _read_admin_pin_env() -> str:
    return (os.getenv("ADMIN_PIN", "") or "").strip()


def require_admin_pin_header(x_admin_pin: Optional[str] = Header(default=None)):
    admin_pin = _read_admin_pin_env()
    if not admin_pin:
        raise HTTPException(status_code=500, detail="ADMIN_PIN is not configured on server")
    if (x_admin_pin or "").strip() != admin_pin:
        raise HTTPException(status_code=401, detail="Invalid admin PIN")


def require_admin_pin_header_or_query(
    x_admin_pin: Optional[str] = Header(default=None),
    admin_pin: Optional[str] = Query(default=None),
    pin: Optional[str] = Query(default=None),
):
    server_pin = _read_admin_pin_env()
    if not server_pin:
        raise HTTPException(status_code=500, detail="ADMIN_PIN is not configured on server")

    provided = (x_admin_pin or "").strip() or (admin_pin or "").strip() or (pin or "").strip()
    if provided != server_pin:
        raise HTTPException(status_code=401, detail="Invalid admin PIN")


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


@router.get("/bins", dependencies=[Depends(require_admin_pin_header)])
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

        cur.execute(
            """
            SELECT session_id FROM stocktake_sessions
            WHERE session_id IS NOT NULL AND TRIM(session_id) <> ''
            ORDER BY session_id
            """
        )
        sessions = [r["session_id"] for r in cur.fetchall()]

        return sorted(set(bins + sessions))
    finally:
        conn.close()


@router.get("/bin_products", dependencies=[Depends(require_admin_pin_header)])
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


# ✅ Add/Update one product into a bin (no CSV needed)
@router.post("/bin_products/add", dependencies=[Depends(require_admin_pin_header)])
def add_bin_product(payload: dict = Body(...)):
    bin_code = (payload.get("bin_code") or "").strip()
    product_code = (payload.get("product_code") or "").strip()
    description = (payload.get("description") or "").strip()
    baseline_qty = payload.get("baseline_qty")

    try:
        baseline_qty = float(baseline_qty if baseline_qty is not None else 0)
    except Exception:
        baseline_qty = 0.0

    if not bin_code or not product_code:
        raise HTTPException(status_code=400, detail="bin_code and product_code required")

    # If no description provided, try pull from products table
    if not description:
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT full_description FROM products WHERE product_code = ? LIMIT 1", (product_code,))
            r = cur.fetchone()
            if r:
                description = r["full_description"] or ""
        finally:
            conn.close()

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT OR REPLACE INTO bin_products
            (bin_code, product_code, description, baseline_qty, is_main, alt_index)
            VALUES (?, ?, ?, ?, 1, 0)
            """,
            (bin_code, product_code, description, baseline_qty),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ✅ Remove product from bin
@router.post("/bin_products/remove", dependencies=[Depends(require_admin_pin_header)])
def remove_bin_product(payload: dict = Body(...)):
    bin_code = (payload.get("bin_code") or "").strip()
    product_code = (payload.get("product_code") or "").strip()
    if not bin_code or not product_code:
        raise HTTPException(status_code=400, detail="bin_code and product_code required")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM bin_products WHERE bin_code = ? AND product_code = ?",
            (bin_code, product_code),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/bins/upload", dependencies=[Depends(require_admin_pin_header)])
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


def _resolve_product(barcode: Optional[str], product_code: Optional[str]) -> Tuple[str, str, Optional[str]]:
    """
    ✅ Correct priority:
    1) barcode_aliases (barcode -> product_code)  [best for scanning]
    2) barcode_overrides (product_code override barcode)
    3) products.barcode exact match (Kerridge)
    4) product_code exact match
    5) numeric barcode might actually be product_code
    Returns: (resolved_code, resolved_desc, resolved_effective_barcode)
    """
    barcode = (barcode or "").strip().replace(" ", "")
    product_code = (product_code or "").strip()

    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1) barcode_aliases first (best for scanning)
        if barcode:
            cur.execute(
                """
                SELECT
                    p.product_code,
                    p.full_description,
                    COALESCE(o.barcode, p.barcode) AS effective_barcode
                FROM barcode_aliases a
                JOIN products p ON p.product_code = a.product_code
                LEFT JOIN barcode_overrides o ON o.product_code = p.product_code
                WHERE a.barcode = ?
                LIMIT 1
                """,
                (barcode,),
            )
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r["effective_barcode"] or barcode

        # 2) if product_code was provided, see if override exists
        if product_code:
            cur.execute(
                """
                SELECT
                    p.product_code,
                    p.full_description,
                    COALESCE(o.barcode, p.barcode) AS effective_barcode
                FROM products p
                LEFT JOIN barcode_overrides o ON o.product_code = p.product_code
                WHERE p.product_code = ?
                LIMIT 1
                """,
                (product_code,),
            )
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r["effective_barcode"] or None

        # 3) barcode direct match against products.barcode (Kerridge)
        if barcode:
            cur.execute(
                """
                SELECT
                    p.product_code,
                    p.full_description,
                    COALESCE(o.barcode, p.barcode) AS effective_barcode
                FROM products p
                LEFT JOIN barcode_overrides o ON o.product_code = p.product_code
                WHERE p.barcode = ?
                LIMIT 1
                """,
                (barcode,),
            )
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r["effective_barcode"] or barcode

        # 4) numeric barcode might actually be product_code
        if barcode and barcode.isdigit():
            cur.execute(
                """
                SELECT
                    p.product_code,
                    p.full_description,
                    COALESCE(o.barcode, p.barcode) AS effective_barcode
                FROM products p
                LEFT JOIN barcode_overrides o ON o.product_code = p.product_code
                WHERE p.product_code = ?
                LIMIT 1
                """,
                (barcode,),
            )
            r = cur.fetchone()
            if r:
                return r["product_code"], r["full_description"], r["effective_barcode"] or barcode

        # fail-safe
        return (product_code or barcode or ""), "UNKNOWN", (barcode or None)
    finally:
        conn.close()


@router.post("/item", dependencies=[Depends(require_admin_pin_header)])
def add_or_update_item(payload: dict = Body(...)):
    session_id = (payload.get("session_id") or "").strip()
    barcode = payload.get("barcode")
    product_code = payload.get("product_code")
    updated_by = (payload.get("updated_by") or "").strip() or None

    try:
        quantity = float(payload.get("quantity") or 0)
    except Exception:
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
            (session_id, resolved_code, resolved_desc, resolved_barcode, quantity, updated_by, now_iso()),
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


@router.get("/items", dependencies=[Depends(require_admin_pin_header)])
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


@router.delete("/items", dependencies=[Depends(require_admin_pin_header)])
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


@router.post("/move", dependencies=[Depends(require_admin_pin_header)])
def move_item(payload: dict = Body(...)):
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

        cur.execute(
            """
            INSERT OR IGNORE INTO stocktake_sessions (session_id, label, created_at)
            VALUES (?, ?, ?)
            """,
            (to_session_id, to_session_id, now_iso()),
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
            (to_session_id, r["product_code"], r["description"], r["barcode"], r["quantity"], r["updated_by"], now_iso()),
        )

        cur.execute("DELETE FROM stocktake_items WHERE session_id = ? AND product_code = ?", (from_session_id, product_code))

        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/export", dependencies=[Depends(require_admin_pin_header_or_query)])
def export_session(session_id: str = Query(...)):
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
            w.writerow([r["session_id"], r["product_code"], r["description"], r["barcode"] or "", r["quantity"], r["updated_by"] or "", r["updated_at"] or ""])

        data = output.getvalue().encode("utf-8")
        return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="stocktake_{session_id}.csv"'})
    finally:
        conn.close()


@router.get("/export_all_bins", dependencies=[Depends(require_admin_pin_header_or_query)])
def export_all_bins():
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
            w.writerow([r["session_id"], r["product_code"], r["description"], r["barcode"] or "", r["quantity"], r["updated_by"] or "", r["updated_at"] or ""])

        data = output.getvalue().encode("utf-8")
        return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="stocktake_ALL_bins.csv"'})
    finally:
        conn.close()


@router.get("/export_all_merged", dependencies=[Depends(require_admin_pin_header_or_query)])
def export_all_merged():
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
        return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="stocktake_ALL_merged.csv"'})
    finally:
        conn.close()
