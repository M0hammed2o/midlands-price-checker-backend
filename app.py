Here is import_csv.py and app.py please check both. 
import_csv.py:
import csv
import re
from typing import Dict, Any, List, Optional, Tuple

from db import get_conn

# ---------- helpers ----------
def _norm(s: Any) -> str:
    return str(s or "").strip()

def _to_float(x: Any) -> float:
    s = _norm(x)
    if not s:
        return 0.0
    # handle "1,234.56"
    s = s.replace(" ", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def _clean_barcode(raw: Any) -> Optional[str]:
    """
    Kerridge barcodes can look like:
      ^60095 51 80 27 61
      6009509920844
      0000000000000 (meaning "no barcode")
    We normalize to digits-only, return None if unusable.
    """
    s = _norm(raw)
    if not s:
        return None
    s = s.replace("^", "")
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    if set(digits) == {"0"}:
        return None
    # sometimes Kerridge includes short/odd codes; keep them if digits exist
    return digits

def _detect_headers(fieldnames: List[str]) -> Dict[str, str]:
    """
    Map many possible header spellings to standard keys.
    Returns dict of standard_key -> actual_csv_header
    """
    fn = [f.strip() for f in (fieldnames or [])]
    lower = {f.lower(): f for f in fn}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    mapping = {}

    # barcode header can be "Bar Code" or "BarCode" etc
    bc = pick("Bar Code", "Barcode", "BarCode", "ScanCode", "Scan Code", "scancode")
    if bc:
        mapping["barcode"] = bc

    pc = pick("Product Code", "ProductCode", "Code", "product_code")
    if pc:
        mapping["product_code"] = pc

    desc = pick("Full Description", "Description", "full_description", "Name")
    if desc:
        mapping["full_description"] = desc

    price = pick("VAT Inclusive Price", "Retail Price", "Price", "vat_inclusive_price", "retail_price")
    if price:
        mapping["retail_price"] = price

    mpc = pick("Manufacturers Product Code", "Manufacturer Product Code", "manufacturers_product_code", "MFG Code")
    if mpc:
        mapping["manufacturers_product_code"] = mpc

    return mapping

def _read_rows(csv_path: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    # try utf-8-sig to handle BOM
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        mapping = _detect_headers(reader.fieldnames or [])
        rows = []
        for r in reader:
            rows.append(r)
        return rows, mapping

def _ensure_schema():
    """
    Ensure products table exists and has required columns.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                product_code TEXT PRIMARY KEY,
                full_description TEXT NOT NULL,
                retail_price REAL NOT NULL DEFAULT 0,
                manufacturers_product_code TEXT,
                barcode TEXT,
                updated_at TEXT
            )
            """
        )
        # Index barcode for faster scanning lookup
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
        conn.commit()
    finally:
        conn.close()

# ---------- core import ----------
def _upsert_products(records: List[Dict[str, Any]], prefer_barcode: bool) -> int:
    """
    Upsert records into products table.
    prefer_barcode=True means "if record has barcode, set it; else NEVER overwrite an existing barcode."
    """
    conn = get_conn()
    cur = conn.cursor()
    changed = 0
    try:
        for rec in records:
            code = _norm(rec.get("product_code"))
            if not code:
                continue

            desc = _norm(rec.get("full_description")) or "Unknown product"
            price = float(rec.get("retail_price") or 0.0)
            mpc = _norm(rec.get("manufacturers_product_code")) or None
            bc = rec.get("barcode")  # already cleaned or None

            # skip obvious non-product rows (your sample has "Line Group", "Handling charge")
            low = (desc or "").lower()
            if code in ("999998", "999999") or "line group" in low or "handling charge" in low:
                continue

            # fetch existing
            cur.execute(
                "SELECT product_code, barcode FROM products WHERE product_code = ?",
                (code,),
            )
            existing = cur.fetchone()

            if existing:
                existing_barcode = existing["barcode"] if isinstance(existing, dict) else existing[1]

                # barcode merge rule:
                # - if prefer_barcode and bc present -> update barcode
                # - else if not prefer_barcode -> only set barcode if currently empty AND bc present
                new_barcode = existing_barcode
                if bc:
                    if prefer_barcode:
                        new_barcode = bc
                    else:
                        if not existing_barcode:
                            new_barcode = bc

                cur.execute(
                    """
                    UPDATE products
                    SET full_description = ?,
                        retail_price = ?,
                        manufacturers_product_code = ?,
                        barcode = ?,
                        updated_at = datetime('now')
                    WHERE product_code = ?
                    """,
                    (desc, price, mpc, new_barcode, code),
                )
                changed += 1
            else:
                cur.execute(
                    """
                    INSERT INTO products
                    (product_code, full_description, retail_price, manufacturers_product_code, barcode, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """,
                    (code, desc, price, mpc, bc if prefer_barcode else (bc or None)),
                )
                changed += 1

        conn.commit()
        return changed
    finally:
        conn.close()

def import_products_two_reports(csv_with_barcodes: str, csv_without_barcodes: str) -> Dict[str, Any]:
    """
    Merge strategy:
      1) Import barcode report first with prefer_barcode=True
      2) Import no-barcode report second with prefer_barcode=False (never wipes barcode)
    """
    _ensure_schema()

    rows_a, map_a = _read_rows(csv_with_barcodes)
    rows_b, map_b = _read_rows(csv_without_barcodes)

    def to_records(rows: List[Dict[str, Any]], mapping: Dict[str, str], has_barcode: bool) -> List[Dict[str, Any]]:
        out = []
        for r in rows:
            rec = {
                "product_code": _norm(r.get(mapping.get("product_code", ""), "")),
                "full_description": _norm(r.get(mapping.get("full_description", ""), "")),
                "retail_price": _to_float(r.get(mapping.get("retail_price", ""), 0)),
                "manufacturers_product_code": _norm(r.get(mapping.get("manufacturers_product_code", ""), "")) or None,
                "barcode": None,
            }
            if has_barcode and "barcode" in mapping:
                rec["barcode"] = _clean_barcode(r.get(mapping["barcode"]))
            out.append(rec)
        return out

    rec_a = to_records(rows_a, map_a, has_barcode=True)
    rec_b = to_records(rows_b, map_b, has_barcode=False)

    changed_a = _upsert_products(rec_a, prefer_barcode=True)
    changed_b = _upsert_products(rec_b, prefer_barcode=False)

    return {
        "imported_with_barcodes": changed_a,
        "imported_without_barcodes": changed_b,
        "total_changed": changed_a + changed_b,
    }

App.py:
# app.py
import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Query, UploadFile, File, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import FileResponse
from dotenv import load_dotenv

from db import init_db, get_conn
from import_csv import import_products
from emailer import send_reorder_email
from stocktake import router as stocktake_router, init_stocktake_tables

load_dotenv()

app = FastAPI(title="Midlands Price Checker")

ALLOWED_ORIGINS = [
    "https://midlands-price-checker.pages.dev",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Admin-Pin"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGES_DIR = os.path.join(BASE_DIR, "product_images")
os.makedirs(IMAGES_DIR, exist_ok=True)

DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

REPORT_CSV_PATH = os.path.join(DATA_DIR, "ProductScanApp_clean.csv")

app.include_router(stocktake_router)


def init_bridge_tables():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS process_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                requested_by TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS process_request_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                product_code TEXT NOT NULL,
                qty REAL NOT NULL,
                description TEXT,
                price REAL,
                FOREIGN KEY(request_id) REFERENCES process_requests(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


@app.on_event("startup")
def startup():
    init_db()
    init_stocktake_tables()
    init_bridge_tables()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/admin/upload_report")
async def upload_report(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        with open(REPORT_CSV_PATH, "wb") as f:
            f.write(content)

        inserted_or_updated = import_products(csv_path=REPORT_CSV_PATH)
        return {"ok": True, "imported": inserted_or_updated}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV import failed: {repr(e)}")


class ProductOut(BaseModel):
    product_code: str
    full_description: str
    retail_price: float
    manufacturers_product_code: Optional[str] = None
    barcode: Optional[str] = None
    image_url: Optional[str] = None


def _image_path_for(code: str) -> str:
    safe = (code or "").strip()
    return os.path.join(IMAGES_DIR, f"{safe}.jpg")


def _rows_to_products(rows) -> List[ProductOut]:
    out: List[ProductOut] = []
    for r in rows:
        code = r["product_code"]
        img_path = _image_path_for(code)
        image_url = f"/products/{code}/image" if os.path.exists(img_path) else None

        out.append(
            ProductOut(
                product_code=code,
                full_description=r["full_description"],
                retail_price=float(r["retail_price"]),
                manufacturers_product_code=r["manufacturers_product_code"],
                barcode=r["barcode"],
                image_url=image_url,
            )
        )
    return out


def _search_products_internal(q: str, mode: str = "smart", limit: int = 25) -> List[ProductOut]:
    q = (q or "").strip()
    if q == "":
        return []

    mode = (mode or "smart").strip()
    limit = int(limit)

    conn = get_conn()
    cur = conn.cursor()
    try:
        if mode == "smart":
            q_compact = q.replace(" ", "")

            # ✅ If digits, treat like scan: alias-first
            if q_compact.isdigit():
                # 1) barcode_aliases -> products
                cur.execute(
                    """
                    SELECT p.*
                    FROM barcode_aliases a
                    JOIN products p ON p.product_code = a.product_code
                    WHERE a.barcode = ?
                    LIMIT ?
                    """,
                    (q_compact, limit),
                )
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

                # 2) products.barcode exact
                cur.execute("SELECT * FROM products WHERE barcode = ? LIMIT ?", (q_compact, limit))
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

                # 3) product_code exact
                cur.execute("SELECT * FROM products WHERE product_code = ? LIMIT ?", (q_compact, limit))
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

            # 4) name contains
            cur.execute(
                "SELECT * FROM products WHERE LOWER(full_description) LIKE LOWER(?) "
                "ORDER BY full_description LIMIT ?",
                (f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "name":
            cur.execute(
                "SELECT * FROM products WHERE LOWER(full_description) LIKE LOWER(?) "
                "ORDER BY full_description LIMIT ?",
                (f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "code":
            cur.execute(
                """
                SELECT * FROM products
                WHERE product_code = ?
                   OR manufacturers_product_code = ?
                   OR product_code LIKE ?
                   OR manufacturers_product_code LIKE ?
                ORDER BY full_description
                LIMIT ?
                """,
                (q, q, f"%{q}%", f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "barcode":
            q_compact = q.replace(" ", "")

            # alias-first in barcode mode too
            cur.execute(
                """
                SELECT p.*
                FROM barcode_aliases a
                JOIN products p ON p.product_code = a.product_code
                WHERE a.barcode = ?
                LIMIT ?
                """,
                (q_compact, limit),
            )
            rows = cur.fetchall()
            if rows:
                return _rows_to_products(rows)

            cur.execute(
                """
                SELECT * FROM products
                WHERE barcode = ? OR barcode LIKE ?
                LIMIT ?
                """,
                (q_compact, f"%{q_compact}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        return _search_products_internal(q=q, mode="smart", limit=limit)
    finally:
        conn.close()

# -----------------------------
# Admin: Upload TWO reports (with + without barcodes)
# -----------------------------
@app.post("/admin/upload_reports", dependencies=[Depends(require_admin_pin)])
async def upload_reports(
    file_barcodes: UploadFile = File(...),
    file_nobarcodes: UploadFile = File(...),
):
    def _is_csv(f: UploadFile) -> bool:
        return (f.filename or "").lower().endswith(".csv")

    if not _is_csv(file_barcodes) or not _is_csv(file_nobarcodes):
        raise HTTPException(status_code=400, detail="Please upload 2 .csv files")

    content_a = await file_barcodes.read()
    content_b = await file_nobarcodes.read()

    if not content_a or not content_b:
        raise HTTPException(status_code=400, detail="One of the uploaded files is empty")

    try:
        barcoded_path = os.path.join(DATA_DIR, "report_with_barcodes.csv")
        nobar_path = os.path.join(DATA_DIR, "report_without_barcodes.csv")

        with open(barcoded_path, "wb") as f:
            f.write(content_a)
        with open(nobar_path, "wb") as f:
            f.write(content_b)

        from import_csv import import_products_two_reports

        result = import_products_two_reports(
            csv_with_barcodes=barcoded_path,
            csv_without_barcodes=nobar_path,
        )
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV import failed: {repr(e)}")

@app.get("/products/search", response_model=List[ProductOut])
def search_products(
    q: str = Query("", min_length=0),
    query: str = Query("", min_length=0),
    search: str = Query("", min_length=0),
    mode: str = Query("smart", pattern="^(smart|name|code|barcode)$"),
    limit: int = Query(25, ge=1, le=100),
):
    effective_q = (q or "").strip() or (query or "").strip() or (search or "").strip()
    return _search_products_internal(q=effective_q, mode=mode, limit=limit)


@app.get("/search", response_model=List[ProductOut])
def legacy_search(
    q: str = Query("", min_length=0),
    mode: str = Query("smart", pattern="^(smart|name|code|barcode)$"),
    limit: int = Query(25, ge=1, le=100),
):
    return _search_products_internal(q=(q or "").strip(), mode=mode, limit=limit)


@app.get("/products/{product_code}/image")
def get_product_image(product_code: str):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    path = _image_path_for(code)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(path, media_type="image/jpeg")


@app.post("/products/{product_code}/image")
async def upload_product_image(product_code: str, file: UploadFile = File(...)):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    filename = (file.filename or "").lower()
    if not (filename.endswith(".jpg") or filename.endswith(".jpeg") or filename.endswith(".png") or filename.endswith(".webp")):
        raise HTTPException(status_code=400, detail="Upload an image: .jpg/.jpeg/.png/.webp")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    path = _image_path_for(code)
    try:
        with open(path, "wb") as f:
            f.write(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save image: {repr(e)}")

    return {"ok": True, "product_code": code, "image_url": f"/products/{code}/image"}


@app.delete("/products/{product_code}/image")
def delete_product_image(product_code: str):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    path = _image_path_for(code)
    if not os.path.exists(path):
        return {"ok": True, "deleted": False, "reason": "Image did not exist"}

    try:
        os.remove(path)
        return {"ok": True, "deleted": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete image: {repr(e)}")


# -----------------------------
# Reorder Email (unchanged)
# -----------------------------
@app.post("/reorder")
def reorder(payload: dict = Body(...)):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON payload (expected object)")

    requested_by = (
        (payload.get("requested_by") or payload.get("updated_by") or payload.get("user") or "")
        .strip()
        or "Unknown"
    )

    if ("product_code" in payload or "productCode" in payload or "code" in payload) and (
        "qty" in payload or "quantity" in payload or "count" in payload
    ):
        raw_lines = [payload]
    else:
        raw_lines = (
            payload.get("lines")
            or payload.get("items")
            or payload.get("cart")
            or payload.get("products")
            or payload.get("rows")
            or payload.get("order_items")
            or payload.get("reorder_items")
            or payload.get("data")
        )

        if raw_lines is None:
            for v in payload.values():
                if isinstance(v, list):
                    raw_lines = v
                    break

        if raw_lines is None:
            raw_lines = []

    if isinstance(raw_lines, dict):
        raw_lines = [raw_lines]

    if not isinstance(raw_lines, list) or len(raw_lines) == 0:
        raise HTTPException(status_code=400, detail="No reorder lines provided (expected lines/items/cart array)")

    lines_text: List[str] = []
    idx = 1

    for line in raw_lines:
        if not isinstance(line, dict):
            continue

        product_code = (line.get("product_code") or line.get("productCode") or line.get("code") or "").strip()

        qty_val = line.get("qty")
        if qty_val is None:
            qty_val = line.get("quantity")
        if qty_val is None:
            qty_val = line.get("count")

        note = (line.get("note") or line.get("reason") or "").strip() or None

        if not product_code:
            continue

        try:
            qty = float(qty_val)
        except Exception:
            qty = 0.0

        if qty <= 0:
            continue

        note_txt = f" | Note: {note}" if note else ""
        lines_text.append(f"{idx}. {product_code}  x{qty}{note_txt}")
        idx += 1

    if not lines_text:
        raise HTTPException(status_code=400, detail="All lines were empty/invalid (need product_code + qty>0)")

    subject = f"Reorder Request - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = (
        f"Requested by: {requested_by}\n"
        f"Time: {datetime.now().isoformat(timespec='seconds')}\n\n"
        "Items:\n" + "\n".join(lines_text) + "\n"
    )

    try:
        send_reorder_email(subject=subject, body=body)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reorder email failed: {repr(e)}")


@app.post("/admin/reorder")
def admin_reorder(payload: dict = Body(...)):
    return reorder(payload)
