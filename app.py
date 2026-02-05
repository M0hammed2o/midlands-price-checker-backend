import os
import re
from datetime import datetime
from typing import List, Optional

from fastapi import (
    FastAPI,
    Query,
    UploadFile,
    File,
    HTTPException,
    Body,
    Header,
    Depends,
)
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

# -----------------------------
# Simple Admin PIN protection (kept for upload_report + images only)
# -----------------------------
ADMIN_PIN = os.getenv("ADMIN_PIN", "").strip()

def require_admin_pin(x_admin_pin: Optional[str] = Header(default=None)):
    if not ADMIN_PIN:
        raise HTTPException(status_code=500, detail="ADMIN_PIN is not configured on server")
    if (x_admin_pin or "").strip() != ADMIN_PIN:
        raise HTTPException(status_code=401, detail="Invalid admin PIN")

# -----------------------------
# CORS
# -----------------------------
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
    allow_headers=["Content-Type", "X-Admin-Pin", "Authorization"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGES_DIR = os.path.join(BASE_DIR, "product_images")
os.makedirs(IMAGES_DIR, exist_ok=True)

DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

REPORT_CSV_PATH = os.path.join(DATA_DIR, "ProductScanApp_clean.csv")

app.include_router(stocktake_router)

# -----------------------------
# Bridge / Process Request tables
# -----------------------------
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

# -----------------------------
# Admin: Upload report CSV
# -----------------------------
@app.post("/admin/upload_report", dependencies=[Depends(require_admin_pin)])
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

# -----------------------------
# Models
# -----------------------------
class ProductOut(BaseModel):
    product_code: str
    full_description: str
    retail_price: float
    manufacturers_product_code: Optional[str] = None
    barcode: Optional[str] = None
    image_url: Optional[str] = None

class BarcodeUpdateIn(BaseModel):
    barcode: Optional[str] = None  # allow null to clear via PUT if you want

def _image_path_for(code: str) -> str:
    safe = (code or "").strip()
    return os.path.join(IMAGES_DIR, f"{safe}.jpg")

def _normalize_barcode(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.lstrip("^").strip()
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    if set(digits) == {"0"}:
        return None
    return digits

def _rows_to_products(rows) -> List[ProductOut]:
    out: List[ProductOut] = []
    for r in rows:
        code = r["product_code"]
        img_path = _image_path_for(code)
        image_url = f"/products/{code}/image" if os.path.exists(img_path) else None

        # We select effective_barcode via SQL alias
        effective_barcode = r["effective_barcode"]

        out.append(
            ProductOut(
                product_code=code,
                full_description=r["full_description"],
                retail_price=float(r["retail_price"]),
                manufacturers_product_code=r["manufacturers_product_code"],
                barcode=effective_barcode,
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
        # Helper base select (effective barcode = override else product)
        base_select = """
            SELECT
              p.*,
              COALESCE(bo.barcode, p.barcode) AS effective_barcode
            FROM products p
            LEFT JOIN barcode_overrides bo ON bo.product_code = p.product_code
        """

        if mode == "smart":
            q_compact = q.replace(" ", "")
            if q_compact.isdigit():
                # First try alias table (fast + accurate)
                cur.execute(
                    """
                    SELECT p.*, COALESCE(bo.barcode, p.barcode) AS effective_barcode
                    FROM barcode_aliases ba
                    JOIN products p ON p.product_code = ba.product_code
                    LEFT JOIN barcode_overrides bo ON bo.product_code = p.product_code
                    WHERE ba.barcode = ?
                    LIMIT ?
                    """,
                    (q_compact, limit),
                )
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

                # Then try effective barcode match (override or CSV)
                cur.execute(
                    base_select + " WHERE COALESCE(bo.barcode, p.barcode) = ? LIMIT ?",
                    (q_compact, limit),
                )
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

                # Then product code exact
                cur.execute(base_select + " WHERE p.product_code = ? LIMIT ?", (q_compact, limit))
                rows = cur.fetchall()
                if rows:
                    return _rows_to_products(rows)

            # Name search
            cur.execute(
                base_select
                + " WHERE LOWER(p.full_description) LIKE LOWER(?) ORDER BY p.full_description LIMIT ?",
                (f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "name":
            cur.execute(
                base_select
                + " WHERE LOWER(p.full_description) LIKE LOWER(?) ORDER BY p.full_description LIMIT ?",
                (f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "code":
            cur.execute(
                base_select
                + """
                WHERE p.product_code = ?
                   OR p.manufacturers_product_code = ?
                   OR p.product_code LIKE ?
                   OR p.manufacturers_product_code LIKE ?
                ORDER BY p.full_description
                LIMIT ?
                """,
                (q, q, f"%{q}%", f"%{q}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        if mode == "barcode":
            q_compact = q.replace(" ", "")

            # alias match first
            cur.execute(
                """
                SELECT p.*, COALESCE(bo.barcode, p.barcode) AS effective_barcode
                FROM barcode_aliases ba
                JOIN products p ON p.product_code = ba.product_code
                LEFT JOIN barcode_overrides bo ON bo.product_code = p.product_code
                WHERE ba.barcode = ?
                LIMIT ?
                """,
                (q_compact, limit),
            )
            rows = cur.fetchall()
            if rows:
                return _rows_to_products(rows)

            # fallback to effective barcode direct match
            cur.execute(
                base_select
                + """
                WHERE COALESCE(bo.barcode, p.barcode) = ?
                   OR COALESCE(bo.barcode, p.barcode) LIKE ?
                LIMIT ?
                """,
                (q_compact, f"%{q_compact}%", limit),
            )
            return _rows_to_products(cur.fetchall())

        return _search_products_internal(q=q, mode="smart", limit=limit)
    finally:
        conn.close()

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

# -----------------------------
# ✅ NEW: Barcode update endpoints (NO PIN, as requested)
# -----------------------------
@app.put("/products/{product_code}/barcode")
def set_product_barcode(product_code: str, payload: BarcodeUpdateIn = Body(...)):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    new_barcode = _normalize_barcode(payload.barcode)

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Ensure product exists
        cur.execute("SELECT product_code FROM products WHERE product_code = ? LIMIT 1", (code,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Product not found")

        now = datetime.now().isoformat(timespec="seconds")

        # Upsert override (or clear it)
        if new_barcode:
            cur.execute(
                """
                INSERT INTO barcode_overrides (product_code, barcode, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(product_code) DO UPDATE SET
                    barcode=excluded.barcode,
                    updated_at=excluded.updated_at
                """,
                (code, new_barcode, now),
            )
            # Maintain alias mapping for scanning
            cur.execute(
                """
                INSERT INTO barcode_aliases (barcode, product_code, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(barcode) DO UPDATE SET
                    product_code=excluded.product_code,
                    updated_at=excluded.updated_at
                """,
                (new_barcode, code, now),
            )
        else:
            cur.execute("DELETE FROM barcode_overrides WHERE product_code = ?", (code,))

        conn.commit()
        return {"ok": True, "product_code": code, "barcode": new_barcode}
    finally:
        conn.close()

@app.delete("/products/{product_code}/barcode")
def clear_product_barcode_override(product_code: str):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM barcode_overrides WHERE product_code = ?", (code,))
        conn.commit()
        return {"ok": True, "product_code": code, "cleared": True}
    finally:
        conn.close()

# -----------------------------
# Product Images (unchanged, still PIN protected)
# -----------------------------
@app.get("/products/{product_code}/image")
def get_product_image(product_code: str):
    code = (product_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="product_code required")

    path = _image_path_for(code)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(path, media_type="image/jpeg")

@app.post("/products/{product_code}/image", dependencies=[Depends(require_admin_pin)])
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

@app.delete("/products/{product_code}/image", dependencies=[Depends(require_admin_pin)])
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
# Bridge: Process Requests (unchanged)
# -----------------------------
class ProcessItemIn(BaseModel):
    product_code: str
    qty: float = Field(..., gt=0)
    description: Optional[str] = None
    price: Optional[float] = None

class ProcessRequestIn(BaseModel):
    requested_by: Optional[str] = None
    items: List[ProcessItemIn]

@app.post("/bridge/process_requests")
def create_process_request(payload: ProcessRequestIn):
    if not payload.items:
        raise HTTPException(status_code=400, detail="No items provided")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO process_requests (created_at, requested_by) VALUES (?, ?)",
            (datetime.now().isoformat(timespec="seconds"), (payload.requested_by or "").strip() or None),
        )
        request_id = cur.lastrowid

        for it in payload.items:
            code = (it.product_code or "").strip()
            if not code:
                continue
            cur.execute(
                """
                INSERT INTO process_request_items
                (request_id, product_code, qty, description, price)
                VALUES (?, ?, ?, ?, ?)
                """,
                (request_id, code, float(it.qty), it.description, it.price),
            )

        conn.commit()
        return {"ok": True, "id": request_id}
    finally:
        conn.close()

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

    raw_lines = payload.get("lines") or payload.get("items") or payload.get("cart") or []
    if isinstance(raw_lines, dict):
        raw_lines = [raw_lines]

    if not isinstance(raw_lines, list) or len(raw_lines) == 0:
        raise HTTPException(status_code=400, detail="No reorder lines provided")

    lines_text: List[str] = []
    idx = 1

    for line in raw_lines:
        if not isinstance(line, dict):
            continue

        product_code = (line.get("product_code") or line.get("productCode") or line.get("code") or "").strip()
        qty_val = line.get("qty") or line.get("quantity") or line.get("count")
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
        raise HTTPException(status_code=400, detail="All lines were empty/invalid")

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
