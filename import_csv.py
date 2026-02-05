import csv
from typing import Optional, List, Dict

from db import get_conn

def _pick(row: dict, *keys: str) -> Optional[str]:
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None

def _to_float(value: Optional[str]) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    s = s.replace("R", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def _read_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, "rb") as f:
        raw = f.read()

    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            text = None
    if text is None:
        text = raw.decode("latin-1", errors="replace")

    reader = csv.DictReader(text.splitlines())
    return list(reader)

def import_products(csv_path: str) -> int:
    rows = _read_csv_rows(csv_path)
    if not rows:
        return 0

    conn = get_conn()
    cur = conn.cursor()

    inserted_or_updated = 0
    try:
        for row in rows:
            product_code = _pick(row, "product_code", "Product Code", "Code")
            full_description = _pick(row, "full_description", "Full Description", "Description", "Name")
            retail_price = _to_float(_pick(row, "retail_price", "Retail Price", "VAT Inclusive Price", "Price"))

            mfg_code = _pick(row, "manufacturers_product_code", "Manufacturers Product Code", "Manufacturer Code")

            # barcode from CSV (if any)
            barcode = _pick(row, "barcode", "Barcode", "ScanCode", "scancode", "scan_code")
            if barcode is not None:
                barcode = barcode.strip().replace(" ", "")  # normalize

            if not product_code or not full_description:
                continue

            cur.execute(
                """
                INSERT INTO products (product_code, full_description, retail_price, manufacturers_product_code, barcode)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_code) DO UPDATE SET
                    full_description = excluded.full_description,
                    retail_price = excluded.retail_price,
                    manufacturers_product_code = excluded.manufacturers_product_code,
                    -- ✅ DO NOT wipe barcode if excluded.barcode is NULL
                    barcode = COALESCE(excluded.barcode, products.barcode)
                """,
                (product_code, full_description, float(retail_price), mfg_code, barcode),
            )
            inserted_or_updated += 1

        conn.commit()
        return inserted_or_updated
    finally:
        conn.close()
