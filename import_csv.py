# import_csv.py
import csv
import re
from typing import Dict, Any, List, Optional, Tuple

from db import get_conn


# ---------- helpers ----------
def _norm(s: Any) -> str:
    # Kerridge/Excel often includes NBSP (\xa0)
    return str(s or "").replace("\xa0", " ").strip()


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
    Normalize to digits-only, return None if unusable.
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
    return digits


def _detect_headers(fieldnames: List[str]) -> Dict[str, str]:
    """
    Map many possible header spellings to standard keys.
    Returns dict: standard_key -> actual_csv_header
    """
    fn = [f.strip() for f in (fieldnames or []) if f is not None]
    lower = {f.lower(): f for f in fn}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    mapping: Dict[str, str] = {}

    bc = pick("Bar Code", "Barcode", "BarCode", "ScanCode", "Scan Code", "scancode", "EAN", "EAN13", "UPC")
    if bc:
        mapping["barcode"] = bc

    pc = pick("Product Code", "ProductCode", "Code", "product_code", "productcode")
    if pc:
        mapping["product_code"] = pc

    desc = pick("Full Description", "Description", "full_description", "Name", "FullDescription")
    if desc:
        mapping["full_description"] = desc

    price = pick("VAT Inclusive Price", "Retail Price", "Price", "vat_inclusive_price", "retail_price")
    if price:
        mapping["retail_price"] = price

    mpc = pick(
        "Manufacturers Product Code",
        "Manufacturer Product Code",
        "manufacturers_product_code",
        "MFG Code",
        "MFG",
    )
    if mpc:
        mapping["manufacturers_product_code"] = mpc

    return mapping


def _sniff_dialect(sample: str):
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
    except Exception:
        return csv.excel


def _read_rows(csv_path: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Robust reader:
    - tries multiple encodings
    - sniffs delimiter
    - last fallback uses errors=replace (never crashes)
    """
    encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    last_err = None
    for enc in encodings_to_try:
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                sample = f.read(8192)
                f.seek(0)
                dialect = _sniff_dialect(sample)
                reader = csv.DictReader(f, dialect=dialect)
                mapping = _detect_headers(reader.fieldnames or [])
                rows = [r for r in reader]
                return rows, mapping
        except Exception as e:
            last_err = e

    # final fallback: replace bad bytes instead of failing
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        sample = f.read(8192)
        f.seek(0)
        dialect = _sniff_dialect(sample)
        reader = csv.DictReader(f, dialect=dialect)
        mapping = _detect_headers(reader.fieldnames or [])
        rows = [r for r in reader]
        return rows, mapping


# ---------- schema / migrations ----------
def _ensure_updated_at_column():
    """
    Your DB may already have a 'products' table created before updated_at existed.
    SQLite won't auto-add columns, so we patch it safely.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(products)")
        cols = [row[1] for row in cur.fetchall()]  # row[1] = column name
        if "updated_at" not in cols:
            cur.execute("ALTER TABLE products ADD COLUMN updated_at TEXT")
            conn.commit()
    finally:
        conn.close()


def _ensure_schema():
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
        conn.commit()
    finally:
        conn.close()

    # patch older databases
    _ensure_updated_at_column()


# ---------- core import ----------
def _upsert_products(records: List[Dict[str, Any]], prefer_barcode: bool) -> int:
    """
    prefer_barcode=True: barcode in record overwrites existing barcode
    prefer_barcode=False: barcode in record only set if existing is empty (never wipes)
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
            price = float(noteq := (rec.get("retail_price") or 0.0))  # safe cast
            mpc = _norm(rec.get("manufacturers_product_code")) or None
            bc = rec.get("barcode")

            # skip obvious non-product rows
            low = (desc or "").lower()
            if code in ("999998", "999999") or "line group" in low or "handling charge" in low:
                continue

            cur.execute("SELECT product_code, barcode FROM products WHERE product_code = ?", (code,))
            existing = cur.fetchone()

            if existing:
                # sqlite row can be tuple-like
                existing_barcode = existing[1]

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
                    (desc, float(noteq), mpc, new_barcode, code),
                )
                changed += 1
            else:
                cur.execute(
                    """
                    INSERT INTO products
                    (product_code, full_description, retail_price, manufacturers_product_code, barcode, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """,
                    (code, desc, float(noteq), mpc, bc if prefer_barcode else (bc or None)),
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
        out: List[Dict[str, Any]] = []

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
