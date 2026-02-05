# bin_import.py
import csv
import io
from typing import Any, Dict, List, Optional, Union


def _pick(row: dict, *keys: str) -> Optional[str]:
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def _to_float(v: Optional[str]) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def _decode_csv_bytes(content: bytes) -> str:
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return content.decode(enc)
        except UnicodeDecodeError:
            continue
    return content.decode("latin-1", errors="replace")


def parse_bin_locations_csv(csv_input: Union[str, bytes]) -> List[Dict[str, Any]]:
    """
    stocktake.py calls parse_bin_locations_csv(content) where content is bytes.
    This function supports BOTH:
      - bytes (uploaded file content)
      - str  (file path)

    Expected headers (any of these will work):
      bin_code:       bin_code | Bin | Bin Code | BIN
      product_code:   product_code | Product Code | Code
      description:    description | Description | Full Description | Product
      baseline_qty:   baseline_qty | Baseline Qty | Qty | Quantity

    Returns a list of dicts.
    """
    if isinstance(csv_input, (bytes, bytearray)):
        text = _decode_csv_bytes(bytes(csv_input))
        f = io.StringIO(text)
    else:
        # treat as path
        f = open(csv_input, "r", encoding="utf-8-sig", newline="")

    try:
        reader = csv.DictReader(f)
        out: List[Dict[str, Any]] = []

        for row in reader:
            bin_code = _pick(row, "bin_code", "Bin", "Bin Code", "BIN")
            product_code = _pick(row, "product_code", "Product Code", "Code")
            description = _pick(row, "description", "Description", "Full Description", "Product")
            baseline_qty = _to_float(_pick(row, "baseline_qty", "Baseline Qty", "Qty", "Quantity"))

            if not bin_code or not product_code:
                continue

            out.append(
                {
                    "bin_code": str(bin_code).strip(),
                    "product_code": str(product_code).strip(),
                    "description": (str(description).strip() if description else ""),
                    "baseline_qty": float(baseline_qty),
                    # keep defaults for your schema
                    "is_main": 1,
                    "alt_index": 0,
                }
            )

        return out
    finally:
        f.close()
