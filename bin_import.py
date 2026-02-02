import csv
import io
from typing import Any, Dict, List


def parse_bin_locations_csv(content: bytes) -> List[Dict[str, Any]]:
    """
    Expected columns (case-insensitive):
      - product_code
      - description
      - stock_qty (optional)
      - main_bin
      - alt_bins (optional)  e.g. "12,14,90" or "12|14|90"
    """
    text = content.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    # normalize headers
    fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
    if not fieldnames:
        return []

    def get(row, *names):
        for n in names:
            if n in row:
                return row.get(n)
        return None

    out: List[Dict[str, Any]] = []

    for raw in reader:
        row = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in raw.items()}

        product_code = (get(row, "product_code", "product", "code") or "").strip()
        description = (get(row, "description", "desc") or "").strip()
        main_bin = (get(row, "main_bin", "bin", "bin_code") or "").strip()
        alt_bins = (get(row, "alt_bins", "alts", "alt") or "").strip()
        qty_raw = (get(row, "stock_qty", "baseline_qty", "qty", "quantity") or "").strip()

        if not product_code or not main_bin:
            continue

        baseline_qty = None
        if qty_raw != "":
            try:
                baseline_qty = float(qty_raw)
            except:
                baseline_qty = None

        # main bin record
        out.append(
            dict(
                bin_code=str(main_bin),
                product_code=str(product_code),
                description=description,
                baseline_qty=baseline_qty,
                is_main=1,
                alt_index=0,
            )
        )

        # alt bins (optional)
        if alt_bins:
            # allow commas or pipes
            parts = [p.strip() for p in alt_bins.replace("|", ",").split(",") if p.strip()]
            for idx, b in enumerate(parts, start=1):
                out.append(
                    dict(
                        bin_code=str(b),
                        product_code=str(product_code),
                        description=description,
                        baseline_qty=baseline_qty,
                        is_main=0,
                        alt_index=idx,
                    )
                )

    return out