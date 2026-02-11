# db.py
import os
import sqlite3
from pathlib import Path
from typing import Iterable


def _pick_db_path() -> str:
    """
    Priority:
      1) PRODUCTS_DB_PATH (if present)
      2) DB_PATH (if present)
      3) /var/data/midlands.db (if persistent disk mounted)
      4) ./midlands.db (local dev)
    """
    p = (os.getenv("PRODUCTS_DB_PATH") or "").strip()
    if p:
        return p

    p = (os.getenv("DB_PATH") or "").strip()
    if p:
        return p

    if os.path.isdir("/var/data"):
        return "/var/data/midlands.db"

    return "midlands.db"


DB_PATH = _pick_db_path()
DB_URL = f"sqlite:///{DB_PATH}"  # informational only


def get_conn() -> sqlite3.Connection:
    p = Path(DB_PATH)
    if p.parent and str(p.parent) not in ("", "."):
        p.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # safer defaults
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
    except Exception:
        pass
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def _has_column(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]  # (cid, name, type, notnull, dflt_value, pk)
    return col in cols


def _ensure_columns(cur: sqlite3.Cursor, table: str, columns_sql: Iterable[str]) -> None:
    for col_sql in columns_sql:
        col_name = col_sql.split()[0].strip()
        if not _has_column(cur, table, col_name):
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_sql};")


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # -------------------------
    # Products (CSV baseline)
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
          product_code TEXT PRIMARY KEY,
          full_description TEXT NOT NULL,
          retail_price REAL NOT NULL DEFAULT 0,
          manufacturers_product_code TEXT,
          barcode TEXT,
          updated_at TEXT
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_desc ON products(full_description);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_mfg_code ON products(manufacturers_product_code);")
    _ensure_columns(cur, "products", ["updated_at TEXT"])

    # -------------------------
    # Barcode overrides
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS barcode_overrides (
          product_code TEXT PRIMARY KEY,
          barcode TEXT,
          updated_at TEXT
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_barcode_overrides_barcode ON barcode_overrides(barcode);")
    _ensure_columns(cur, "barcode_overrides", ["updated_at TEXT"])

    # -------------------------
    # Barcode aliases: barcode -> product_code
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS barcode_aliases (
          barcode TEXT PRIMARY KEY,
          product_code TEXT NOT NULL,
          updated_at TEXT
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_barcode_aliases_product ON barcode_aliases(product_code);")
    _ensure_columns(cur, "barcode_aliases", ["updated_at TEXT"])

    # -------------------------
    # Stocktake tables (safe to keep)
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stocktake_sessions (
            session_id TEXT PRIMARY KEY,
            label TEXT,
            created_at TEXT
        );
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
        );
        """
    )

    # Bin expected products
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
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bin_products_bin ON bin_products(bin_code);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bin_products_code ON bin_products(product_code);")

    conn.commit()
    conn.close()
