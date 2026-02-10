# db.py
import os
import sqlite3
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "midlands.db")
DB_URL = f"sqlite:///{DB_PATH}"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Safer defaults
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
    except Exception:
        pass

    return conn


def _has_table(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _has_column(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]  # row[1] = column name
    return column in cols


def _ensure_products_schema(cur: sqlite3.Cursor) -> None:
    """
    Create products table (new installs) and migrate old installs.
    """
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

    # MIGRATION: if products exists but missing updated_at, add it
    # (This is what fixes: OperationalError('no such column: updated_at'))
    if _has_table(cur, "products") and not _has_column(cur, "products", "updated_at"):
        cur.execute("ALTER TABLE products ADD COLUMN updated_at TEXT;")

    # indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_desc ON products(full_description);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_mfg_code ON products(manufacturers_product_code);")


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # -------------------------
    # Products (CSV baseline)
    # -------------------------
    _ensure_products_schema(cur)

    # -------------------------
    # Barcode overrides (manual, survives CSV refreshes)
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

    # -------------------------
    # Barcode aliases: barcode -> product_code (BEST for scanning)
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

    # -------------------------
    # Stock Take
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

    # Bin expected products (what SHOULD be in a bin)
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
