from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).parent / "midlands.db"

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # -------------------------
    # Products (price checker search)
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
          product_code TEXT PRIMARY KEY,
          full_description TEXT NOT NULL,
          retail_price REAL NOT NULL DEFAULT 0,
          manufacturers_product_code TEXT,
          barcode TEXT
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_desc ON products(full_description);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_mfg_code ON products(manufacturers_product_code);")

    # -------------------------
    # Kerridge Bridge Queue
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS process_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('pending','in_progress','completed','cancelled')),
          payload_json TEXT NOT NULL,
          requested_by TEXT,
          kerridge_order_number TEXT
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_process_requests_status ON process_requests(status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_process_requests_created_at ON process_requests(created_at);")

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

    # Bin → Products mapping (drives bin dropdown + baseline)
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