import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "scanner.db"

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS universe_sp500 (
            ticker TEXT PRIMARY KEY
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS realized_vol (
            ticker TEXT NOT NULL,
            window INTEGER NOT NULL,
            asof_date TEXT NOT NULL,
            rv REAL NOT NULL,
            PRIMARY KEY (ticker, window, asof_date)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS spot_close (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL NOT NULL,
            PRIMARY KEY (ticker, date)
        )
        """)
        conn.commit()
