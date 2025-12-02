# steps/step2_active_accounts.py

import sqlite3
from .config import DB_PATH

# Starting point required by the spec
START_DATE = "2025-01-01"


def create_active_accounts_table(conn):
    """
    Creates the active_accounts table and populates it with all accounts
    that have activity on or after START_DATE.

    In this context, "activity (queue or status change)" is defined as:
      - at least one daily_status record with changed_datetime >= START_DATE 00:00:00

    Daily updates are the canonical source of actual changes with precise timestamps.
    Monthly snapshots are derived state and are used later for state reconstruction,
    not for detecting new activity.
    """
    cur = conn.cursor()

    # Drop if rerunning
    cur.execute("DROP TABLE IF EXISTS active_accounts;")

    cur.execute("""
        CREATE TABLE active_accounts (
            account_id INTEGER PRIMARY KEY
        );
    """)

    sql = """
    INSERT INTO active_accounts (account_id)
    SELECT DISTINCT account AS account_id
    FROM daily_status
    WHERE changed_datetime >= ? || ' 00:00:00';
    """

    cur.execute(sql, (START_DATE,))
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM active_accounts;").fetchone()[0]
    print(f"[step2] Active accounts on or after {START_DATE}: {count}")


def debug_sample(conn, limit=10):
    """
    Optional helper to inspect a few active accounts with their names.
    """
    cur = conn.cursor()
    rows = cur.execute(f"""
        SELECT a.account_id, acc.name
        FROM active_accounts a
        LEFT JOIN accounts acc
            ON acc.account_id = a.account_id
        ORDER BY a.account_id
        LIMIT {limit};
    """).fetchall()

    print("[step2] Sample active accounts:")
    for r in rows:
        print(f"  account_id={r[0]}, name={r[1]}")


def run():
    """
    Executes Step 2:
      - identifies accounts with activity on or after START_DATE
      - persists them in active_accounts
    """
    print(f"[step2] Using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("[step2] Creating and populating active_accounts...")
        create_active_accounts_table(conn)

        debug_sample(conn)
        print("[step2] Step 2 completed successfully.")
    finally:
        conn.close()
