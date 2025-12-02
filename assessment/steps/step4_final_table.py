import sqlite3
import pandas as pd
from .config import DB_PATH

def create_final_table(conn):
    cur = conn.cursor()

    # Drop table if rerunning
    cur.execute("DROP TABLE IF EXISTS final_latest_accounts;")

    # Create output structure
    cur.execute("""
        CREATE TABLE final_latest_accounts (
            account_id INTEGER PRIMARY KEY,
            name TEXT,
            address TEXT,
            latest_update_datetime TEXT,
            queue TEXT,
            status TEXT
        );
    """)

    # Merge last changes (from step3) with account details
    sql = """
        INSERT INTO final_latest_accounts (
            account_id, name, address,
            latest_update_datetime, queue, status
        )
        SELECT
            lc.account_id,
            acc.name,
            acc.address,
            lc.latest_update_datetime,
            lc.queue,
            lc.status
        FROM latest_status_collections_legal lc
        LEFT JOIN accounts acc
            ON acc.account_id = lc.account_id;
    """

    cur.execute(sql)
    conn.commit()

def preview_results(conn, limit=10):
    cur = conn.cursor()
    rows = cur.execute(f"""
        SELECT *
        FROM final_latest_accounts
        ORDER BY latest_update_datetime DESC
        LIMIT {limit};
    """).fetchall()

    print("[step4] Sample rows from final dataset:")
    for r in rows:
        print(r)

def export_to_csv(conn):
    df = pd.read_sql_query("SELECT * FROM final_latest_accounts", conn)
    df.to_csv("final_latest_accounts.csv", index=False)
    print("[step4] Exported final_latest_accounts.csv")

def run():
    print(f"[step4] Using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("[step4] Creating and populating final_latest_accounts...")
        create_final_table(conn)

        count = conn.execute("SELECT COUNT(*) FROM final_latest_accounts;").fetchone()[0]
        print(f"[step4] Rows in final_latest_accounts: {count}")

        preview_results(conn)
        export_to_csv(conn)

        print("[step4] Step 4 completed successfully.")
    finally:
        conn.close()
