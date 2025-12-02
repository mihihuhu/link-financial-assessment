import sqlite3
import time
from .config import DB_PATH


def run_query(conn, repeats=5):
    """
    Run the target query multiple times and return:
      - average execution time
      - number of rows returned (from the last run)
    """
    sql = """
        SELECT
            ds.account,
            ds.queue,
            ds.status,
            ds.changed_datetime,
            a.name,
            a.address
        FROM daily_status ds
        JOIN accounts a
            ON a.account_id = ds.account
        WHERE ds.account IN (
            SELECT account_id
            FROM latest_status_collections_legal
        )
          AND UPPER(ds.queue) IN ('COLLECTIONS', 'LEGAL');
    """

    cur = conn.cursor()
    total_time = 0.0
    row_count = 0

    for _ in range(repeats):
        start = time.perf_counter()
        rows = cur.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        total_time += elapsed
        row_count = len(rows)

    avg_time = total_time / repeats if repeats > 0 else 0.0
    return avg_time, row_count


def drop_indexes(conn):
    """
    Drop indexes, if they exist.
    """
    cur = conn.cursor()
    # IF EXISTS syntax to avoid errors if indexes were not created
    cur.execute("DROP INDEX IF EXISTS idx_daily_account_queue_changed;")
    cur.execute("DROP INDEX IF EXISTS idx_daily_queue_changed;")
    cur.execute("DROP INDEX IF EXISTS idx_latest_status_account;")
    conn.commit()


def create_indexes(conn):
    """
    Create indexes to optimize the target query.
    """
    cur = conn.cursor()

    # Index helping the WHERE and JOIN on daily_status:
    #   - filter by queue
    #   - filter by account
    #   - sort/scan by changed_datetime if needed
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_account_queue_changed
        ON daily_status (account, queue, changed_datetime);
    """)

    # Optional index focused on queue / datetime scans
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_queue_changed
        ON daily_status (queue, changed_datetime);
    """)

    # Index on latest_status_collections_legal to speed up the IN (subquery)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_latest_status_account
        ON latest_status_collections_legal (account_id);
    """)

    conn.commit()


def run():
    print(f"[step5] Using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # 1. Baseline: no indexes
        print("[step5] Dropping indexes (if any) for baseline measurement...")
        drop_indexes(conn)

        print("[step5] Measuring performance BEFORE indexing...")
        avg_before, rows = run_query(conn, repeats=5)
        print(
            f"[step5] Baseline: {rows} rows, "
            f"average time over 5 runs: {avg_before:.6f} seconds"
        )

        # 2. Create indexes
        print("[step5] Creating indexes for optimization...")
        create_indexes(conn)
        print("[step5] Indexes created.")

        # 3. Measure again
        print("[step5] Measuring performance AFTER indexing...")
        avg_after, rows_after = run_query(conn, repeats=5)
        print(
            f"[step5] With indexes: {rows_after} rows, "
            f"average time over 5 runs: {avg_after:.6f} seconds"
        )

        # 4. Final comparison
        print("[step5] Final comparison:")
        print(f"        Baseline avg: {avg_before:.6f} s")
        print(f"        Indexed avg: {avg_after:.6f} s")

    finally:
        conn.close()
        print("[step5] Step 5 completed.")
