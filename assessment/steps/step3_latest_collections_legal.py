# steps/step3_latest_collections_legal.py

import sqlite3
from .config import DB_PATH

# Reference date for "as of Nov 27th"
REFERENCE_DATETIME = "2025-11-27 23:59:59"

TARGET_QUEUES = ["COLLECTIONS", "LEGAL"]


def create_latest_status_table(conn):
    """
    Step 3:
      - determine which accounts are in target queues (COLLECTIONS / LEGAL)
        as of REFERENCE_DATETIME
      - for those accounts, find their most recent queue/status change,
        considering both daily updates and monthly snapshots.
    """
    cur = conn.cursor()

    # Drop if rerunning
    cur.execute("DROP TABLE IF EXISTS latest_status_collections_legal;")

    sql = f"""
    WITH all_events AS (
        -- Daily events
        SELECT
            account,
            queue,
            status,
            changed_datetime AS event_datetime,
            'DAILY' AS source
        FROM daily_status

        UNION ALL

        -- Monthly snapshots - use snapshot_date as event datetime at midnight
        SELECT
            account,
            queue,
            status,
            snapshot_date || ' 00:00:00' AS event_datetime,
            'MONTHLY' AS source
        FROM monthly_status
    ),
    events_parsed AS (
        SELECT
            account,
            queue,
            status,
            datetime(event_datetime) AS event_dt
        FROM all_events
        WHERE event_datetime IS NOT NULL
    ),

    -- State of each account as of the reference datetime
    state_as_of_ref AS (
        SELECT
            account,
            queue,
            status,
            event_dt,
            ROW_NUMBER() OVER (
                PARTITION BY account
                ORDER BY event_dt DESC
            ) AS rn
        FROM events_parsed
        WHERE event_dt <= datetime(?)
    ),

    -- Accounts that are in target queues at the reference datetime
    target_accounts AS (
        SELECT
            account
        FROM state_as_of_ref
        WHERE rn = 1
          AND UPPER(queue) IN ({",".join(["?" for _ in TARGET_QUEUES])})
    ),

    -- For those accounts, find their latest change (overall)
    latest_changes AS (
        SELECT
            e.account,
            e.queue,
            e.status,
            e.event_dt,
            ROW_NUMBER() OVER (
                PARTITION BY e.account
                ORDER BY e.event_dt DESC
            ) AS rn
        FROM events_parsed e
        JOIN target_accounts t
            ON t.account = e.account
    )

    SELECT
        account AS account_id,
        queue,
        status,
        event_dt AS latest_update_datetime
    FROM latest_changes
    WHERE rn = 1;
    """

    params = [REFERENCE_DATETIME] + [q.upper() for q in TARGET_QUEUES]

    cur.execute(f"""
        CREATE TABLE latest_status_collections_legal AS
        {sql}
    """, params)

    conn.commit()

    count = cur.execute("""
        SELECT COUNT(*) FROM latest_status_collections_legal;
    """).fetchone()[0]

    print(
        "[step3] Accounts in target queues as of "
        f"{REFERENCE_DATETIME} with a latest change: {count}"
    )


def debug_sample(conn, limit=10):
    cur = conn.cursor()
    rows = cur.execute(f"""
        SELECT account_id, queue, status, latest_update_datetime
        FROM latest_status_collections_legal
        ORDER BY account_id
        LIMIT {limit};
    """).fetchall()

    print("[step3] Sample latest changes for target accounts:")
    for r in rows:
        print(
            f"  account_id={r[0]}, queue={r[1]}, "
            f"status={r[2]}, latest_update_datetime={r[3]}"
        )


def run():
    print(f"[step3] Using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("[step3] Computing latest changes for accounts "
              "in COLLECTIONS or LEGAL as of reference date...")
        create_latest_status_table(conn)
        debug_sample(conn)
        print("[step3] Step 3 completed successfully.")
    finally:
        conn.close()
