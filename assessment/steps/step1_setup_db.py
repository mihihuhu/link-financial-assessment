import sqlite3
import pandas as pd
import re

from .config import DATA_DIR, DB_PATH


def create_tables(conn):
    cur = conn.cursor()

    # Drop if rerunning
    cur.execute("DROP TABLE IF EXISTS daily_status;")
    cur.execute("DROP TABLE IF EXISTS monthly_status;")
    cur.execute("DROP TABLE IF EXISTS accounts;")

    # Accounts table
    cur.execute("""
        CREATE TABLE accounts (
            account_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT
        );
    """)

    # Daily status table
    cur.execute("""
        CREATE TABLE daily_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account INTEGER NOT NULL,
            queue TEXT,
            status TEXT,
            changed_datetime TEXT NOT NULL,
            FOREIGN KEY (account) REFERENCES accounts(account_id)
        );
    """)

    # Monthly table
    cur.execute("""
        CREATE TABLE monthly_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account INTEGER NOT NULL,
            queue TEXT,
            status TEXT,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            year INTEGER NOT NULL,
            snapshot_date TEXT NOT NULL,
            FOREIGN KEY (account) REFERENCES accounts(account_id)
        );
    """)

    conn.commit()


# -----------------------------------------------------------
# Load ACCOUNTS
# -----------------------------------------------------------

def load_accounts(conn):
    accounts_path = DATA_DIR / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"[step1] accounts.csv not found: {accounts_path}")

    df = pd.read_csv(accounts_path)

    # Validate required columns
    expected_cols = {"account_id", "name", "address"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"[step1] Missing required columns in accounts.csv: {missing}")

    # Deduplicate accounts by account_id (keep the first)
    if df["account_id"].duplicated().any():
        print("[step1] Warning: duplicate account_id rows detected. Deduplicating.")
        df = df.drop_duplicates(subset=["account_id"], keep="first")

    df.to_sql("accounts", conn, if_exists="append", index=False)
    print(f"[step1] Loaded {len(df)} rows into accounts")


# -----------------------------------------------------------
# Load DAILY STATUS
# -----------------------------------------------------------

def load_daily_status(conn):
    daily_files = sorted(DATA_DIR.glob("daily_*.csv"))

    if not daily_files:
        print("[step1] No daily_*.csv files found.")
        return

    frames = []

    for path in daily_files:
        df = pd.read_csv(path)

        expected_cols = {"account", "queue", "status", "changed_datetime"}
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"[step1] Missing columns in {path.name}: {missing}")

        raw = df["changed_datetime"].astype(str)

        # ISO-like: 2025-10-20 00:01:10 (YYYY-MM-DD HH:MM:SS)
        iso_mask = raw.str.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

        # Parse DD/MM/YYYY HH:MM:SS (and similar) with dayfirst=True
        dt_dayfirst = pd.to_datetime(
            raw.where(~iso_mask),
            errors="coerce",
            dayfirst=True
        )

        # Parse ISO strings with an explicit format
        dt_iso = pd.to_datetime(
            raw.where(iso_mask),
            errors="coerce",
            format="%Y-%m-%d %H:%M:%S"
        )

        # Combine both
        df["changed_datetime"] = dt_dayfirst.fillna(dt_iso)


        # Report invalid datetimes
        if df["changed_datetime"].isna().any():
            bad = df[df["changed_datetime"].isna()]
            print(f"[step1] Warning: invalid changed_datetime rows in {path.name}:")
            print(bad)
            # Drop invalid rows instead of inserting corrupt data
            df = df.dropna(subset=["changed_datetime"])

        # Convert to ISO
        df["changed_datetime"] = df["changed_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Remove fully duplicated rows
        df = df.drop_duplicates()

        frames.append(df)

    full_df = pd.concat(frames, ignore_index=True)

    # Final deduplication across all daily files
    full_df = full_df.drop_duplicates(subset=["account", "queue", "status", "changed_datetime"])

    full_df.to_sql("daily_status", conn, if_exists="append", index=False)
    print(f"[step1] Loaded {len(full_df)} rows into daily_status")


# -----------------------------------------------------------
# Load MONTHLY STATUS
# -----------------------------------------------------------
def load_monthly_status(conn):
    monthly_files = sorted(DATA_DIR.glob("monthly_*.csv"))
    if not monthly_files:
        print("[step1] No monthly_*.csv files found")
        return

    frames = []
    for path in monthly_files:
        df = pd.read_csv(path)

        expected_cols = {"account", "queue", "status", "month", "day"}
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"[step1] Missing columns in {path.name}: {missing}")

        # Extract year from file name: monthly_YYYYMM*.csv
        fname = path.name
        try:
            year = int(fname.split("_")[1][:4])
        except Exception:
            raise ValueError(f"[step1] Cannot extract year from filename: {fname}")

        # Ensure numeric month/day
        df["month"] = pd.to_numeric(df["month"], errors="coerce")
        df["day"] = pd.to_numeric(df["day"], errors="coerce")

        # Drop rows where month/day are invalid
        before = len(df)
        df = df.dropna(subset=["month", "day"])
        after = len(df)
        if after < before:
            print(f"[step1] Dropped {before - after} rows with invalid month/day in {path.name}")

        df["month"] = df["month"].astype(int)
        df["day"] = df["day"].astype(int)

        # Set year column for all remaining rows
        df["year"] = year

        # Build snapshot_date as YYYY-MM-DD
        df["snapshot_date"] = df.apply(
            lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r['day']):02d}",
            axis=1
        )

        # Deduplicate per file
        df = df.drop_duplicates(
            subset=["account", "queue", "status", "year", "month", "day"]
        )

        frames.append(df)

    full_df = pd.concat(frames, ignore_index=True)

    # Final sanity check: drop any rows missing critical fields
    before = len(full_df)
    full_df = full_df.dropna(subset=["account", "year", "month", "day", "snapshot_date"])
    after = len(full_df)
    if after < before:
        print(f"[step1] Dropped {before - after} rows with missing year/month/day/snapshot_date in monthly_status")

    # Enforce integer types
    full_df["year"] = full_df["year"].astype(int)
    full_df["month"] = full_df["month"].astype(int)
    full_df["day"] = full_df["day"].astype(int)

    # Deduplicate across all files
    full_df = full_df.drop_duplicates(
        subset=["account", "queue", "status", "year", "month", "day"]
    )

    full_df.to_sql("monthly_status", conn, if_exists="append", index=False)
    print(f"[step1] Loaded {len(full_df)} rows into monthly_status")


# -----------------------------------------------------------
# Run Step 1
# -----------------------------------------------------------

def run():
    """Executes Step 1: schema creation + data loading + validation."""
    print(f"[step1] Using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("[step1] Creating tables...")
        create_tables(conn)

        print("[step1] Loading accounts...")
        load_accounts(conn)

        print("[step1] Loading daily_status...")
        load_daily_status(conn)

        print("[step1] Loading monthly_status...")
        load_monthly_status(conn)

        print("[step1] Step 1 completed successfully.")
    finally:
        conn.close()
