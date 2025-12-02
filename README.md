This project implements a data processing pipeline using Python and SQLite. The entire solution is modular, and aligned with the assessment requirements.

Step 1 – Database Setup and Data Loading

File: steps/step1_setup_db.py

Creates tables: accounts, daily_status, monthly_status

Loads: accounts.csv, all daily_.csv, all monthly_.csv

Key processing:

- Daily field changed_datetime is parsed and normalized to YYYY-MM-DD HH:MM:SS

- Monthly (month, day) is combined with the year extracted from the filename, producing a proper snapshot_datetime

Data validation ensures:

- required columns exist

- day/month values within valid ranges

- invalid datetimes are logged and excluded

Step 2 – Identify Active Accounts From 2025-01-01

File: steps/step2_active_accounts.py

Definition of “activity”: A queue or status change occurring on or after 2025-01-01

The canonical source of true change events is daily_status, since it contains exact timestamps

Monthly snapshots represent derived state (state at the beginning of each month reflecting changes from the previous month), so they are not used to infer new post-cutoff activity

Results are stored in: active_accounts(account_id) => 499 active accounts.

Step 3 – Latest Queue/Status Change for Target Accounts

File: steps/step3_latest_collections_legal.py

For accounts that as of Nov 27th are in queues COLLECTIONS or LEGAL, determine their most recent queue and/or status change, considering both daily updates and monthly snapshots.

Processing:

- Convert monthly snapshots to full timestamps (snapshot_datetime)

- Union daily and monthly events into all_events

- Reconstruct the state of each account as of 2025-11-27 23:59:59

- Select accounts whose queue at that moment is COLLECTIONS or LEGAL

- For those accounts, compute their latest overall change (daily or monthly)

- Results stored in: latest_status_collections_legal => 144 accounts meet the criteria

Step 4 – Final Output Table

File: steps/step4_final_table.py

Produces the final dataset required by the assessment:

This joins: latest_status_collections_legal, accounts and exports the result to final_latest_accounts.csv

Step 5 – Performance Measurement & Optimization

File: steps/step5_performance.py

Performance Results (391 rows): Measurement Avg over 5 runs Before indexing 0.005997 s After indexing 0.003131 s

≈ 48 percent faster

Indexes significantly improved filtering by queue and subquery resolution.

How to Run - run the entire pipeline: python orchestrator.py

Outputs generated:

SQLite table: final_latest_accounts

CSV export: final_latest_accounts.csv

Database file: data.db
