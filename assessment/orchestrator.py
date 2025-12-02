from steps.step1_setup_db import run as run_step1
from steps.step2_active_accounts import run as run_step2
from steps.step3_latest_collections_legal import run as run_step3
from steps.step4_final_table import run as run_step4
from steps.step5_performance import run as run_step5


def main():
    print("Starting orchestrator")

    # Step 1: create tables and load raw data
    run_step1()

    # Step 2: identify active accounts from 2025-01-01 onwards
    run_step2()

    # Step 3: compute latest changes for accounts that
    # are in COLLECTIONS or LEGAL as of 2025-11-27
    run_step3()

    # Step 4: build final output table with account details
    run_step4()

    # Step 5: measure and optimize query performance
    run_step5()

    print("Orchestration finished")


if __name__ == "__main__":
    main()
