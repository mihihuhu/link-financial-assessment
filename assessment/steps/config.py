import os
from pathlib import Path

# Determine project root dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Path to the data folder containing CSV files
DATA_DIR = PROJECT_ROOT / "data"

# Path to the SQLite database
DB_PATH = PROJECT_ROOT / "data.db"

# Ensure directories exist
if not DATA_DIR.exists():
    raise FileNotFoundError(f"[config] Data directory not found: {DATA_DIR}")
