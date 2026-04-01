"""
MBTA Error Data Collection
"""

import sqlite3
from pathlib import Path
import os
import logging

from dotenv import load_dotenv
import click

load_dotenv()

logging.basicConfig()
logger = logging.getLogger()

DB_INIT_SCRIPT_PATH = Path("./init_db.sql")
MBTA_API_KEY = os.environ["MBTA_API_KEY"]


def setup_db(db_path: Path):
    """
    Sets up the DB on startup
    """

    if not DB_INIT_SCRIPT_PATH.is_file():
        raise ValueError(f"DB_INIT_SCRIPT_PATH not a file: {DB_INIT_SCRIPT_PATH}")

    with open(DB_INIT_SCRIPT_PATH, mode="r", encoding="utf-8") as f:
        init_script = f.read()

    # creates the db if it does not exist
    db_path.touch()

    with sqlite3.connect(db_path) as conn:
        conn.executescript(init_script)

    logger.info("DB successfully setup")
