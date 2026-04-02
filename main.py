"""
MBTA Error Data Collection
"""

import sqlite3
from pathlib import Path
import logging
import os
import signal
import threading

from dotenv import load_dotenv
import click

from alert_stream import AlertStream
from prediction_stream import PredictionStream

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(filename)s:%(funcName)s — %(levelname)s — %(message)s"
)
logging.getLogger("httpx").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

DB_INIT_SCRIPT_PATH = Path("./init_db.sql")


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


@click.command()
@click.argument(
    "db_path",
    type=click.Path(path_type=Path, allow_dash=False),
)
def main(db_path: Path):
    """
    Run the data collection

    DB_PATH is path to new or current SQLite DB. Will be created and setup if needed.
    """

    logger.info("Starting up MBTA error data collection")
    logger.info("DB init script path: %s", DB_INIT_SCRIPT_PATH)
    logger.info("DB path: %s", db_path)

    setup_db(db_path)

    alert_stream = AlertStream()
    prediction_stream = PredictionStream(db_path)

    logger.info("Starting stream reader threads")

    alert_thread = threading.Thread(target=alert_stream.open, name="AlertThread")
    prediction_thread = threading.Thread(
        target=prediction_stream.open, name="PredictionThread"
    )

    alert_thread.start()
    prediction_thread.start()

    while alert_thread.is_alive() and prediction_thread.is_alive():
        alert_thread.join(timeout=1)

    dead = "AlertThread" if not alert_thread.is_alive() else "PredictionThread"
    logger.critical("%s died — shutting down", dead)
    os.kill(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
