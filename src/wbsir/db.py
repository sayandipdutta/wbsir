from contextlib import closing
import sqlite3
from pathlib import Path
import logging

DB_PATH = Path("wbsir.db")
INIT_SCRIPT_PATH = Path("initdb.sql")


logger = logging.getLogger(__name__)


def init_db():
    if not INIT_SCRIPT_PATH.exists():
        raise FileNotFoundError(f"{INIT_SCRIPT_PATH} not found")
    logger.info(f"Initializing database at {DB_PATH}")
    DB_PATH.unlink(missing_ok=True)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        _ = conn.executescript(INIT_SCRIPT_PATH.read_text())
    logger.info("Database initialized.")


if __name__ == "__main__":
    init_db()
