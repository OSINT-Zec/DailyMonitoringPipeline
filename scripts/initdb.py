#!/usr/bin/env python3
"""
Initialize (or verify) the PostgreSQL schema for the OSINT Monitor.

- Loads environment variables from project root `.env`
- Connects to the database specified by POSTGRES_URL
- Calls `src.models.ensure_schema(engine)` to create/upgrade tables
"""

from __future__ import annotations

import sys
import os
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Ensure `src` is importable (project root is parent of this file's directory)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from src.models import ensure_schema
except Exception as e:
    print(f"ERROR: Could not import src.models.ensure_schema: {e}", file=sys.stderr)
    sys.exit(2)

# ---------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def load_env() -> None:
    """Load .env from project root if present."""
    # Prefer an explicit path, else fall back to auto-discovery
    explicit = ROOT / ".env"
    if explicit.exists():
        load_dotenv(dotenv_path=explicit)
    else:
        # find_dotenv searches upward from CWD; this is a safe fallback
        env_path = find_dotenv()
        if env_path:
            load_dotenv(env_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize/verify PostgreSQL schema for OSINT Monitor."
    )
    parser.add_argument(
        "--url",
        dest="url",
        default=None,
        help="Override POSTGRES_URL (postgresql://user:pass@host:port/dbname)",
    )
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()

    url = args.url or os.getenv("POSTGRES_URL")
    if not url:
        logging.error("POSTGRES_URL is not set (and no --url provided).")
        return 2

    logging.info("Connecting to database …")
    engine = None
    try:
        engine = create_engine(url, pool_pre_ping=True, future=True)
        # Simple connectivity check
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1;")

        logging.info("Ensuring schema …")
        ensure_schema(engine)

        logging.info("✅ DB schema ensured.")
        return 0
    except SQLAlchemyError as db_err:
        logging.error("Database error while ensuring schema: %s", db_err)
        return 1
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return 1
    finally:
        if engine is not None:
            engine.dispose()
            logging.debug("Engine disposed.")


if __name__ == "__main__":
    sys.exit(main())

