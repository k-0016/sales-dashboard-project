import logging
import pytest
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# -------------------------------
# Logging setup (applies to all tests)
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -------------------------------
# Database fixture
# -------------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER", "salesuser")
DB_PASS = os.getenv("DB_PASS", "salespass")
DB_NAME = os.getenv("DB_NAME", "salesdb")
DB_HOST = os.getenv("DB_HOST", "postgres_db")  # for Docker networking
DB_PORT = os.getenv("DB_PORT", "5432")

@pytest.fixture(scope="session")
def engine():
    """Provide a reusable SQLAlchemy engine for all tests"""
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)
