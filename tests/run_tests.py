"""
Entrypoint for running all pytest tests inside Airflow DAG
- Discovers all tests under /opt/airflow/dags/tests/
- Logs a summary of results
- Exits non-zero if any test fails (so Airflow task fails)
"""

import logging
import sys
import pytest

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("tests.runner")

# Constants
TEST_PATH = "/opt/airflow/dags/tests/"


def main():
    """Run pytest programmatically and log results"""
    log.info(f"▶️ Starting pytest for directory: {TEST_PATH}")
    
    # Run pytest (capture result code)
    result = pytest.main([TEST_PATH, "-v", "--disable-warnings"])
    
    if result == 0:
        log.info("✅ All tests passed successfully")
    else:
        log.error(f"❌ Tests failed with exit code {result}")
        sys.exit(result)


if __name__ == "__main__":
    main()

