# Upstream encode/broadcaster test files, kept byte-for-byte unchanged, to check
# that the fork stays API compatible:
#   test_*_0_3_1.py      from tag 0.3.1 (the last release)
#   test_*_master.py     from master 6b3ea71 (unreleased 0.3.2)
# They hardcode localhost:5432 for Postgres. Run with: pytest tests_upstream
from tests.conftest import pytest_runtest_setup  # noqa: F401  (pre-create Kafka topic)
