import os
import sys
import tempfile

import pytest

root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Highest priority last (inserted at 0, so last = first on path)
# dags/ must come before scripts/ so dags/utils shadows scripts/utils
_paths = [
    os.path.join(root, "scripts", "data_generator"),
    os.path.join(root, "scripts"),
    os.path.join(root, "dags", "etl"),
    os.path.join(root, "dags"),
    root,
]
for p in _paths:
    p = os.path.abspath(p)
    # Remove existing entry to avoid duplicates at wrong position
    while p in sys.path:
        sys.path.remove(p)
    sys.path.insert(0, p)

# Clear cached 'utils' so dags/utils takes precedence over scripts/utils
for mod_name in list(sys.modules):
    if mod_name == "utils" or mod_name.startswith("utils."):
        del sys.modules[mod_name]


def pytest_collectstart(collector):
    """Before each module collection, ensure dags/ is at front of sys.path
    and clear cached utils module (generator.py pushes scripts/ to front)."""
    dags_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))
    if sys.path[0] != dags_path:
        while dags_path in sys.path:
            sys.path.remove(dags_path)
        sys.path.insert(0, dags_path)
    for mod_name in list(sys.modules):
        if mod_name == "utils" or mod_name.startswith("utils."):
            del sys.modules[mod_name]


os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("LOG_DIR", tempfile.mkdtemp())


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for testing"""

    class MockConnection:
        def __init__(self):
            self.closed = False

        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

        def close(self):
            self.closed = True

    class MockCursor:
        def __init__(self):
            self.executed_queries = []

        def execute(self, query, params=None):
            self.executed_queries.append((query, params))

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    return MockConnection()


@pytest.fixture
def sample_sales_data():
    """Sample sales data for testing"""
    return [
        {"sale_id": 1, "transaction_id": "TXN001", "customer_id": "CUST001", "product_id": "PROD001"},
        {"sale_id": 2, "transaction_id": "TXN002", "customer_id": "CUST002", "product_id": "PROD002"},
        {"sale_id": 3, "transaction_id": "TXN003", "customer_id": "CUST001", "product_id": "PROD003"},
    ]


@pytest.fixture
def sample_quarantine_data():
    """Sample quarantine data for testing"""
    return [
        {"id": 1, "payload": '{"sale_id": 1}', "error_reason": "NULL customer_id", "replayed": False},
        {"id": 2, "payload": '{"sale_id": 2}', "error_reason": "Invalid price", "replayed": True},
    ]
