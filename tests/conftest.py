import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")


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
