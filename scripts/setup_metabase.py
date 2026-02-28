import requests
import json
import time

METABASE_URL = "http://localhost:3000"
EMAIL = "admin@mini-data-platform.com"
PASSWORD = "admin123"

DB_CONFIG = {
    "name": "Mini Data Platform",
    "engine": "postgres",
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def wait_for_metabase():
    print("Waiting for Metabase to be ready...")
    for i in range(30):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200:
                print("Metabase is ready!")
                return True
        except:
            pass
        time.sleep(2)
    return False

def setup_metabase():
    if not wait_for_metabase():
        print("Metabase not ready")
        return None

    session = requests.Session()
    
    # Create admin user
    print("Creating admin user...")
    r = session.post(f"{METABASE_URL}/api/setup", json={
        "email": EMAIL,
        "first_name": "Admin",
        "last_name": "User",
        "password": PASSWORD,
        "site_name": "Mini Data Platform"
    })
    
    # Try to login (if user already exists)
    if r.status_code != 200:
        print("Admin user may already exist, attempting login...")
        r = session.post(f"{METABASE_URL}/api/session", json={
            "email": EMAIL,
            "password": PASSWORD
        })
        if r.status_code != 200:
            # Try default admin
            r = session.post(f"{METABASE_URL}/api/session", json={
                "email": "admin@metabase.com",
                "password": "admin"
            })
    
    if r.status_code == 200:
        token = r.json().get("id") or r.json().get("token")
        session.headers.update({"X-Metabase-Session": token})
        print("Logged in successfully!")
        return session
    
    print(f"Login failed: {r.status_code} - {r.text}")
    return None

def add_database(session):
    print("Adding database...")
    r = session.post(f"{METABASE_URL}/api/database", json=DB_CONFIG)
    if r.status_code in [200, 201]:
        db = r.json()
        print(f"Database added: {db.get('id')}")
        return db.get('id')
    else:
        print(f"Failed to add database: {r.status_code} - {r.text}")
        return None

def create_collection(session, name):
    r = session.post(f"{METABASE_URL}/api/collection", json={
        "name": name,
        "color": "#509EE3"
    })
    if r.status_code in [200, 201]:
        return r.json().get('id')
    return None

def create_question(session, db_id, name, query, collection_id=None):
    payload = {
        "name": name,
        "database_id": db_id,
        "dataset_query": {
            "type": "query",
            "query": {
                "source_table": None,
                "aggregation": ["count"],
                "breakout": []
            },
            "database": db_id
        },
        "display": "table",
        "visualization_settings": {}
    }
    
    # Parse simple SQL
    sql = query.strip().lower()
    if sql.startswith("select"):
        payload["dataset_query"]["type"] = "native"
        payload["dataset_query"]["native"] = {
            "query": query,
            "template_tags": {}
        }
    
    if collection_id:
        payload["collection_id"] = collection_id
    
    r = session.post(f"{METABASE_URL}/api/card", json=payload)
    if r.status_code in [200, 201]:
        print(f"Created question: {name}")
        return r.json().get('id')
    else:
        print(f"Failed to create question {name}: {r.status_code} - {r.text}")
        return None

def create_dashboard(session, name, collection_id=None):
    payload = {"name": name}
    if collection_id:
        payload["collection_id"] = collection_id
    
    r = session.post(f"{METABASE_URL}/api/dashboard", json=payload)
    if r.status_code in [200, 201]:
        return r.json().get('id')
    return None

def main():
    session = setup_metabase()
    if not session:
        print("Failed to setup Metabase")
        return
    
    db_id = add_database(session)
    if not db_id:
        print("Failed to add database")
        return
    
    coll_id = create_collection(session, "Sales Analytics")
    if not coll_id:
        coll_id = 1
    
    # Create dashboards
    questions = [
        ("Daily Sales", "SELECT sale_date, total_transactions, net_revenue FROM gold.daily_sales ORDER BY sale_date DESC LIMIT 30"),
        ("Product Performance", "SELECT product_name, category, total_revenue, number_of_transactions FROM gold.product_performance ORDER BY total_revenue DESC LIMIT 20"),
        ("Customer Analytics", "SELECT customer_name, customer_tier, total_revenue, total_purchases FROM gold.customer_analytics ORDER BY total_revenue DESC LIMIT 20"),
        ("Store Performance", "SELECT store_location, region, total_revenue, total_transactions FROM gold.store_performance ORDER BY total_revenue DESC"),
        ("Category Insights", "SELECT category, total_revenue, total_products_sold, average_order_value FROM gold.category_insights ORDER BY total_revenue DESC"),
    ]
    
    for name, query in questions:
        create_question(session, db_id, name, query, coll_id)
    
    print("\n=== Metabase Setup Complete ===")
    print(f"URL: {METABASE_URL}")
    print(f"Email: {EMAIL}")
    print(f"Password: {PASSWORD}")
    print(f"Database: airflow (PostgreSQL)")

if __name__ == "__main__":
    main()
