import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


def generate_sales_data(num_rows: int = 1000) -> pd.DataFrame:
    np.random.seed(42)
    random.seed(42)
    
    start_date = datetime(2024, 1, 1)
    
    data = {
        "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
        "timestamp": [
            (start_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).isoformat() for _ in range(num_rows)
        ],
        "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(num_rows)],
        "customer_name": [
            random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "FrankGrace", "Henry", ""])
            + " " +
            random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"])
            for _ in range(num_rows)
        ],
        "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(num_rows)],
        "product_name": [
            random.choice(["Laptop", "Phone", "Tablet", "Headphones", "Camera", "Watch", "Speaker", "Monitor", "Keyboard", "Mouse"])
            + " " +
            random.choice(["Pro", "Plus", "Max", "Ultra", "Lite", "Standard", "Premium"])
            for _ in range(num_rows)
        ],
        "category": random.choices(
            ["Electronics", "Accessories", "Software", "Services", "Peripherals"],
            weights=[40, 25, 15, 12, 8],
            k=num_rows
        ),
        "quantity": np.random.randint(1, 10, size=num_rows),
        "unit_price": np.round(np.random.uniform(9.99, 1999.99, size=num_rows), 2),
        "discount_percentage": np.round(np.random.uniform(0, 25, size=num_rows), 2),
        "payment_method": random.choices(
            ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"],
            weights=[35, 25, 20, 15, 5],
            k=num_rows
        ),
        "store_location": random.choices(
            ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
             "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
            k=num_rows
        ),
    }
    
    df = pd.DataFrame(data)
    
    df["discount_amount"] = np.round(df["unit_price"] * df["discount_percentage"] / 100, 2)
    df["total_amount"] = np.round(df["unit_price"] * df["quantity"] - df["discount_amount"], 2)
    
    column_order = [
        "transaction_id", "timestamp", "customer_id", "customer_name",
        "product_id", "product_name", "category", "quantity", 
        "unit_price", "discount_percentage", "discount_amount",
        "total_amount", "payment_method", "store_location"
    ]
    
    df = df[column_order]
    
    return df


def save_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Generated {len(df)} rows -> {output_path}")


def generate_batch(batch_size: int = 1000, num_batches: int = 1, output_dir: str = "data/raw") -> list[str]:
    generated_files = []
    
    for batch in range(1, num_batches + 1):
        df = generate_sales_data(num_rows=batch_size)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sales_data_batch_{batch}_{timestamp}.parquet"
        filepath = os.path.join(output_dir, filename)
        save_to_parquet(df, filepath)
        generated_files.append(filepath)
    
    return generated_files


if __name__ == "__main__":
    files = generate_batch(batch_size=1000, num_batches=1, output_dir="data/raw")
    print(f"\nGenerated {len(files)} file(s)")
