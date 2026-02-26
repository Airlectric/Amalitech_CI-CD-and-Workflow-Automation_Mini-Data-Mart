key = "sales/ingest_date=2026-02-26/sales_c50a31eb.parquet"
print(f"Key: {key}")
print(f"Contains /sales/: {'/sales/' in key}")
print(f"Contains sales/: {'sales/' in key}")
