from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional, List, Any
import duckdb
import pandas as pd
import os
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DuckDB Query API",
    description="Query MinIO/S3 directly using DuckDB schema-on-read",
    version="1.0.0"
)

templates = Jinja2Templates(directory="templates")

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
    "use_ssl": os.getenv("MINIO_USE_SSL", "false").lower() == "true"
}


def get_duckdb_connection():
    conn = duckdb.connect()
    
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        conn.execute(f"""
            SET s3_endpoint='{MINIO_CONFIG['endpoint']}';
            SET s3_access_key_id='{MINIO_CONFIG['access_key']}';
            SET s3_secret_access_key='{MINIO_CONFIG['secret_key']}';
            SET s3_use_ssl={str(MINIO_CONFIG['use_ssl']).lower()};
            SET s3_url_style='path';
        """)
        logger.info("DuckDB httpfs configured for MinIO")
    except Exception as e:
        logger.warning(f"Failed to setup httpfs: {e}")
    
    return conn


class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    rows: int
    columns: List[str]
    data: List[Any]
    execution_time_ms: float


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "service": "DuckDB Query API",
        "version": "1.0.0"
    })


@app.get("/datasets", response_class=HTMLResponse)
def list_datasets(request: Request):
    conn = get_duckdb_connection()
    
    datasets = []
    
    try:
        conn.execute("SELECT 1 as test FROM read_parquet('s3://bronze/**/*.parquet') LIMIT 1")
        datasets.append({
            "bucket": "bronze",
            "status": "available",
            "note": "Raw ingestion data"
        })
    except Exception as e:
        error_msg = str(e)
        if "No files" in error_msg:
            datasets.append({"bucket": "bronze", "status": "empty", "note": "No data in bucket"})
        else:
            datasets.append({"bucket": "bronze", "status": "error", "note": error_msg[:80]})
    
    conn.close()
    
    return templates.TemplateResponse("datasets.html", {
        "request": request,
        "datasets": datasets,
        "service": "DuckDB Query API"
    })


@app.get("/explore/{bucket}", response_class=HTMLResponse)
def explore_bucket(request: Request, bucket: str):
    conn = get_duckdb_connection()
    
    try:
        query = f"SELECT file_name FROM parquet_schema('s3://{bucket}/**/*.parquet') LIMIT 200"
        result = conn.execute(query)
        df = result.df()
        
        folders = set()
        for path in df['file_name'].astype(str):
            match = re.match(r's3://[^/]+/([^/]+)/', path)
            if match:
                folders.add(match.group(1))
        
        datasets_list = [{"dataset_name": f, "file_count": "multiple"} for f in sorted(folders)]
        
        conn.close()
        
        return templates.TemplateResponse("explore.html", {
            "request": request,
            "bucket": bucket,
            "datasets": datasets_list,
            "service": "DuckDB Query API"
        })
        
    except Exception as e:
        conn.close()
        return templates.TemplateResponse("explore.html", {
            "request": request,
            "bucket": bucket,
            "error": str(e),
            "service": "DuckDB Query API"
        })


@app.get("/explore/{bucket}/{dataset}", response_class=HTMLResponse)
def explore_dataset(request: Request, bucket: str, dataset: str):
    conn = get_duckdb_connection()
    
    try:
        query = f"SELECT * FROM read_parquet('s3://{bucket}/{dataset}/**/*.parquet') LIMIT 100"
        result = conn.execute(query)
        df = result.df()
        
        columns = list(df.columns)
        data = df.head(50).values.tolist()
        row_count = len(df)
        
        conn.close()
        
        return templates.TemplateResponse("explore.html", {
            "request": request,
            "bucket": bucket,
            "dataset": dataset,
            "columns": columns,
            "data": data,
            "row_count": row_count,
            "service": "DuckDB Query API"
        })
        
    except Exception as e:
        conn.close()
        return templates.TemplateResponse("explore.html", {
            "request": request,
            "bucket": bucket,
            "dataset": dataset,
            "error": str(e),
            "service": "DuckDB Query API"
        })


@app.post("/query", response_model=QueryResponse)
def execute_query(request: QueryRequest):
    import time
    start_time = time.time()
    
    conn = get_duckdb_connection()
    
    try:
        result = conn.execute(request.query)
        df = result.df()
        
        execution_time = (time.time() - start_time) * 1000
        
        return QueryResponse(
            rows=len(df),
            columns=list(df.columns),
            data=df.values.tolist(),
            execution_time_ms=round(execution_time, 2)
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    finally:
        conn.close()


@app.post("/query-ui", response_class=HTMLResponse)
def execute_query_ui(request: Request, query: str = ""):
    if not query:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "service": "DuckDB Query API",
            "version": "1.0.0",
            "error": "Please enter a query"
        })
    
    conn = get_duckdb_connection()
    
    try:
        result = conn.execute(query)
        df = result.df()
        
        columns = list(df.columns)
        data = df.values.tolist()
        row_count = len(df)
        
        conn.close()
        
        return templates.TemplateResponse("index.html", {
            "request": request,
            "service": "DuckDB Query API",
            "version": "1.0.0",
            "query": query,
            "columns": columns,
            "data": data[:100],
            "row_count": row_count,
            "execution_time": "N/A"
        })
        
    except Exception as e:
        conn.close()
        return templates.TemplateResponse("index.html", {
            "request": request,
            "service": "DuckDB Query API",
            "version": "1.0.0",
            "query": query,
            "error": str(e)
        })


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/sample-queries")
def sample_queries():
    return {
        "queries": {
            "list_files": "SELECT * FROM read_parquet('s3://bronze/**/*.parquet') LIMIT 10",
            "filter_by_date": "SELECT * FROM read_parquet('s3://bronze/**/*.parquet') WHERE sale_date = '2026-02-26'",
            "aggregate": "SELECT category, COUNT(*) as count, SUM(net_amount) as total FROM read_parquet('s3://bronze/**/*.parquet') GROUP BY category",
            "describe": "DESCRIBE SELECT * FROM read_parquet('s3://bronze/**/*.parquet') LIMIT 1"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
