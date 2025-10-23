#!/usr/bin/env python3
"""
Test script to verify Gold job can read Silver tables.
Run with: spark-submit --jars ... test_gold_connection.py
"""
import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")

spark = SparkSession.builder.appName("TestGoldConnection") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("Testing Silver table access for Gold aggregation")
print("="*60)

tables_to_check = [
    "fact_consultation",
    "fact_hospitalisation", 
    "fact_deces",
    "dim_patient",
    "dim_etablissement"
]

for table in tables_to_check:
    path = f"s3a://silver/{table}"
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        count = df.count()
        cols = df.columns[:10]  # first 10 columns
        print(f"\n✅ {table}")
        print(f"   Rows: {count:,}")
        print(f"   Columns ({len(df.columns)}): {', '.join(cols)}...")
        
        # Check for date columns
        date_cols = [c for c in df.columns if 'date' in c.lower()]
        if date_cols:
            print(f"   Date columns: {', '.join(date_cols)}")
            # Show date range
            for dc in date_cols[:2]:
                try:
                    df.select(dc).summary("min", "max").show()
                except:
                    pass
    except Exception as e:
        print(f"\n❌ {table}: {e}")

spark.stop()
print("\n" + "="*60)
print("Test completed")
print("="*60)
