#!/usr/bin/env python3
"""Affiche les 10 premières lignes de chaque table Bronze"""

from pyspark.sql import SparkSession

# Créer session Spark
spark = SparkSession.builder \
    .appName("View_Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

tables = ["deces", "etablissements", "professionnels_sante", "hospitalisations", "satisfaction_mco_2017"]

for table in tables:
    print("\n" + "="*80)
    print(f"📊 TABLE: {table.upper()}")
    print("="*80)
    
    try:
        df = spark.read.parquet(f"s3a://bronze/{table}/")
        print(f"\n📈 Nombre total de lignes: {df.count():,}")
        print(f"📋 Nombre de colonnes: {len(df.columns)}")
        
        print("\n🔍 Schema:")
        df.printSchema()
        
        print(f"\n📄 10 premières lignes:")
        df.show(10, truncate=50, vertical=False)
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()

spark.stop()
print("\n✅ Terminé")
