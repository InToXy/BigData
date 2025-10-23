#!/usr/bin/env python3
"""List and preview Gold datasets."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ListGold') \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin123') \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI('s3a://gold'),
    spark._jsc.hadoopConfiguration()
)

print('\n' + '='*70)
print('📊 GOLD DATASETS')
print('='*70)

try:
    status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path('s3a://gold/'))
    for item in status:
        name = item.getPath().getName()
        print(f'\n📁 {name}')
        try:
            df = spark.read.parquet(f's3a://gold/{name}')
            count = df.count()
            cols = ', '.join(df.columns)
            print(f'   Lignes: {count:,}')
            print(f'   Colonnes: {cols}')
            print(f'   Aperçu:')
            df.show(5, truncate=False)
        except Exception as e:
            print(f'   Erreur lecture: {e}')
except Exception as e:
    print(f'Erreur: {e}')

print('='*70)
spark.stop()
