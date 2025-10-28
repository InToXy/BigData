#!/usr/bin/env python3
"""
Pipeline Bronze - Décès 2019 uniquement
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET = "bronze"

spark = SparkSession.builder \
    .appName("Bronze Deces 2019") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("📥 Lecture décès...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("file:///data/source/DECES EN FRANCE/deces.csv")

print(f"   Total lignes: {df.count()}")
print(f"   Colonnes: {df.columns}")

# Conversion date
df = df.withColumn("date_deces", to_date(col("date_deces"), "yyyy-MM-dd"))
df = df.withColumn("annee_deces", year(col("date_deces")))

# Filtrage 2019
df_2019 = df.filter(col("annee_deces") == 2019)
count_2019 = df_2019.count()
print(f"   ✅ Décès 2019: {count_2019}")

# Anonymisation
for pii_col in ["nom", "prenom"]:
    df_2019 = df_2019.withColumn(pii_col, sha2(col(pii_col), 256))

# Colonnes techniques
batch_id = str(uuid.uuid4())
df_2019 = df_2019 \
    .withColumn("_source_system", lit("CSV")) \
    .withColumn("_source_table", lit("deces")) \
    .withColumn("_ingestion_date", current_timestamp()) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_version", lit(1)) \
    .withColumn("_is_current", lit(True)) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_sk", monotonically_increasing_id())

# Écriture
output_path = f"s3a://{BUCKET}/deces/"
print(f"💾 Écriture vers {output_path}...")
df_2019.write.mode("overwrite").parquet(output_path)
print("✅ TERMINÉ")

spark.stop()
