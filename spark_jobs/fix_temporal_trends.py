#!/usr/bin/env python3
"""
Régénérer uniquement kpi_temporal_trends avec le bon format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, year, quarter, current_timestamp
)

# Configuration MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# Créer session Spark
spark = SparkSession.builder \
    .appName("Fix_Temporal_Trends") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🔧 Régénération kpi_temporal_trends")
print("=" * 60)

# Lire les tables Silver
print("\n📥 Lecture des tables Silver...")
fact_consultation = spark.read.parquet("s3a://silver/fact_consultation/")
fact_hosp = spark.read.parquet("s3a://silver/fact_hospitalisation/")

print(f"   ✅ fact_consultation: {fact_consultation.count():,} lignes")
print(f"   ✅ fact_hospitalisation: {fact_hosp.count():,} lignes")

# Lire fact_deces depuis Bronze (n'existe pas dans Silver)
try:
    fact_deces = spark.read.parquet("s3a://silver/fact_deces/")
    print(f"   ✅ fact_deces (Silver): {fact_deces.count():,} lignes")
except:
    print("   ⚠️  fact_deces non trouvée dans Silver, lecture depuis Bronze...")
    fact_deces = spark.read.parquet("s3a://bronze/deces/") \
        .withColumnRenamed("date_deces", "date_deces")
    print(f"   ✅ fact_deces (Bronze): {fact_deces.count():,} lignes")

# Agrégation consultations
print("\n📊 Agrégation consultations...")
consult_agg = fact_consultation \
    .filter(col("date_consultation").isNotNull()) \
    .groupBy(
        year(col("date_consultation")).alias("annee"),
        quarter(col("date_consultation")).alias("trimestre")
    ) \
    .agg(count("*").alias("nb_consultations"))

# Agrégation hospitalisations
print("📊 Agrégation hospitalisations...")
hosp_agg = fact_hosp \
    .filter(col("date_entree").isNotNull()) \
    .groupBy(
        year(col("date_entree")).alias("annee"),
        quarter(col("date_entree")).alias("trimestre")
    ) \
    .agg(count("*").alias("nb_hospitalisations"))

# Agrégation décès
print("📊 Agrégation décès...")
deces_agg = fact_deces \
    .filter(col("date_deces").isNotNull()) \
    .groupBy(
        year(col("date_deces")).alias("annee"),
        quarter(col("date_deces")).alias("trimestre")
    ) \
    .agg(count("*").alias("nb_deces"))

# Joindre toutes les agrégations
print("\n🔗 Fusion des agrégations...")
kpi = consult_agg \
    .join(hosp_agg, ["annee", "trimestre"], "full") \
    .join(deces_agg, ["annee", "trimestre"], "full")

# Remplir les valeurs nulles et calculer le total
kpi = kpi \
    .fillna(0, subset=["nb_consultations", "nb_hospitalisations", "nb_deces"]) \
    .withColumn("activite_totale", 
               col("nb_consultations") + col("nb_hospitalisations") + col("nb_deces")) \
    .withColumn("calcul_date", current_timestamp()) \
    .orderBy("annee", "trimestre")

print("\n📋 Aperçu des données:")
kpi.show(10, truncate=False)

print(f"\n💾 Écriture dans s3a://gold/kpi_temporal_trends/...")
kpi.write.mode("overwrite").parquet("s3a://gold/kpi_temporal_trends/")

print(f"✅ KPI temporal_trends régénéré: {kpi.count()} lignes")
print("=" * 60)

spark.stop()
