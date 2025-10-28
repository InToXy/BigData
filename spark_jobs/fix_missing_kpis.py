#!/usr/bin/env python3
"""
Corriger kpi_deces_by_region et kpi_satisfaction_global
en lisant depuis Bronze au lieu de Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, year, avg, min as spark_min, max as spark_max, 
    current_timestamp
)

# Configuration MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# Créer session Spark
spark = SparkSession.builder \
    .appName("Fix_Missing_KPIs") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🔧 Correction KPIs manquants")
print("=" * 60)

# ============================================================
# KPI 3: Décès by Region
# ============================================================
print("\n📊 KPI 3: Décès by Region...")

# Lire depuis Bronze
deces_bronze = spark.read.parquet("s3a://bronze/deces/")
print(f"   ✅ Bronze deces lu: {deces_bronze.count():,} lignes")

# Vérifier les colonnes disponibles
print(f"\n   📋 Colonnes disponibles: {deces_bronze.columns}")

# Calculer l'âge à partir de date_naissance et date_deces
from pyspark.sql.functions import datediff, lit

deces_with_age = deces_bronze \
    .filter(col("date_deces").isNotNull()) \
    .filter(col("date_naissance").isNotNull()) \
    .withColumn("age", (datediff(col("date_deces"), col("date_naissance")) / 365.25).cast("int"))

# Agrégation par région et sexe (utiliser 'region' au lieu de 'lieu_deces')
kpi_deces = deces_with_age \
    .withColumn("annee", year(col("date_deces"))) \
    .groupBy("annee", "region", "sexe") \
    .agg(
        count("*").alias("nb_deces"),
        avg("age").alias("age_moyen_deces"),
        spark_min("age").alias("age_min_deces"),
        spark_max("age").alias("age_max_deces")
    ) \
    .withColumn("calcul_date", current_timestamp()) \
    .orderBy("annee", col("nb_deces").desc())

print(f"\n   📈 Aperçu kpi_deces_by_region:")
kpi_deces.show(10, truncate=False)

print(f"\n   💾 Écriture dans s3a://gold/kpi_deces_by_region/...")
kpi_deces.write.mode("overwrite").parquet("s3a://gold/kpi_deces_by_region/")
print(f"   ✅ kpi_deces_by_region: {kpi_deces.count()} lignes")

# ============================================================
# KPI 4: Satisfaction Global
# ============================================================
print("\n\n📊 KPI 4: Satisfaction Global...")

# Lire depuis Bronze (satisfaction_mco_2017)
try:
    satisfaction_bronze = spark.read.parquet("s3a://bronze/satisfaction_mco_2017/")
    print(f"   ✅ Bronze satisfaction_mco_2017 lu: {satisfaction_bronze.count():,} lignes")
    
    # Vérifier les colonnes disponibles
    print(f"\n   📋 Colonnes disponibles: {satisfaction_bronze.columns}")
    
    # Chercher les colonnes de score
    score_cols = [c for c in satisfaction_bronze.columns if 'score' in c.lower() or 'taux' in c.lower()]
    print(f"   📊 Colonnes de score trouvées: {score_cols}")
    
    # Agrégation simple - compter les réponses par source
    kpi_satisfaction = satisfaction_bronze \
        .selectExpr("'ESATIS MCO 2017' as type_enquete") \
        .groupBy("type_enquete") \
        .agg(count("*").alias("nb_reponses"))
    
    # Calculer un score moyen si disponible
    if 'taux_recommandation' in satisfaction_bronze.columns:
        kpi_satisfaction = satisfaction_bronze \
            .selectExpr("'ESATIS MCO 2017' as type_enquete") \
            .groupBy("type_enquete") \
            .agg(
                count("*").alias("nb_reponses"),
                avg("taux_recommandation").alias("score_moyen")
            )
    
    kpi_satisfaction = kpi_satisfaction.withColumn("calcul_date", current_timestamp())
    
    print(f"\n   📈 Aperçu kpi_satisfaction_global:")
    kpi_satisfaction.show(10, truncate=False)
    
    print(f"\n   💾 Écriture dans s3a://gold/kpi_satisfaction_global/...")
    kpi_satisfaction.write.mode("overwrite").parquet("s3a://gold/kpi_satisfaction_global/")
    print(f"   ✅ kpi_satisfaction_global: {kpi_satisfaction.count()} lignes")
    
except Exception as e:
    print(f"   ❌ Erreur satisfaction: {e}")

print("\n" + "=" * 60)
print("✅ Correction terminée")
print("=" * 60)

spark.stop()
