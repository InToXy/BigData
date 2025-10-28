#!/usr/bin/env python3
"""Analyse de la qualité des données Bronze"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull

spark = SparkSession.builder \
    .appName("Data_Quality_Check") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

tables = {
    "deces": ["date_naissance", "date_deces", "sexe", "region", "departement"],
    "etablissements": ["finess", "identifiant_organisation", "raison_sociale", "code_postal", "commune"],
    "professionnels_sante": ["identifiant_original", "profession", "specialite"],
    "hospitalisations": ["date_entree", "id_patient_original", "identifiant_organisation", "code_diagnostic"],
    "satisfaction_mco_2017": ["identifiant_organisation", "score_all_ajust", "region"]
}

print("\n" + "="*80)
print("🔍 ANALYSE DE QUALITÉ DES DONNÉES BRONZE")
print("="*80)

for table_name, key_columns in tables.items():
    print(f"\n📊 TABLE: {table_name.upper()}")
    print("-" * 80)
    
    df = spark.read.parquet(f"s3a://bronze/{table_name}/")
    total_rows = df.count()
    print(f"   Total lignes: {total_rows:,}")
    
    # Analyse NULL par colonne
    print("\n   Colonnes clés avec valeurs NULL:")
    for col_name in key_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows) * 100
            status = "❌" if null_pct > 10 else "⚠️" if null_pct > 0 else "✅"
            print(f"      {status} {col_name:30s}: {null_count:>10,} NULL ({null_pct:>6.2f}%)")
    
    # Lignes complètement vides
    null_condition = None
    for col_name in key_columns:
        if col_name in df.columns:
            if null_condition is None:
                null_condition = col(col_name).isNull()
            else:
                null_condition = null_condition & col(col_name).isNull()
    
    if null_condition is not None:
        completely_null = df.filter(null_condition).count()
        if completely_null > 0:
            print(f"\n   ⚠️ Lignes avec TOUTES les colonnes clés NULL: {completely_null:,}")

print("\n" + "="*80)
print("✅ Analyse terminée")
print("="*80)

spark.stop()
