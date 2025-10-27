#!/usr/bin/env python3
"""
Script pour afficher le contenu détaillé de chaque table Gold (KPIs)
- Schéma complet
- Toutes les données (car ce sont des KPIs agrégés)
- Statistiques descriptives
"""
from pyspark.sql import SparkSession
import os

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin123"
GOLD_BUCKET = "gold"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder.appName("ShowGoldContent")
    
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def list_gold_tables(spark):
    """Liste toutes les tables dans le bucket Gold."""
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI(f's3a://{GOLD_BUCKET}/'),
        sc._jsc.hadoopConfiguration()
    )
    
    path = sc._jvm.org.apache.hadoop.fs.Path(f's3a://{GOLD_BUCKET}/')
    statuses = fs.listStatus(path)
    
    tables = []
    for status in statuses:
        if status.isDirectory():
            tables.append(status.getPath().getName())
    
    return sorted(tables)

def show_kpi_content(spark, kpi_name):
    """Affiche le contenu complet d'un KPI."""
    path = f"s3a://{GOLD_BUCKET}/{kpi_name}"
    
    print("\n" + "="*100)
    print(f"📊 KPI: {kpi_name.upper()}")
    print("="*100)
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        
        # Statistiques de base
        count = df.count()
        num_cols = len(df.columns)
        
        print(f"\n📈 Statistiques:")
        print(f"   • Nombre de lignes: {count:,}")
        print(f"   • Nombre de colonnes: {num_cols}")
        
        # Schéma
        print(f"\n🔍 Schéma des colonnes:")
        print("   " + "-"*90)
        print(f"   {'Colonne':<40} {'Type':<30} {'Nullable':<10}")
        print("   " + "-"*90)
        for field in df.schema.fields:
            print(f"   {field.name:<40} {str(field.dataType):<30} {str(field.nullable):<10}")
        print("   " + "-"*90)
        
        # Afficher TOUTES les données (car KPIs agrégés = peu de lignes)
        if count > 0:
            print(f"\n📋 Contenu complet du KPI ({count} ligne(s)):")
            df.show(count if count <= 1000 else 1000, truncate=False, vertical=False)
            
            if count > 1000:
                print(f"\n⚠️  Affichage limité à 1000 lignes (total: {count:,})")
            
            # Statistiques descriptives pour les colonnes numériques
            numeric_cols = [f.name for f in df.schema.fields 
                          if str(f.dataType) in ['LongType', 'IntegerType', 'DoubleType', 'FloatType', 'DecimalType']]
            
            if numeric_cols and count > 1:
                print(f"\n📊 Statistiques descriptives (colonnes numériques):")
                df.select(numeric_cols).describe().show(truncate=False)
        else:
            print(f"\n⚠️  KPI vide - aucune donnée à afficher")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la lecture de {kpi_name}: {e}")

def main():
    print("="*100)
    print("🔍 ANALYSE DÉTAILLÉE DES KPIs GOLD")
    print("="*100)
    
    spark = get_spark_session()
    print("\n✅ Spark initialisé")
    
    # Lister tous les KPIs
    kpis = list_gold_tables(spark)
    print(f"\n📦 {len(kpis)} KPI(s) trouvé(s) dans Gold:")
    for i, kpi in enumerate(kpis, 1):
        print(f"   {i}. {kpi}")
    
    # Afficher le contenu de chaque KPI
    for kpi in kpis:
        show_kpi_content(spark, kpi)
    
    spark.stop()
    print("\n" + "="*100)
    print("✅ Analyse terminée")
    print("="*100 + "\n")

if __name__ == "__main__":
    main()
