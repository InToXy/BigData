#!/usr/bin/env python3
"""
Script pour afficher le contenu détaillé de chaque table Silver
- Schéma (types de données)
- Échantillon de données (5 premières lignes)
- Statistiques sur chaque colonne
"""
from pyspark.sql import SparkSession
import os

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin123"
SILVER_BUCKET = "silver"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder.appName("ShowSilverContent")
    
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def list_silver_tables(spark):
    """Liste toutes les tables dans le bucket Silver."""
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI(f's3a://{SILVER_BUCKET}/'),
        sc._jsc.hadoopConfiguration()
    )
    
    path = sc._jvm.org.apache.hadoop.fs.Path(f's3a://{SILVER_BUCKET}/')
    statuses = fs.listStatus(path)
    
    tables = []
    for status in statuses:
        if status.isDirectory():
            tables.append(status.getPath().getName())
    
    return sorted(tables)

def show_table_content(spark, table_name):
    """Affiche le contenu détaillé d'une table."""
    path = f"s3a://{SILVER_BUCKET}/{table_name}"
    
    print("\n" + "="*100)
    print(f"📊 TABLE: {table_name.upper()}")
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
        print(f"   {'Colonne':<40} {'Type':<20} {'Nullable':<10}")
        print("   " + "-"*90)
        for field in df.schema.fields:
            print(f"   {field.name:<40} {str(field.dataType):<20} {str(field.nullable):<10}")
        print("   " + "-"*90)
        
        # Échantillon de données
        if count > 0:
            print(f"\n📋 Échantillon de données (5 premières lignes):")
            df.show(5, truncate=True, vertical=False)
            
            # Statistiques descriptives pour les colonnes numériques
            numeric_cols = [f.name for f in df.schema.fields 
                          if str(f.dataType) in ['LongType', 'IntegerType', 'DoubleType', 'FloatType', 'DecimalType']]
            
            if numeric_cols:
                print(f"\n📊 Statistiques descriptives (colonnes numériques):")
                df.select(numeric_cols).describe().show()
        else:
            print(f"\n⚠️  Table vide - aucune donnée à afficher")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la lecture de {table_name}: {e}")

def main():
    print("="*100)
    print("🔍 ANALYSE DÉTAILLÉE DES TABLES SILVER")
    print("="*100)
    
    spark = get_spark_session()
    print("\n✅ Spark initialisé")
    
    # Lister toutes les tables
    tables = list_silver_tables(spark)
    print(f"\n📦 {len(tables)} tables trouvées dans Silver:")
    for i, table in enumerate(tables, 1):
        print(f"   {i}. {table}")
    
    # Afficher le contenu de chaque table
    for table in tables:
        show_table_content(spark, table)
    
    spark.stop()
    print("\n" + "="*100)
    print("✅ Analyse terminée")
    print("="*100 + "\n")

if __name__ == "__main__":
    main()
