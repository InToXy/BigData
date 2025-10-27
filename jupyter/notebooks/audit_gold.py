#!/usr/bin/env python3
"""
audit_gold.py

Analyse et audit de la zone Gold (datasets KPI).
Produit des statistiques sur nombre de tables, lignes, colonnes et stockage.
"""
import os
from pyspark.sql import SparkSession
from datetime import datetime

# Config MinIO/S3A
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")

def get_spark_session(app_name: str = "AuditGold") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_directory_size(spark, path):
    """Calculate total size of a directory in bytes using Hadoop FileSystem."""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path),
            spark._jsc.hadoopConfiguration()
        )
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        
        if not fs.exists(hadoop_path):
            return 0
        
        content_summary = fs.getContentSummary(hadoop_path)
        return content_summary.getLength()
    except Exception as e:
        print(f"Erreur calcul taille pour {path}: {e}")
        return 0


def list_gold_tables(spark):
    """Liste toutes les tables dans le bucket Gold."""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI('s3a://gold'),
            spark._jsc.hadoopConfiguration()
        )
        
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path('s3a://gold/')
        if not fs.exists(hadoop_path):
            print("❌ Bucket gold/ n'existe pas")
            return []
        
        status_list = fs.listStatus(hadoop_path)
        tables = []
        
        for status in status_list:
            path = status.getPath()
            name = path.getName()
            if not name.startswith('.') and not name.startswith('_'):
                tables.append(name)
        
        return sorted(tables)
    except Exception as e:
        print(f"Erreur lors du listing des tables Gold: {e}")
        return []


def audit_gold_table(spark, table_name):
    """Analyse une table Gold et retourne ses statistiques."""
    path = f"s3a://gold/{table_name}"
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        
        # Statistiques de base
        row_count = df.count()
        col_count = len(df.columns)
        
        # Taille sur disque
        size_bytes = get_directory_size(spark, path)
        size_mb = size_bytes / (1024 * 1024)
        
        return {
            'table': table_name,
            'rows': row_count,
            'columns': col_count,
            'size_bytes': size_bytes,
            'size_mb': size_mb,
            'columns_list': df.columns
        }
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        return None


def main():
    spark = get_spark_session()
    
    print("\n" + "="*80)
    print("📊 AUDIT DE LA ZONE GOLD")
    print("="*80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    # Lister toutes les tables Gold
    tables = list_gold_tables(spark)
    
    if not tables:
        print("⚠️  Aucune table trouvée dans la zone Gold")
        spark.stop()
        return
    
    print(f"📁 Nombre de tables trouvées: {len(tables)}\n")
    
    # Analyser chaque table
    results = []
    total_rows = 0
    total_cols = 0
    total_size_mb = 0
    
    for i, table in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Analyse de {table}...", end=" ")
        stats = audit_gold_table(spark, table)
        
        if stats:
            results.append(stats)
            total_rows += stats['rows']
            total_cols += stats['columns']
            total_size_mb += stats['size_mb']
            print(f"✅ ({stats['rows']:,} lignes, {stats['columns']} colonnes, {stats['size_mb']:.2f} MB)")
        else:
            print("❌")
    
    # Afficher le résumé
    print("\n" + "="*80)
    print("📋 RÉSUMÉ DÉTAILLÉ DES TABLES")
    print("="*80)
    print(f"{'Table':<45} {'Lignes':>12} {'Colonnes':>10} {'Taille':>12}")
    print("-"*80)
    
    for stat in sorted(results, key=lambda x: x['rows'], reverse=True):
        print(f"{stat['table']:<45} {stat['rows']:>12,} {stat['columns']:>10} {stat['size_mb']:>11.2f} MB")
    
    print("-"*80)
    print(f"{'TOTAL':<45} {total_rows:>12,} {total_cols:>10} {total_size_mb:>11.2f} MB")
    print("="*80)
    
    # Résumé pour tableau comparatif
    print("\n" + "="*80)
    print("📊 RÉSUMÉ POUR TABLEAU COMPARATIF")
    print("="*80)
    print(f"Zone           : Gold")
    print(f"Nombre de tables : {len(results)}")
    print(f"Lignes totales  : {total_rows:,} lignes")
    print(f"Colonnes totales: {total_cols} colonnes")
    print(f"Stockage estimé : ~{total_size_mb:.0f} MB")
    print("="*80)
    
    # Analyse des colonnes uniques
    all_columns = set()
    for stat in results:
        all_columns.update(stat['columns_list'])
    
    print(f"\n📌 Colonnes uniques à travers toutes les tables: {len(all_columns)}")
    print(f"📌 Moyenne de lignes par table: {total_rows / len(results):,.0f}")
    print(f"📌 Moyenne de colonnes par table: {total_cols / len(results):.1f}")
    print(f"📌 Taille moyenne par table: {total_size_mb / len(results):.2f} MB")
    
    # Top 5 tables par taille
    print("\n" + "="*80)
    print("🏆 TOP 5 TABLES PAR VOLUME DE DONNÉES")
    print("="*80)
    top_5 = sorted(results, key=lambda x: x['rows'], reverse=True)[:5]
    for i, stat in enumerate(top_5, 1):
        print(f"{i}. {stat['table']}: {stat['rows']:,} lignes ({stat['size_mb']:.2f} MB)")
    
    print("\n" + "="*80)
    print("✅ Audit terminé")
    print("="*80 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
