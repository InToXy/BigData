#!/usr/bin/env python3
"""
Script d'audit des zones Bronze et Silver
Génère un rapport détaillé du contenu de chaque zone
"""
from pyspark.sql import SparkSession
import os

# Configuration MinIO
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bucket_bronze": "bronze",
    "bucket_silver": "silver"
}

def get_spark_session():
    """Crée une session Spark pour l'audit."""
    try:
        jars_dir = "/home/jovyan/jars"
        jar_files = [
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
            f"{jars_dir}/hadoop-common-3.3.4.jar"
        ]
        
        jars_path = ",".join(jar_files)
        
        builder = SparkSession.builder \
            .appName("Audit_Bronze_Silver") \
            .config("spark.jars", jars_path)
            
        # Configuration S3A
        hadoop_conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": MINIO_CONFIG["endpoint"],
            "spark.hadoop.fs.s3a.access.key": MINIO_CONFIG["access_key"],
            "spark.hadoop.fs.s3a.secret.key": MINIO_CONFIG["secret_key"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
        
        for key, value in hadoop_conf.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Configuration Hadoop explicite
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def list_tables_in_bucket(spark, bucket_name):
    """Liste toutes les tables (dossiers) dans un bucket."""
    try:
        # Utiliser l'API Hadoop pour lister les dossiers
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(f"s3a://{bucket_name}"),
            hadoop_conf
        )
        
        path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket_name}/")
        
        if not fs.exists(path):
            print(f"⚠️  Bucket {bucket_name} n'existe pas ou est vide")
            return []
        
        file_statuses = fs.listStatus(path)
        tables = []
        
        for status in file_statuses:
            if status.isDirectory():
                table_name = status.getPath().getName()
                # Ignorer les dossiers système
                if not table_name.startswith('_') and not table_name.startswith('.'):
                    tables.append(table_name)
        
        return sorted(tables)
        
    except Exception as e:
        print(f"❌ Erreur listing {bucket_name}: {e}")
        return []

def analyze_table(spark, bucket_name, table_name):
    """Analyse une table et retourne ses statistiques."""
    try:
        table_path = f"s3a://{bucket_name}/{table_name}"
        
        df = spark.read.parquet(table_path)
        
        row_count = df.count()
        col_count = len(df.columns)
        
        # Estimation du stockage (approximation)
        # Moyenne de 100 bytes par ligne (très approximatif)
        storage_mb = (row_count * 100) / (1024 * 1024)
        
        return {
            "table": table_name,
            "rows": row_count,
            "columns": col_count,
            "storage_mb": storage_mb
        }
        
    except Exception as e:
        print(f"  ⚠️  Erreur lecture {table_name}: {str(e)[:100]}")
        return {
            "table": table_name,
            "rows": 0,
            "columns": 0,
            "storage_mb": 0
        }

def audit_zone(spark, zone_name, bucket_name):
    """Audite une zone complète."""
    print(f"\n{'='*80}")
    print(f"🔍 AUDIT ZONE {zone_name.upper()}")
    print(f"{'='*80}")
    
    tables = list_tables_in_bucket(spark, bucket_name)
    
    if not tables:
        print(f"❌ Aucune table trouvée dans {zone_name}")
        return {
            "zone": zone_name,
            "nb_tables": 0,
            "total_rows": 0,
            "total_columns": 0,
            "total_storage_mb": 0,
            "tables_detail": []
        }
    
    print(f"📊 Tables trouvées: {len(tables)}")
    print()
    
    tables_stats = []
    total_rows = 0
    total_columns = 0
    total_storage = 0
    
    for i, table in enumerate(tables, 1):
        print(f"  [{i}/{len(tables)}] Analyse de {table}...", end=" ")
        
        stats = analyze_table(spark, bucket_name, table)
        tables_stats.append(stats)
        
        total_rows += stats["rows"]
        total_columns += stats["columns"]
        total_storage += stats["storage_mb"]
        
        print(f"✅ {stats['rows']:,} lignes, {stats['columns']} colonnes")
    
    return {
        "zone": zone_name,
        "nb_tables": len(tables),
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_storage_mb": total_storage,
        "tables_detail": tables_stats
    }

def generate_comparison_report(bronze_stats, silver_stats):
    """Génère le rapport comparatif demandé."""
    print("\n" + "="*80)
    print("📋 RAPPORT COMPARATIF BRONZE vs SILVER")
    print("="*80)
    print()
    
    # Tableau demandé
    print("┌─────────┬─────────────────┬──────────────────┬──────────────────┬──────────────────┐")
    print("│ Zone    │ Nombre tables   │ Lignes totales   │ Colonnes totales │ Stockage estimé  │")
    print("├─────────┼─────────────────┼──────────────────┼──────────────────┼──────────────────┤")
    print(f"│ Bronze  │ {bronze_stats['nb_tables']:>15} │ {bronze_stats['total_rows']:>16,} │ {bronze_stats['total_columns']:>16} │ ~{bronze_stats['total_storage_mb']:>14.1f} MB │")
    print(f"│ Silver  │ {silver_stats['nb_tables']:>15} │ {silver_stats['total_rows']:>16,} │ {silver_stats['total_columns']:>16} │ ~{silver_stats['total_storage_mb']:>14.1f} MB │")
    print("└─────────┴─────────────────┴──────────────────┴──────────────────┴──────────────────┘")
    print()
    
    # Détails par table Bronze
    if bronze_stats['tables_detail']:
        print("\n📊 DÉTAIL DES TABLES BRONZE:")
        print("─" * 80)
        for table in bronze_stats['tables_detail']:
            print(f"  • {table['table']:<30} {table['rows']:>10,} lignes │ {table['columns']:>3} cols │ ~{table['storage_mb']:>6.1f} MB")
    
    # Détails par table Silver
    if silver_stats['tables_detail']:
        print("\n📊 DÉTAIL DES TABLES SILVER:")
        print("─" * 80)
        for table in silver_stats['tables_detail']:
            print(f"  • {table['table']:<30} {table['rows']:>10,} lignes │ {table['columns']:>3} cols │ ~{table['storage_mb']:>6.1f} MB")
    
    # Analyse comparative
    print("\n" + "="*80)
    print("📈 ANALYSE COMPARATIVE")
    print("="*80)
    
    if bronze_stats['nb_tables'] > 0:
        enrichment_ratio = (silver_stats['nb_tables'] / bronze_stats['nb_tables']) * 100
        print(f"  • Enrichissement tables: {enrichment_ratio:.1f}% (Silver a {silver_stats['nb_tables'] - bronze_stats['nb_tables']:+d} tables)")
    
    if bronze_stats['total_rows'] > 0:
        row_ratio = (silver_stats['total_rows'] / bronze_stats['total_rows']) * 100
        print(f"  • Volume de lignes: {row_ratio:.1f}% (Silver vs Bronze)")
    
    if bronze_stats['total_columns'] > 0:
        col_ratio = (silver_stats['total_columns'] / bronze_stats['total_columns']) * 100
        print(f"  • Enrichissement colonnes: {col_ratio:.1f}%")
    
    storage_diff = silver_stats['total_storage_mb'] - bronze_stats['total_storage_mb']
    print(f"  • Différence stockage: {storage_diff:+.1f} MB")
    
    print("\n✅ Audit terminé!")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║          AUDIT DES ZONES BRONZE ET SILVER                   ║
    ║     Analyse comparative des couches de données              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        print("✅ Spark initialisé")
        
        # Audit Bronze
        bronze_stats = audit_zone(spark, "Bronze", MINIO_CONFIG["bucket_bronze"])
        
        # Audit Silver
        silver_stats = audit_zone(spark, "Silver", MINIO_CONFIG["bucket_silver"])
        
        # Rapport comparatif
        generate_comparison_report(bronze_stats, silver_stats)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'audit: {e}")
        import traceback
        traceback.print_exc()
