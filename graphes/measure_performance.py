#!/usr/bin/env python3
"""
Script de mesure des performances de lecture depuis MinIO (bronze layer)
Génère des graphiques montrant le temps de lecture et le nombre de lignes par dataset
"""
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import os

print("🚀 Démarrage du script de performance MinIO...")

# Configuration MinIO (identique à bronze_ingestion.py)
MINIO_ENDPOINT = "http://minio:9000"  # Utiliser le nom du container Docker
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "bronze"

# Créer la session Spark
spark = (
    SparkSession.builder
    .appName("MinIO_Performance_Test")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# Configuration Hadoop supplémentaire
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

spark.sparkContext.setLogLevel("ERROR")  # Réduire le bruit des logs

print("✅ Spark configuré avec succès")

# Datasets à tester
datasets = [
    "activites_professionnels",
    "adherents",
    "consultations",
    "deces",
    "diagnostics",
    "patients",
    "professionnels_sante_pg",
    "etablissements"
]

print(f"\n📊 Test de lecture sur {len(datasets)} datasets depuis MinIO...\n")

performance_data = []

for ds in datasets:
    dataset_path = f"s3a://{BUCKET}/{ds}"
    start_time = time.time()
    try:
        df_spark = spark.read.parquet(dataset_path)
        count = df_spark.count()
        elapsed = time.time() - start_time
        print(f"✅ Dataset '{ds:30s}' - {count:>10,} lignes en {elapsed:6.2f}s")
        performance_data.append({"dataset": ds, "rows": count, "time": elapsed})
    except Exception as e:
        error_msg = str(e)[:80]
        print(f"❌ Dataset '{ds:30s}' - ERREUR: {error_msg}")
        performance_data.append({"dataset": ds, "rows": 0, "time": None})

# Créer DataFrame pandas
performance_df = pd.DataFrame(performance_data)

print(f"\n📈 Génération des graphiques...")

# Style seaborn
sns.set_theme(style="whitegrid")

# Filtrer les données valides
valid_times = performance_df[performance_df['time'].notnull()]
valid_rows = performance_df[performance_df['rows'] > 0]

if not valid_times.empty:
    # Graphique 1: Temps de lecture
    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.bar(valid_times['dataset'], valid_times['time'], color='steelblue', edgecolor='navy', linewidth=1.2)
    
    # Ajouter les valeurs sur les barres
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}s',
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
    ax.set_ylabel('Temps (secondes)', fontsize=13, fontweight='bold')
    ax.set_title('⏱️  Temps de lecture des datasets depuis MinIO (Bronze Layer)', 
                 fontsize=15, fontweight='bold', pad=20)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/temps_lecture.png", dpi=150, bbox_inches='tight')
    print("✅ Graphique 'temps_lecture.png' sauvegardé")
    plt.close()
else:
    print("⚠️  Aucune donnée de temps valide pour générer le graphique")

if not valid_rows.empty:
    # Graphique 2: Nombre de lignes
    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.bar(valid_rows['dataset'], valid_rows['rows'], color='coral', edgecolor='darkred', linewidth=1.2)
    
    # Ajouter les valeurs sur les barres
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
    ax.set_ylabel('Nombre de lignes', fontsize=13, fontweight='bold')
    ax.set_title('📊 Nombre de lignes par dataset (Bronze Layer)', 
                 fontsize=15, fontweight='bold', pad=20)
    plt.xticks(rotation=45, ha='right')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/nombre_lignes.png", dpi=150, bbox_inches='tight')
    print("✅ Graphique 'nombre_lignes.png' sauvegardé")
    plt.close()
else:
    print("⚠️  Aucune donnée de lignes valide pour générer le graphique")

# Graphique 3: Performance (lignes/seconde)
if not valid_times.empty and not valid_rows.empty:
    perf_df = valid_times.merge(valid_rows[['dataset', 'rows']], on='dataset')
    perf_df['rows_per_sec'] = perf_df['rows'] / perf_df['time']
    
    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.bar(perf_df['dataset'], perf_df['rows_per_sec'], color='seagreen', edgecolor='darkgreen', linewidth=1.2)
    
    # Ajouter les valeurs sur les barres
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
    ax.set_ylabel('Lignes par seconde', fontsize=13, fontweight='bold')
    ax.set_title('🚀 Débit de lecture (lignes/seconde)', 
                 fontsize=15, fontweight='bold', pad=20)
    plt.xticks(rotation=45, ha='right')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/performance_debit.png", dpi=150, bbox_inches='tight')
    print("✅ Graphique 'performance_debit.png' sauvegardé")
    plt.close()

# Afficher le résumé
print(f"\n{'='*70}")
print(f"📋 RÉSUMÉ DES PERFORMANCES")
print(f"{'='*70}")
print(performance_df[['dataset', 'rows', 'time']].to_string(index=False))
print(f"{'='*70}")

# Statistiques globales
if not valid_times.empty:
    total_rows = valid_rows['rows'].sum()
    total_time = valid_times['time'].sum()
    avg_speed = total_rows / total_time if total_time > 0 else 0
    
    print(f"\n📊 STATISTIQUES GLOBALES:")
    print(f"   • Datasets traités: {len(valid_times)}/{len(datasets)}")
    print(f"   • Total de lignes: {total_rows:,}")
    print(f"   • Temps total: {total_time:.2f}s")
    print(f"   • Débit moyen: {avg_speed:,.0f} lignes/seconde")

# Arrêter Spark
spark.stop()
print("\n✅ Script terminé avec succès!")
