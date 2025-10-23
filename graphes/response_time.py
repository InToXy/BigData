#!/usr/bin/env python3
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import os

print("🚀 Démarrage du script de performance...")

# Définir HADOOP_CONF_DIR pour utiliser notre configuration custom
os.environ['HADOOP_CONF_DIR'] = '/home/alban/.hadoop'

# --- Configuration Spark pour MinIO avec contournement du bug Hadoop 3.3.x ---
spark = (
    SparkSession.builder
    .appName("PerformanceGraphs")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # IMPORTANT: Valeurs numériques en millisecondes uniquement
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")
    .config("spark.hadoop.fs.s3a.threads.max", "10")
    .getOrCreate()
)

# Configuration Hadoop après initialisation pour OVERRIDE les valeurs par défaut bugguées
hadoop_conf = spark._jsc.hadoopConfiguration()
# Supprimer les propriétés conflictuelles
hadoop_conf.unset("fs.s3a.connection.timeout")
hadoop_conf.unset("fs.s3a.socket.send.buffer")
hadoop_conf.unset("fs.s3a.socket.recv.buffer")
hadoop_conf.unset("fs.s3a.threads.keepalivetime")

# Redéfinir avec des valeurs numériques
hadoop_conf.setInt("fs.s3a.connection.timeout", 200000)
hadoop_conf.setInt("fs.s3a.connection.establish.timeout", 5000)
hadoop_conf.setInt("fs.s3a.attempts.maximum", 3)
hadoop_conf.setInt("fs.s3a.connection.maximum", 100)

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark configuré avec succès")

# --- Buckets et datasets à tester ---
bucket = "bronze"
datasets = [
    "activites_professionnels",
    "adherents",
    "consultations",
    "deces"
]

# --- Mesure du temps de lecture des datasets ---
performance_data = []

print(f"\n📊 Test de lecture sur {len(datasets)} datasets...\n")

for ds in datasets:
    dataset_path = f"s3a://{bucket}/{ds}"
    start_time = time.time()
    try:
        df_spark = spark.read.parquet(dataset_path)
        count = df_spark.count()
        elapsed = time.time() - start_time
        print(f"✅ Dataset '{ds}' lu ({count:,} lignes) en {elapsed:.2f}s")
        performance_data.append({"dataset": ds, "rows": count, "time": elapsed})
    except Exception as e:
        print(f"❌ Erreur sur '{ds}': {str(e)[:100]}")
        performance_data.append({"dataset": ds, "rows": 0, "time": None})

# --- Conversion en DataFrame pandas pour seaborn ---
performance_df = pd.DataFrame(performance_data)

print(f"\n📈 Génération des graphiques...")

# --- Graphiques de performance ---
sns.set(style="whitegrid")

# Temps de lecture
plt.figure(figsize=(12, 6))
valid_times = performance_df[performance_df['time'].notnull()]
if not valid_times.empty:
    sns.barplot(x="dataset", y="time", data=valid_times, hue="dataset", palette="viridis", legend=False)
    plt.title("Temps de lecture des datasets depuis MinIO (s)", fontsize=14, fontweight='bold')
    plt.ylabel("Temps (secondes)", fontsize=12)
    plt.xlabel("Dataset", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/temps_lecture.png", dpi=150)
    print("✅ Graphique 'temps_lecture.png' sauvegardé")
else:
    print("⚠️  Aucune donnée de temps valide pour générer le graphique")

# Nombre de lignes
plt.figure(figsize=(12, 6))
valid_rows = performance_df[performance_df['rows'] > 0]
if not valid_rows.empty:
    sns.barplot(x="dataset", y="rows", data=valid_rows, hue="dataset", palette="magma", legend=False)
    plt.title("Nombre de lignes par dataset", fontsize=14, fontweight='bold')
    plt.ylabel("Nombre de lignes", fontsize=12)
    plt.xlabel("Dataset", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    # Formater l'axe Y avec des séparateurs de milliers
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/nombre_lignes.png", dpi=150)
    print("✅ Graphique 'nombre_lignes.png' sauvegardé")
else:
    print("⚠️  Aucune donnée de lignes valide pour générer le graphique")

# Afficher le résumé
print(f"\n📋 RÉSUMÉ DES PERFORMANCES:")
print(performance_df.to_string(index=False))

# --- Fin ---
spark.stop()
print("\n✅ Script terminé avec succès!")
