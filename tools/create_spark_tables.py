#!/usr/bin/env python3
"""
Serveur Spark SQL Thrift pour exposer les données Gold à Superset
"""
from pyspark.sql import SparkSession

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET_GOLD = "gold"
BUCKET_SILVER = "silver"

spark = SparkSession.builder \
    .appName("Spark SQL Thrift Server") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("""
╔═══════════════════════════════════════════╗
║   CRÉATION DES TABLES POUR SUPERSET       ║
╚═══════════════════════════════════════════╝
""")

# Créer les bases de données
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Charger et enregistrer les KPIs Gold
print("📊 Chargement des KPIs Gold...")

# KPI 1: Décès par année
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_deces_par_annee/")
df.write.mode("overwrite").saveAsTable("gold.kpi_deces_par_annee")
print(f"   ✅ gold.kpi_deces_par_annee ({df.count()} lignes)")

# KPI 2: Décès par région
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_deces_par_region/")
df.write.mode("overwrite").saveAsTable("gold.kpi_deces_par_region")
print(f"   ✅ gold.kpi_deces_par_region ({df.count()} lignes)")

# KPI 3: Statistiques démographiques
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_demographic_summary/")
df.write.mode("overwrite").saveAsTable("gold.kpi_demographic_summary")
print(f"   ✅ gold.kpi_demographic_summary ({df.count()} lignes)")

# KPI 4: Tendances temporelles
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_temporal_trends/")
df.write.mode("overwrite").saveAsTable("gold.kpi_temporal_trends")
print(f"   ✅ gold.kpi_temporal_trends ({df.count()} lignes)")

# KPI 5: Top départements
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_top_departements/")
df.write.mode("overwrite").saveAsTable("gold.kpi_top_departements")
print(f"   ✅ gold.kpi_top_departements ({df.count()} lignes)")

# KPI 6: Distribution âge
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_distribution_age/")
df.write.mode("overwrite").saveAsTable("gold.kpi_distribution_age")
print(f"   ✅ gold.kpi_distribution_age ({df.count()} lignes)")

# KPI 7: Synthèse globale
df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/kpi_synthese_globale/")
df.write.mode("overwrite").saveAsTable("gold.kpi_synthese_globale")
print(f"   ✅ gold.kpi_synthese_globale ({df.count()} lignes)")

# Charger aussi les tables Silver principales
print("\n📊 Chargement des tables Silver...")

df = spark.read.parquet(f"s3a://silver/fait_deces/")
df.write.mode("overwrite").saveAsTable("silver.fait_deces")
print(f"   ✅ silver.fait_deces ({df.count()} lignes)")

df = spark.read.parquet(f"s3a://silver/dim_temps/")
df.write.mode("overwrite").saveAsTable("silver.dim_temps")
print(f"   ✅ silver.dim_temps ({df.count()} lignes)")

df = spark.read.parquet(f"s3a://silver/dim_geographie/")
df.write.mode("overwrite").saveAsTable("silver.dim_geographie")
print(f"   ✅ silver.dim_geographie ({df.count()} lignes)")

print("\n✅ Tables créées avec succès!")
print("\nListe des tables disponibles:")
print("\n=== GOLD ===")
spark.sql("SHOW TABLES IN gold").show(truncate=False)
print("\n=== SILVER ===")
spark.sql("SHOW TABLES IN silver").show(truncate=False)

print("""
╔═══════════════════════════════════════════╗
║   TABLES PRÊTES POUR SUPERSET              ║
╚═══════════════════════════════════════════╝

📌 Connexion Superset:
   - Base de données: Spark SQL
   - URI: N/A (utiliser CSV/Parquet upload ou connecteur DuckDB)
   
📌 Alternative recommandée:
   - Copier les fichiers Parquet dans Superset
   - Ou utiliser DuckDB pour requêter directement MinIO
""")

spark.stop()
