#!/usr/bin/env python3
"""
Charger les KPIs Gold dans PostgreSQL pour Superset
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET_GOLD = "gold"

# Configuration PostgreSQL
POSTGRES_HOST = "chu_postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "healthcare_data"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin123"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

spark = SparkSession.builder \
    .appName("Gold to PostgreSQL") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("""
╔═══════════════════════════════════════════╗
║     CHARGEMENT GOLD → POSTGRESQL           ║
║        Pour accès via Superset             ║
╚═══════════════════════════════════════════╝
""")

# Propriétés de connexion PostgreSQL
postgres_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Liste des KPIs à charger
kpis = [
    "kpi_deces_par_annee",
    "kpi_deces_par_region",
    "kpi_demographic_summary",
    "kpi_temporal_trends",
    "kpi_top_departements",
    "kpi_distribution_age",
    "kpi_synthese_globale",
    # KPIs Métier
    "kpi_consultation_etablissement",
    "kpi_consultation_professionnel",
    "kpi_hospitalisation_globale",
    "kpi_hospitalisation_sexe_age",
    "kpi_deces_region_2019",
    "kpi_satisfaction_region",
    "kpi_consultations_synthese"
]

print("📊 Chargement des KPIs dans PostgreSQL...\n")

for kpi in kpis:
    try:
        # Lire depuis MinIO
        df = spark.read.parquet(f"s3a://{BUCKET_GOLD}/{kpi}/")
        count = df.count()
        
        # Écrire dans PostgreSQL
        df.write.jdbc(
            url=POSTGRES_URL,
            table=kpi,
            mode="overwrite",
            properties=postgres_properties
        )
        
        print(f"   ✅ {kpi}: {count} lignes")
        
    except Exception as e:
        print(f"   ❌ {kpi}: Erreur - {str(e)}")

print("""
\n╔═══════════════════════════════════════════╗
║     KPIs DISPONIBLES DANS POSTGRESQL       ║
╚═══════════════════════════════════════════╝

📌 Connexion Superset PostgreSQL:
   Host: chu_postgres
   Port: 5432
   Database: healthcare_data
   User: admin
   Password: admin123

📊 Tables disponibles:
""")

for kpi in kpis:
    print(f"   - {kpi}")

print("\n🎯 Prêt pour la visualisation dans Superset!")

spark.stop()
