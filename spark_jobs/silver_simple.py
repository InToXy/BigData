#!/usr/bin/env python3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import uuid

MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

spark = SparkSession.builder \
    .appName("Silver") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
batch_id = str(uuid.uuid4())

print("""
╔═══════════════════════════════════════════╗
║       PIPELINE SILVER - MODELISATION      ║
║     Dimensions + Faits pour KPIs Gold     ║
╚═══════════════════════════════════════════╝
""")
print(f"📦 Batch: {batch_id}\n")

# CHARGEMENT BRONZE
print("📥 CHARGEMENT BRONZE")
df_deces = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/deces/")
print(f"   ✅ deces: {df_deces.count()}")

df_etab = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/etablissements/")
print(f"   ✅ etablissements: {df_etab.count()}")

df_prof = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/professionnels_sante/")
print(f"   ✅ professionnels: {df_prof.count()}")

df_activite = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/activite_professionnels/")
print(f"   ✅ activite: {df_activite.count()}")

df_hospi = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/hospitalisations/")
print(f"   ✅ hospitalisations: {df_hospi.count()}")

# DIMENSIONS
print("\n🔷 DIM_TEMPS")
dim_temps = df_deces.select("date_deces").distinct() \
    .withColumn("date_id", F.monotonically_increasing_id()) \
    .withColumn("annee", F.year(F.col("date_deces"))) \
    .withColumn("mois", F.month(F.col("date_deces"))) \
    .withColumn("jour", F.dayofmonth(F.col("date_deces"))) \
    .withColumn("trimestre", F.quarter(F.col("date_deces"))) \
    .withColumn("_silver_batch_id", F.lit(batch_id))

dim_temps.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/dim_temps/")
print(f"   💾 {dim_temps.count()} lignes")

print("🔷 DIM_GEOGRAPHIE")
dim_geo = df_deces.select(F.col("code_lieu_deces").alias("code_lieu")).distinct() \
    .withColumn("geo_id", F.monotonically_increasing_id()) \
    .withColumn("code_dept", F.substring(F.col("code_lieu"), 1, 2)) \
    .withColumn("_silver_batch_id", F.lit(batch_id))

dim_geo.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/dim_geographie/")
print(f"   💾 {dim_geo.count()} lignes")

print("🔷 DIM_ETABLISSEMENT")
dim_etab = df_etab.select("_sk").withColumnRenamed("_sk", "etablissement_id") \
    .withColumn("_silver_batch_id", F.lit(batch_id))

dim_etab.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/dim_etablissement/")
print(f"   💾 {dim_etab.count()} lignes")

print("🔷 DIM_PROFESSIONNEL")
dim_prof_clean = df_prof.select("_sk").withColumnRenamed("_sk", "professionnel_id") \
    .withColumn("_silver_batch_id", F.lit(batch_id))

dim_prof_clean.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/dim_professionnel/")
print(f"   💾 {dim_prof_clean.count()} lignes")

# FAITS
print("\n📊 FAIT_DECES")
fait_deces = df_deces.select(
    F.col("_sk").alias("deces_id"),
    "date_deces",
    "sexe",
    "date_naissance",
    "code_lieu_deces",
    "annee_deces"
) \
.withColumn("age_deces", 
    F.when(F.col("date_naissance").isNotNull() & F.col("date_deces").isNotNull(),
           F.floor(F.datediff(F.col("date_deces"), F.col("date_naissance")) / 365.25))
) \
.withColumn("categorie_age",
    F.when(F.col("age_deces") < 1, "< 1 an")
    .when(F.col("age_deces") < 18, "1-17 ans")
    .when(F.col("age_deces") < 30, "18-29 ans")
    .when(F.col("age_deces") < 45, "30-44 ans")
    .when(F.col("age_deces") < 60, "45-59 ans")
    .when(F.col("age_deces") < 75, "60-74 ans")
    .when(F.col("age_deces") < 90, "75-89 ans")
    .when(F.col("age_deces") >= 90, "90+ ans")
    .otherwise("Inconnu")
) \
.withColumn("_silver_batch_id", F.lit(batch_id))

# Join avec dim_temps
fait_deces = fait_deces.join(
    dim_temps.select("date_deces", "date_id"),
    "date_deces",
    "left"
)

# Join avec dim_geo
fait_deces = fait_deces.join(
    dim_geo.select(F.col("code_lieu").alias("code_lieu_deces"), "geo_id"),
    "code_lieu_deces",
    "left"
)

fait_deces.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/fait_deces/")
print(f"   💾 {fait_deces.count()} lignes")

print("📊 FAIT_ACTIVITE")
fait_act = df_activite.select("_sk").withColumnRenamed("_sk", "activite_id") \
    .withColumn("_silver_batch_id", F.lit(batch_id))

fait_act.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/fait_activite/")
print(f"   💾 {fait_act.count()} lignes")

print("📊 FAIT_HOSPITALISATION")
fait_hosp = df_hospi.select("_sk").withColumnRenamed("_sk", "hospitalisation_id") \
    .withColumn("_silver_batch_id", F.lit(batch_id))

fait_hosp.write.mode("overwrite").parquet(f"s3a://{BUCKET_SILVER}/fait_hospitalisation/")
print(f"   💾 {fait_hosp.count()} lignes")

print("\n✅ SILVER TERMINÉ")
print("💾 Données dans s3a://silver/")

spark.stop()
