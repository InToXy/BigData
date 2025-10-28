#!/usr/bin/env python3
"""
Pipeline Gold - Création des KPIs agrégés
Génération des indicateurs pour les dashboards
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import uuid

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

spark = SparkSession.builder \
    .appName("Gold KPIs") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
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
║         PIPELINE GOLD - KPIs              ║
║     Indicateurs pour Dashboards           ║
╚═══════════════════════════════════════════╝
""")
print(f"📦 Batch: {batch_id}\n")

# ===== CHARGEMENT SILVER =====
print("📥 CHARGEMENT SILVER")

fait_deces = spark.read.parquet(f"s3a://{BUCKET_SILVER}/fait_deces/")
print(f"   ✅ fait_deces: {fait_deces.count()} lignes")

dim_temps = spark.read.parquet(f"s3a://{BUCKET_SILVER}/dim_temps/")
print(f"   ✅ dim_temps: {dim_temps.count()} lignes")

dim_geo = spark.read.parquet(f"s3a://{BUCKET_SILVER}/dim_geographie/")
print(f"   ✅ dim_geographie: {dim_geo.count()} lignes")

# ===== KPI 1: DÉCÈS PAR ANNÉE/SEXE/ÂGE =====
print("\n📊 KPI_DECES_PAR_ANNEE")

kpi_deces_annee = fait_deces.groupBy("annee_deces", "sexe", "categorie_age") \
    .agg(
        F.count("*").alias("nombre_deces"),
        F.avg("age_deces").alias("age_moyen"),
        F.min("age_deces").alias("age_min"),
        F.max("age_deces").alias("age_max"),
        F.stddev("age_deces").alias("age_ecart_type")
    ) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp())

# Ajouter le pourcentage par rapport au total de l'année
window_annee = Window.partitionBy("annee_deces")
kpi_deces_annee = kpi_deces_annee.withColumn(
    "pourcentage_annee",
    F.round((F.col("nombre_deces") / F.sum("nombre_deces").over(window_annee) * 100), 2)
)

kpi_deces_annee.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_deces_par_annee/")
print(f"   💾 {kpi_deces_annee.count()} lignes (agrégations année/sexe/âge)")

# ===== KPI 2: DÉCÈS PAR RÉGION =====
print("\n📊 KPI_DECES_PAR_REGION")

# Jointure avec dim_geo pour avoir le département
fait_deces_geo = fait_deces.join(
    dim_geo.select("geo_id", "code_dept"),
    "geo_id",
    "left"
)

kpi_deces_region = fait_deces_geo.groupBy("annee_deces", "code_dept") \
    .agg(
        F.count("*").alias("nombre_deces"),
        F.avg("age_deces").alias("age_moyen"),
        F.countDistinct("deces_id").alias("nombre_deces_uniques")
    ) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp())

# Ajouter classement par département
window_dept = Window.partitionBy("annee_deces").orderBy(F.desc("nombre_deces"))
kpi_deces_region = kpi_deces_region.withColumn(
    "rang_departement",
    F.row_number().over(window_dept)
)

kpi_deces_region.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_deces_par_region/")
print(f"   💾 {kpi_deces_region.count()} lignes (répartition géographique)")

# ===== KPI 3: STATISTIQUES DÉMOGRAPHIQUES =====
print("\n📊 KPI_DEMOGRAPHIC_SUMMARY")

kpi_demo = fait_deces.groupBy("annee_deces", "sexe") \
    .agg(
        F.count("*").alias("total_deces"),
        F.avg("age_deces").alias("age_moyen"),
        F.expr("percentile(age_deces, 0.5)").alias("age_median"),
        F.min("age_deces").alias("age_min"),
        F.max("age_deces").alias("age_max"),
        F.stddev("age_deces").alias("age_ecart_type"),
        F.expr("percentile(age_deces, 0.25)").alias("age_q1"),
        F.expr("percentile(age_deces, 0.75)").alias("age_q3")
    ) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp())

kpi_demo.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_demographic_summary/")
print(f"   💾 {kpi_demo.count()} lignes (statistiques démographiques)")

# ===== KPI 4: TENDANCES TEMPORELLES =====
print("\n📊 KPI_TEMPORAL_TRENDS")

# Jointure avec dim_temps pour avoir mois, trimestre
fait_deces_temps = fait_deces.join(
    dim_temps.select("date_id", "annee", "mois", "trimestre"),
    "date_id",
    "left"
)

kpi_tendances = fait_deces_temps.groupBy("annee", "mois", "trimestre") \
    .agg(
        F.count("*").alias("nombre_deces"),
        F.avg("age_deces").alias("age_moyen"),
        F.countDistinct("deces_id").alias("deces_uniques")
    ) \
    .withColumn("annee_mois", F.concat(F.col("annee"), F.lit("-"), 
                                       F.lpad(F.col("mois"), 2, "0"))) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp()) \
    .orderBy("annee", "mois")

kpi_tendances.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_temporal_trends/")
print(f"   💾 {kpi_tendances.count()} lignes (tendances mensuelles)")

# ===== KPI 5: TOP DÉPARTEMENTS =====
print("\n📊 KPI_TOP_DEPARTEMENTS")

kpi_top_dept = kpi_deces_region \
    .filter(F.col("rang_departement") <= 20) \
    .select(
        "annee_deces",
        "code_dept",
        "nombre_deces",
        "age_moyen",
        "rang_departement"
    ) \
    .orderBy("annee_deces", "rang_departement")

kpi_top_dept.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_top_departements/")
print(f"   💾 {kpi_top_dept.count()} lignes (top 20 départements)")

# ===== KPI 6: DISTRIBUTION PAR CATÉGORIE D'ÂGE =====
print("\n📊 KPI_DISTRIBUTION_AGE")

kpi_distrib_age = fait_deces.groupBy("annee_deces", "categorie_age") \
    .agg(
        F.count("*").alias("nombre_deces")
    ) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp())

# Pourcentage par catégorie
window_total = Window.partitionBy("annee_deces")
kpi_distrib_age = kpi_distrib_age.withColumn(
    "pourcentage",
    F.round((F.col("nombre_deces") / F.sum("nombre_deces").over(window_total) * 100), 2)
)

kpi_distrib_age.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_distribution_age/")
print(f"   💾 {kpi_distrib_age.count()} lignes (distribution par âge)")

# ===== KPI 7: SYNTHÈSE GLOBALE =====
print("\n📊 KPI_SYNTHESE_GLOBALE")

kpi_synthese = fait_deces.groupBy("annee_deces") \
    .agg(
        F.count("*").alias("total_deces"),
        F.avg("age_deces").alias("age_moyen_global"),
        F.expr("percentile(age_deces, 0.5)").alias("age_median_global"),
        F.countDistinct("geo_id").alias("nombre_lieux_deces"),
        F.sum(F.when(F.col("sexe") == "M", 1).otherwise(0)).alias("total_hommes"),
        F.sum(F.when(F.col("sexe") == "F", 1).otherwise(0)).alias("total_femmes")
    ) \
    .withColumn("ratio_hommes_femmes", 
                F.round(F.col("total_hommes") / F.col("total_femmes"), 2)) \
    .withColumn("_gold_batch_id", F.lit(batch_id)) \
    .withColumn("_gold_load_date", F.current_timestamp())

kpi_synthese.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_synthese_globale/")
print(f"   💾 {kpi_synthese.count()} lignes (synthèse globale)")

# ===== RÉSUMÉ =====
print("\n" + "="*60)
print("🎉 PIPELINE GOLD TERMINÉ")
print("="*60)

print("\n✅ KPIs créés:")
print("   📊 kpi_deces_par_annee - Agrégations année/sexe/âge")
print("   📊 kpi_deces_par_region - Répartition géographique")
print("   📊 kpi_demographic_summary - Statistiques démographiques")
print("   📊 kpi_temporal_trends - Tendances mensuelles")
print("   📊 kpi_top_departements - Top 20 départements")
print("   📊 kpi_distribution_age - Distribution par catégories d'âge")
print("   📊 kpi_synthese_globale - Vue d'ensemble annuelle")

print("\n💾 KPIs disponibles dans s3a://gold/")
print("🎯 Prêt pour visualisation dans Superset")

spark.stop()
