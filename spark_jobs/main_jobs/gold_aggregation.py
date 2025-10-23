import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min, max,
    when, lit, datediff, floor, year, month, quarter,
    row_number, rank, percent_rank, lag, lead,
    concat_ws, split, regexp_extract, regexp_replace, upper, trim,
    date_add, current_date, current_timestamp, hour,
    broadcast, expr, array_contains, collect_list, first,
    concat
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

# Décorateur pour le monitoring
def log_transformation(func):
    """Décorateur pour logger les transformations"""
    def wrapper(*args, **kwargs):
        table_name = kwargs.get('table_name', func.__name__)
        start_time = time.time()
        print(f"🔄 [{datetime.now()}] Début: {table_name}")
        
        result = func(*args, **kwargs)
        
        duration = time.time() - start_time
        count = result.count() if hasattr(result, 'count') else 0
        print(f"✅ [{datetime.now()}] Fin: {table_name} - {count:,} lignes - {duration:.2f}s")
        
        return result
    return wrapper

# Métriques de qualité
def compute_quality_metrics(df, table_name):
    """Calcule des métriques de qualité"""
    total_rows = df.count()
    
    metrics = {
        "table": table_name,
        "total_rows": total_rows,
        "null_rates": {},
        "distinct_counts": {}
    }
    
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        metrics["null_rates"][col_name] = null_count / total_rows * 100 if total_rows > 0 else 0
        
        if total_rows < 1000000:  # Limiter pour les grandes tables
            metrics["distinct_counts"][col_name] = df.select(col_name).distinct().count()
    
    print(f"📊 Métriques {table_name}:")
    print(f"   - Lignes: {total_rows:,}")
    print(f"   - Colonnes avec >10% nulls: {[k for k,v in metrics['null_rates'].items() if v > 10]}")
    
    return metrics

def get_spark_session():
    """Session Spark optimisée pour le traitement Gold."""
    try:
        builder = SparkSession.builder \
            .appName("Gold Layer BI Superset") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skew.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Gold initialisé")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark Gold: {e}")
        raise

def read_silver_table(spark, table_name):
    """Lit une table depuis le layer Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df = spark.read.parquet(silver_path)
        print(f"✅ Silver '{table_name}' lu: {df.count()} lignes")
        return df
    except Exception as e:
        print(f"❌ Erreur lecture Silver {table_name}: {e}")
        raise

def write_gold_table(df, table_name, partition_cols=None):
    """Écrit une table dans le layer Gold."""
    try:
        gold_path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"
        
        writer = df.write.mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
            
        writer.option("compression", "snappy") \
              .option("maxRecordsPerFile", "100000") \
              .parquet(gold_path)
        
        print(f"✅ Gold '{table_name}' écrit: {df.count()} lignes")
        return True
        
    except Exception as e:
        print(f"❌ Erreur écriture Gold {table_name}: {e}")
        raise

# ============================================================================
# 1. TAUX DE CONSULTATION - ÉTABLISSEMENT X PÉRIODE Y
# ============================================================================

@log_transformation
def create_gold_taux_consultation_etablissement(spark):
    """Taux de consultation des patients dans un établissement X sur une période Y"""
    
    consultations = read_silver_table(spark, "consultations")
    etablissements = read_silver_table(spark, "etablissements")
    
    # CORRECTION : Utiliser des alias pour éviter les conflits de colonnes
    consultations_alias = consultations.alias("c")
    etablissements_alias = etablissements.select("finess_site", "raison_sociale_site", col("region").alias("region_etab")).alias("e")
    
    gold_taux_consult = consultations_alias \
        .filter(col("c.consultation_etablissement_finess").isNotNull()) \
        .groupBy(
            "c.consultation_etablissement_finess",
            "c.consultation_etablissement_nom", 
            "c.region",
            year("c.date_consultation").alias("annee"),
            quarter("c.date_consultation").alias("trimestre"),
            month("c.date_consultation").alias("mois")
        ) \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("c.id_patient").alias("nb_patients_uniques"),
            countDistinct("c.id_prof_sante").alias("nb_professionnels")
        ) \
        .join(
            etablissements_alias, 
            col("consultation_etablissement_finess") == col("e.finess_site"),
            "left"
        ) \
        .withColumn("taux_consultation_patients", 
                   col("nb_consultations") / col("nb_patients_uniques")) \
        .withColumn("période", 
                   concat(col("annee"), lit("-"), col("trimestre"))) \
        .select(
            "consultation_etablissement_finess",
            "consultation_etablissement_nom",
            col("region").alias("region_consultation"),
            "annee",
            "trimestre", 
            "mois",
            "période",
            "nb_consultations",
            "nb_patients_uniques",
            "nb_professionnels",
            "taux_consultation_patients"
        )
    
    compute_quality_metrics(gold_taux_consult, "gold_taux_consultation_etablissement")
    return gold_taux_consult

# ============================================================================
# 2. TAUX CONSULTATION PAR DIAGNOSTIC X PÉRIODE Y  
# ============================================================================

@log_transformation
def create_gold_taux_consultation_diagnostic(spark):
    """Taux de consultation des patients par rapport à un diagnostic X sur une période Y"""
    
    consultations = read_silver_table(spark, "consultations")
    diagnostics = read_silver_table(spark, "diagnostics")
    
    # Consultation par diagnostic et période
    consult_diag_periode = consultations \
        .filter(col("code_diag").isNotNull()) \
        .groupBy(
            "code_diag",
            year("date_consultation").alias("annee"),
            quarter("date_consultation").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_consultations_diagnostic"),
            countDistinct("id_patient").alias("nb_patients_diagnostic")
        )
    
    # Total consultations par période (pour calculer les taux)
    total_consult_periode = consultations \
        .groupBy(
            year("date_consultation").alias("annee"),
            quarter("date_consultation").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("id_patient").alias("nb_patients_total")
        )
    
    gold_taux_consult_diag = consult_diag_periode \
        .join(total_consult_periode, ["annee", "trimestre"]) \
        .join(diagnostics.select("code_diag", "diagnostic", "type_pathologie"), "code_diag") \
        .withColumn("taux_consultation_diagnostic", 
                   col("nb_consultations_diagnostic") / col("nb_consultations_total")) \
        .withColumn("taux_patients_diagnostic",
                   col("nb_patients_diagnostic") / col("nb_patients_total")) \
        .withColumn("période",
                   concat(col("annee"), lit("-"), col("trimestre"))) \
        .select(
            "code_diag",
            "diagnostic", 
            "type_pathologie",
            "annee",
            "trimestre",
            "période",
            "nb_consultations_diagnostic",
            "nb_patients_diagnostic", 
            "nb_consultations_total",
            "nb_patients_total",
            "taux_consultation_diagnostic",
            "taux_patients_diagnostic"
        )
    
    compute_quality_metrics(gold_taux_consult_diag, "gold_taux_consultation_diagnostic")
    return gold_taux_consult_diag

# ============================================================================
# 3. TAUX GLOBAL HOSPITALISATION PÉRIODE Y
# ============================================================================

@log_transformation
def create_gold_taux_hospitalisation_global(spark):
    """Taux global d'hospitalisation des patients dans une période donnée Y"""
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Hospitalisations par période
    hosp_periode = hospitalisations \
        .groupBy(
            year("date_admission").alias("annee"),
            quarter("date_admission").alias("trimestre"),
            month("date_admission").alias("mois")
        ) \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        )
    
    # Population totale (estimation basée sur patients uniques)
    population_base = patients.count()
    
    gold_taux_hosp_global = hosp_periode \
        .withColumn("population_totale_estimee", lit(population_base)) \
        .withColumn("taux_hospitalisation_global", 
                   col("nb_patients_hospitalises") / col("population_totale_estimee")) \
        .withColumn("période",
                   concat(col("annee"), lit("-"), col("trimestre"))) \
        .select(
            "annee",
            "trimestre", 
            "mois",
            "période",
            "nb_hospitalisations",
            "nb_patients_hospitalises",
            "population_totale_estimee",
            "taux_hospitalisation_global",
            "duree_moyenne_sejour",
            "total_jours_hospitalisation"
        )
    
    compute_quality_metrics(gold_taux_hosp_global, "gold_taux_hospitalisation_global")
    return gold_taux_hosp_global

# ============================================================================
# 4. TAUX HOSPITALISATION PAR DIAGNOSTIC PÉRIODE Y
# ============================================================================

@log_transformation
def create_gold_taux_hospitalisation_diagnostic(spark):
    """Taux d'hospitalisation des patients par rapport à des diagnostics sur une période donnée"""
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    diagnostics = read_silver_table(spark, "diagnostics")
    
    # Hospitalisations par diagnostic et période
    hosp_diag_periode = hospitalisations \
        .filter(col("diagnostic_principal").isNotNull()) \
        .groupBy(
            "diagnostic_principal",
            year("date_admission").alias("annee"),
            quarter("date_admission").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_hospitalisations_diagnostic"),
            countDistinct("id_patient").alias("nb_patients_diagnostic"),
            avg("jour_hospitalisation").alias("duree_moyenne_diagnostic")
        )
    
    # Total hospitalisations par période
    total_hosp_periode = hospitalisations \
        .groupBy(
            year("date_admission").alias("annee"),
            quarter("date_admission").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_hospitalisations_total"),
            countDistinct("id_patient").alias("nb_patients_total")
        )
    
    gold_taux_hosp_diag = hosp_diag_periode \
        .join(total_hosp_periode, ["annee", "trimestre"]) \
        .join(diagnostics.select("code_diag", "diagnostic", "gravite_pathologie"), 
              col("diagnostic_principal") == col("code_diag")) \
        .withColumn("taux_hospitalisation_diagnostic",
                   col("nb_hospitalisations_diagnostic") / col("nb_hospitalisations_total")) \
        .withColumn("taux_patients_diagnostic",
                   col("nb_patients_diagnostic") / col("nb_patients_total")) \
        .withColumn("période",
                   concat(col("annee"), lit("-"), col("trimestre"))) \
        .select(
            "diagnostic_principal",
            "diagnostic",
            "gravite_pathologie", 
            "annee",
            "trimestre",
            "période",
            "nb_hospitalisations_diagnostic",
            "nb_patients_diagnostic",
            "nb_hospitalisations_total", 
            "nb_patients_total",
            "taux_hospitalisation_diagnostic",
            "taux_patients_diagnostic",
            "duree_moyenne_diagnostic"
        )
    
    compute_quality_metrics(gold_taux_hosp_diag, "gold_taux_hospitalisation_diagnostic")
    return gold_taux_hosp_diag

# ============================================================================
# 5. TAUX HOSPITALISATION PAR SEXE ET ÂGE
# ============================================================================

@log_transformation
def create_gold_taux_hospitalisation_demographie(spark):
    """Taux d'hospitalisation par sexe, par âge"""
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Hospitalisations par sexe et catégorie d'âge
    hosp_demo = hospitalisations \
        .groupBy("sexe", "categorie_age") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour")
        )
    
    # Population par sexe et catégorie d'âge
    population_demo = patients \
        .groupBy("sexe", "categorie_age") \
        .agg(count("*").alias("population_categorie"))
    
    gold_taux_hosp_demo = hosp_demo \
        .join(population_demo, ["sexe", "categorie_age"]) \
        .withColumn("taux_hospitalisation_categorie",
                   col("nb_patients_hospitalises") / col("population_categorie")) \
        .select(
            "sexe",
            "categorie_age", 
            "nb_hospitalisations",
            "nb_patients_hospitalises",
            "population_categorie",
            "taux_hospitalisation_categorie",
            "duree_moyenne_sejour"
        )
    
    compute_quality_metrics(gold_taux_hosp_demo, "gold_taux_hospitalisation_demographie")
    return gold_taux_hosp_demo

# ============================================================================
# 6. TAUX DE CONSULTATION PAR PROFESSIONNEL
# ============================================================================

@log_transformation
def create_gold_taux_consultation_professionnel(spark):
    """Taux de consultation par professionnel"""
    
    consultations = read_silver_table(spark, "consultations")
    professionnels = read_silver_table(spark, "professionnels_sante")
    
    # Consultations par professionnel
    consult_pro = consultations \
        .filter(col("id_prof_sante").isNotNull()) \
        .groupBy("id_prof_sante") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("id_patient").alias("nb_patients_uniques"),
            countDistinct("code_diag").alias("nb_diagnostics_distincts"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation")
        )
    
    gold_taux_consult_pro = consult_pro \
        .join(professionnels.select("identifiant", "profession", "categorie_professionnelle", "niveau_activite"), 
              col("id_prof_sante") == col("identifiant")) \
        .withColumn("taux_consultation_patient",
                   col("nb_consultations_total") / col("nb_patients_uniques")) \
        .withColumn("productivite_moyenne",
                   col("nb_consultations_total") / col("nb_patients_uniques")) \
        .select(
            "id_prof_sante",
            "profession",
            "categorie_professionnelle", 
            "niveau_activite",
            "nb_consultations_total",
            "nb_patients_uniques",
            "nb_diagnostics_distincts",
            "taux_consultation_patient",
            "productivite_moyenne", 
            "duree_moyenne_consultation"
        )
    
    compute_quality_metrics(gold_taux_consult_pro, "gold_taux_consultation_professionnel")
    return gold_taux_consult_pro

# ============================================================================
# 7. NOMBRE DE DÉCÈS PAR LOCALISATION 2019
# ============================================================================

@log_transformation
def create_gold_deces_localisation_2019(spark):
    """Nombre de décès par localisation (région) et sur l'année 2019"""
    
    deces = read_silver_table(spark, "deces")
    
    gold_deces_2019 = deces \
        .filter(year("date_deces") == 2019) \
        .groupBy("region_deces", "departement_deces") \
        .agg(
            count("*").alias("nb_deces"),
            avg("age").alias("age_moyen_deces"),
            countDistinct("sexe").alias("nb_sexes_distincts")
        ) \
        .withColumn("annee", lit(2019)) \
        .select(
            "region_deces",
            "departement_deces", 
            "annee",
            "nb_deces",
            "age_moyen_deces",
            "nb_sexes_distincts"
        )
    
    compute_quality_metrics(gold_deces_2019, "gold_deces_localisation_2019")
    return gold_deces_2019

# ============================================================================
# 8. TAUX SATISFACTION PAR RÉGION 2020
# ============================================================================

@log_transformation
def create_gold_satisfaction_region_2020(spark):
    """Taux global de satisfaction par région sur l'année 2020"""
    
    satisfaction = read_silver_table(spark, "satisfaction")
    
    gold_satisfaction_2020 = satisfaction \
        .filter(col("score_all_ajust").isNotNull()) \
        .groupBy("region") \
        .agg(
            count("*").alias("nb_etablissements"),
            avg("score_all_ajust").alias("score_satisfaction_moyen"),
            avg("taux_reco_brut").alias("taux_recommandation_moyen"),
            countDistinct("niveau_satisfaction").alias("nb_niveaux_satisfaction")
        ) \
        .withColumn("annee", lit(2020)) \
        .withColumn("classement_satisfaction",
                   when(col("score_satisfaction_moyen") >= 80, "Excellente")
                   .when(col("score_satisfaction_moyen") >= 70, "Bonne")
                   .when(col("score_satisfaction_moyen") >= 60, "Satisfaisante")
                   .otherwise("Insatisfaisante")) \
        .select(
            "region",
            "annee", 
            "nb_etablissements",
            "score_satisfaction_moyen",
            "taux_recommandation_moyen",
            "classement_satisfaction",
            "nb_niveaux_satisfaction"
        )
    
    compute_quality_metrics(gold_satisfaction_2020, "gold_satisfaction_region_2020")
    return gold_satisfaction_2020

# ============================================================================
# 9. TABLE DE FAITS PRINCIPALE POUR BI
# ============================================================================

@log_transformation
def create_gold_fait_principal_bi(spark):
    """Table de faits principale pour analyses BI complètes"""
    
    consultations = read_silver_table(spark, "consultations")
    hospitalisations = read_silver_table(spark, "hospitalisations")
    deces = read_silver_table(spark, "deces")
    satisfaction = read_silver_table(spark, "satisfaction")
    
    # Agréger les consultations
    fait_consultations = consultations \
        .groupBy(
            "region",
            "categorie_age", 
            "sexe",
            year("date_consultation").alias("annee"),
            quarter("date_consultation").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("id_patient").alias("nb_patients_consultations"),
            countDistinct("id_prof_sante").alias("nb_professionnels_consultations"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation")
        )
    
    # Agréger les hospitalisations  
    fait_hospitalisations = hospitalisations \
        .groupBy(
            "region",
            "categorie_age",
            "sexe", 
            year("date_admission").alias("annee"),
            quarter("date_admission").alias("trimestre")
        ) \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalisations"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        )
    
    # Agréger les décès
    fait_deces = deces \
        .filter(year("date_deces") == 2019) \
        .groupBy("region_deces", "categorie_age", "sexe") \
        .agg(
            count("*").alias("nb_deces"),
            avg("age").alias("age_moyen_deces")
        ) \
        .withColumnRenamed("region_deces", "region") \
        .withColumn("annee", lit(2019))
    
    # Agréger la satisfaction
    fait_satisfaction = satisfaction \
        .groupBy("region") \
        .agg(
            count("*").alias("nb_etablissements_satisfaction"),
            avg("score_all_ajust").alias("score_satisfaction_moyen"),
            avg("taux_reco_brut").alias("taux_recommandation_moyen")
        ) \
        .withColumn("annee", lit(2020))
    
    # Union des faits
    gold_fait_principal = fait_consultations \
        .join(fait_hospitalisations, ["region", "categorie_age", "sexe", "annee", "trimestre"], "full") \
        .join(fait_deces, ["region", "categorie_age", "sexe", "annee"], "full") \
        .join(fait_satisfaction, ["region", "annee"], "full") \
        .fillna(0) \
        .withColumn("période", 
                   when(col("trimestre").isNotNull(), 
                        concat(col("annee"), lit("-T"), col("trimestre")))
                   .otherwise(col("annee").cast("string"))) \
        .select(
            "region",
            "categorie_age", 
            "sexe",
            "annee",
            "trimestre",
            "période",
            "nb_consultations",
            "nb_patients_consultations",
            "nb_professionnels_consultations", 
            "duree_moyenne_consultation",
            "nb_hospitalisations",
            "nb_patients_hospitalisations",
            "duree_moyenne_sejour", 
            "total_jours_hospitalisation",
            "nb_deces",
            "age_moyen_deces",
            "nb_etablissements_satisfaction",
            "score_satisfaction_moyen",
            "taux_recommandation_moyen"
        )
    
    compute_quality_metrics(gold_fait_principal, "gold_fait_principal_bi")
    return gold_fait_principal

# ============================================================================
# EXÉCUTION PRINCIPALE
# ============================================================================

def main():
    """Exécute le pipeline complet du Gold Layer."""
    
    print("""
    ╔══════════════════════════════════════════╗
    ║           GOLD LAYER PIPELINE            ║
    ║         BI SUPERSET - ANALYSES           ║
    ╚══════════════════════════════════════════╝
    """)
    
    try:
        # Initialisation Spark
        spark = get_spark_session()
        
        # 1. Taux consultation établissement
        print("📊 Création: Taux consultation par établissement...")
        gold1 = create_gold_taux_consultation_etablissement(spark)
        write_gold_table(gold1, "gold_taux_consultation_etablissement")
        
        # 2. Taux consultation diagnostic  
        print("📊 Création: Taux consultation par diagnostic...")
        gold2 = create_gold_taux_consultation_diagnostic(spark)
        write_gold_table(gold2, "gold_taux_consultation_diagnostic")
        
        # 3. Taux hospitalisation global
        print("📊 Création: Taux hospitalisation global...")
        gold3 = create_gold_taux_hospitalisation_global(spark)
        write_gold_table(gold3, "gold_taux_hospitalisation_global")
        
        # 4. Taux hospitalisation diagnostic
        print("📊 Création: Taux hospitalisation par diagnostic...")
        gold4 = create_gold_taux_hospitalisation_diagnostic(spark)
        write_gold_table(gold4, "gold_taux_hospitalisation_diagnostic")
        
        # 5. Taux hospitalisation démographie
        print("📊 Création: Taux hospitalisation par sexe/âge...")
        gold5 = create_gold_taux_hospitalisation_demographie(spark)
        write_gold_table(gold5, "gold_taux_hospitalisation_demographie")
        
        # 6. Taux consultation professionnel
        print("📊 Création: Taux consultation par professionnel...")
        gold6 = create_gold_taux_consultation_professionnel(spark)
        write_gold_table(gold6, "gold_taux_consultation_professionnel")
        
        # 7. Décès localisation 2019
        print("📊 Création: Décès par localisation 2019...")
        gold7 = create_gold_deces_localisation_2019(spark)
        write_gold_table(gold7, "gold_deces_localisation_2019")
        
        # 8. Satisfaction région 2020
        print("📊 Création: Satisfaction par région 2020...")
        gold8 = create_gold_satisfaction_region_2020(spark)
        write_gold_table(gold8, "gold_satisfaction_region_2020")
        
        # 9. Table de faits principale
        print("📊 Création: Table de faits principale BI...")
        gold9 = create_gold_fait_principal_bi(spark)
        write_gold_table(gold9, "gold_fait_principal_bi")
        
        # RAPPORT FINAL
        print(f"""
    🎉 GOLD LAYER TERMINÉ AVEC SUCCÈS!

    📈 TABLES GOLD CRÉÉES:
    ✅ gold_taux_consultation_etablissement: {gold1.count():,} lignes
    ✅ gold_taux_consultation_diagnostic: {gold2.count():,} lignes  
    ✅ gold_taux_hospitalisation_global: {gold3.count():,} lignes
    ✅ gold_taux_hospitalisation_diagnostic: {gold4.count():,} lignes
    ✅ gold_taux_hospitalisation_demographie: {gold5.count():,} lignes
    ✅ gold_taux_consultation_professionnel: {gold6.count():,} lignes
    ✅ gold_deces_localisation_2019: {gold7.count():,} lignes
    ✅ gold_satisfaction_region_2020: {gold8.count():,} lignes
    ✅ gold_fait_principal_bi: {gold9.count():,} lignes

    🎯 RÉPONSE À TOUTES LES QUESTIONS MÉTIER:
    
    1. 📍 Taux consultation établissement X période Y → gold_taux_consultation_etablissement
    2. 🩺 Taux consultation diagnostic X période Y → gold_taux_consultation_diagnostic  
    3. 🏥 Taux hospitalisation global période Y → gold_taux_hospitalisation_global
    4. 🔬 Taux hospitalisation diagnostics période Y → gold_taux_hospitalisation_diagnostic
    5. 👥 Taux hospitalisation sexe/âge → gold_taux_hospitalisation_demographie
    6. 👨‍⚕️ Taux consultation professionnel → gold_taux_consultation_professionnel
    7. ⚰️  Décès par localisation 2019 → gold_deces_localisation_2019
    8. 😊 Satisfaction par région 2020 → gold_satisfaction_region_2020
    9. 📊 Vue consolidée BI → gold_fait_principal_bi

    🚀 PRÊT POUR L'ANALYSE BI SUPERSET!
        """)
        
        spark.stop()
        
    except Exception as e:
        print(f"💥 Erreur pipeline Gold: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()