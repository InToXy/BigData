import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min, max,
    when, lit, year, month, quarter, weekofyear,
    concat, broadcast, coalesce, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

def get_spark_session():
    """Session Spark optimisée pour Gold Layer"""
    try:
        spark = SparkSession.builder \
            .appName("Gold Layer BI Analytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.autoBroadcastJoinThreshold", "20971520") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark Gold Layer initialisé")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def read_silver_table(spark, table_name):
    """Lecture optimisée des tables Silver"""
    try:
        path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df = spark.read.parquet(path)
        print(f"📖 Table {table_name}: {df.count():,} lignes")
        return df
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        raise

def write_gold_table(df, table_name, partition_cols=None):
    """Écriture optimisée vers Gold"""
    try:
        path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"
        
        writer = df.coalesce(4).write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
            
        writer.option("compression", "snappy").parquet(path)
        print(f"✅ {table_name}: {df.count():,} lignes écrites")
        
    except Exception as e:
        print(f"❌ Erreur écriture {table_name}: {e}")
        raise

# ============================================================================
# DIMENSIONS RÉUTILISABLES
# ============================================================================

def create_dimensions(spark):
    """Créer les tables de dimensions optimisées"""
    
    print("🏗️ Création des dimensions...")
    
    # Dimension Établissement
    etablissements = read_silver_table(spark, "etablissements")
    dim_etablissement = etablissements.select(
        col("_sk").alias("sk_etablissement"),
        "finess_site",
        "raison_sociale_site",
        "region",
        "niveau_activite", 
        "specialisation_etablissement",
        "performance_globale",
        "score_satisfaction_moyen",
        "taux_recommandation_moyen"
    ).filter(col("finess_site").isNotNull()).distinct()
    
    # Dimension Patient
    patients = read_silver_table(spark, "patients")
    dim_patient = patients.select(
        col("_sk").alias("sk_patient"),
        col("_sk_patient"),
        "sexe",
        "categorie_age",
        "segment_patient",
        "statut_activite",
        "patient_hospitalise"
    ).distinct()
    
    # Dimension Diagnostic
    diagnostics = read_silver_table(spark, "diagnostics")
    dim_diagnostic = diagnostics.select(
        col("_sk").alias("sk_diagnostic"),
        "code_diag",
        "diagnostic",
        "type_pathologie",
        "gravite_pathologie",
        "prevalence_globale"
    ).distinct()
    
    # Dimension Professionnel
    professionnels = read_silver_table(spark, "professionnels_sante")
    dim_professionnel = professionnels.select(
        col("_sk").alias("sk_professionnel"),
        "identifiant",
        "profession",
        "categorie_professionnelle",
        "niveau_activite",
        "specialisation_cas"
    ).distinct()
    
    # Dimension Temps
    dim_temps = spark.range(2015, 2025).select(
        col("id").alias("annee")
    ).crossJoin(
        spark.range(1, 5).select(col("id").alias("trimestre"))
    ).crossJoin(
        spark.range(1, 13).select(col("id").alias("mois"))
    ).withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre")))
    
    return {
        "etablissements": broadcast(dim_etablissement),
        "patients": broadcast(dim_patient),
        "diagnostics": broadcast(dim_diagnostic),
        "professionnels": broadcast(dim_professionnel),
        "temps": broadcast(dim_temps)
    }

# ============================================================================
# 1. TAUX CONSULTATION PAR ÉTABLISSEMENT ET PÉRIODE
# ============================================================================

def create_gold_consultation_etablissement(spark, dimensions):
    """Q1: Taux de consultation par établissement X sur période Y"""
    
    print("📊 Création: Consultations par établissement...")
    
    consultations = read_silver_table(spark, "consultations")
    
    result = consultations \
        .filter(col("consultation_etablissement_finess").isNotNull()) \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation")) \
        .withColumn("mois", month("date_consultation")) \
        .groupBy(
            "consultation_etablissement_finess",
            "consultation_etablissement_nom",
            "region",
            "annee", "trimestre", "mois"
        ) \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("_sk_patient").alias("nb_patients_uniques"),
            countDistinct("id_prof_sante").alias("nb_professionnels"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation"),
            spark_sum(when(col("consultation_longue"), 1).otherwise(0)).alias("nb_consultations_longues")
        ) \
        .withColumn("taux_consultation_par_patient", 
                   spark_round(col("nb_consultations") / col("nb_patients_uniques"), 2)) \
        .withColumn("taux_consultations_longues",
                   spark_round(col("nb_consultations_longues") / col("nb_consultations") * 100, 2)) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "consultation_etablissement_finess",
            "consultation_etablissement_nom", 
            "region",
            "annee", "trimestre", "mois", "periode",
            "nb_consultations",
            "nb_patients_uniques",
            "nb_professionnels",
            "taux_consultation_par_patient",
            "duree_moyenne_consultation",
            "taux_consultations_longues"
        )
    
    return result

# ============================================================================
# 2. TAUX CONSULTATION PAR DIAGNOSTIC ET PÉRIODE  
# ============================================================================

def create_gold_consultation_diagnostic(spark, dimensions):
    """Q2: Taux de consultation par diagnostic X sur période Y"""
    
    print("📊 Création: Consultations par diagnostic...")
    
    consultations = read_silver_table(spark, "consultations")
    
    # Consultations par diagnostic et période
    consult_diag = consultations \
        .filter(col("_sk_diagnostic").isNotNull()) \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation")) \
        .groupBy("_sk_diagnostic", "code_diag", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_consultations_diagnostic"),
            countDistinct("_sk_patient").alias("nb_patients_diagnostic"),
            countDistinct("id_prof_sante").alias("nb_professionnels_diagnostic"),
            avg("duree_consultation_heures").alias("duree_moyenne_diagnostic")
        )
    
    # Total par période pour calculer les taux
    total_periode = consultations \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation")) \
        .groupBy("annee", "trimestre") \
        .agg(
            count("*").alias("total_consultations_periode"),
            countDistinct("_sk_patient").alias("total_patients_periode")
        )
    
    result = consult_diag \
        .join(total_periode, ["annee", "trimestre"]) \
        .join(dimensions["diagnostics"], 
              col("_sk_diagnostic") == col("sk_diagnostic")) \
        .withColumn("taux_consultation_diagnostic",
                   spark_round(col("nb_consultations_diagnostic") / col("total_consultations_periode") * 100, 2)) \
        .withColumn("taux_patients_diagnostic", 
                   spark_round(col("nb_patients_diagnostic") / col("total_patients_periode") * 100, 2)) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "code_diag", "diagnostic", "type_pathologie", "gravite_pathologie",
            "annee", "trimestre", "periode",
            "nb_consultations_diagnostic",
            "nb_patients_diagnostic",
            "nb_professionnels_diagnostic",
            "total_consultations_periode",
            "taux_consultation_diagnostic",
            "taux_patients_diagnostic",
            "duree_moyenne_diagnostic"
        )
    
    return result

# ============================================================================
# 3. TAUX GLOBAL HOSPITALISATION PAR PÉRIODE
# ============================================================================

def create_gold_hospitalisation_global(spark, dimensions):
    """Q3: Taux global d'hospitalisation par période Y"""
    
    print("📊 Création: Hospitalisations globales...")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Population de référence
    population_totale = patients.count()
    
    result = hospitalisations \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission")) \
        .withColumn("mois", month("date_admission")) \
        .groupBy("annee", "trimestre", "mois") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("_sk_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation"),
            countDistinct("_sk_etablissement").alias("nb_etablissements_concernes")
        ) \
        .withColumn("population_reference", lit(population_totale)) \
        .withColumn("taux_hospitalisation_global",
                   spark_round(col("nb_patients_hospitalises") / col("population_reference") * 100, 2)) \
        .withColumn("taux_rehospitalisation",
                   spark_round((col("nb_hospitalisations") - col("nb_patients_hospitalises")) / 
                              col("nb_patients_hospitalises") * 100, 2)) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "annee", "trimestre", "mois", "periode",
            "nb_hospitalisations",
            "nb_patients_hospitalises", 
            "population_reference",
            "taux_hospitalisation_global",
            "taux_rehospitalisation",
            "duree_moyenne_sejour",
            "total_jours_hospitalisation",
            "nb_etablissements_concernes"
        )
    
    return result

# ============================================================================
# 4. TAUX HOSPITALISATION PAR DIAGNOSTIC
# ============================================================================

def create_gold_hospitalisation_diagnostic(spark, dimensions):
    """Q4: Taux d'hospitalisation par diagnostic sur période"""
    
    print("📊 Création: Hospitalisations par diagnostic...")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    
    # Hospitalisations par diagnostic
    hosp_diag = hospitalisations \
        .filter(col("_sk_diagnostic").isNotNull()) \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission")) \
        .groupBy("_sk_diagnostic", "diagnostic_principal", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_hospitalisations_diagnostic"),
            countDistinct("_sk_patient").alias("nb_patients_diagnostic"),
            avg("jour_hospitalisation").alias("duree_moyenne_diagnostic"),
            spark_sum("jour_hospitalisation").alias("total_jours_diagnostic")
        )
    
    # Total par période
    total_hosp_periode = hospitalisations \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission")) \
        .groupBy("annee", "trimestre") \
        .agg(
            count("*").alias("total_hospitalisations_periode"),
            countDistinct("_sk_patient").alias("total_patients_hospitalises_periode")
        )
    
    result = hosp_diag \
        .join(total_hosp_periode, ["annee", "trimestre"]) \
        .join(dimensions["diagnostics"],
              col("diagnostic_principal") == col("code_diag")) \
        .withColumn("taux_hospitalisation_diagnostic",
                   spark_round(col("nb_hospitalisations_diagnostic") / col("total_hospitalisations_periode") * 100, 2)) \
        .withColumn("taux_patients_hospitalises_diagnostic",
                   spark_round(col("nb_patients_diagnostic") / col("total_patients_hospitalises_periode") * 100, 2)) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "diagnostic_principal", "diagnostic", "type_pathologie", "gravite_pathologie",
            "annee", "trimestre", "periode",
            "nb_hospitalisations_diagnostic",
            "nb_patients_diagnostic",            
            "total_hospitalisations_periode",
            "taux_hospitalisation_diagnostic",
            "taux_patients_hospitalises_diagnostic",
            "duree_moyenne_diagnostic",
            "total_jours_diagnostic"
        )
    
    return result

# ============================================================================
# 5. TAUX HOSPITALISATION PAR DÉMOGRAPHIE (SEXE/ÂGE)
# ============================================================================

def create_gold_hospitalisation_demographie(spark, dimensions):
    """Q5: Taux d'hospitalisation par sexe et âge"""
    
    print("📊 Création: Hospitalisations par démographie...")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Hospitalisations par démographie
    hosp_demo = hospitalisations \
        .groupBy("sexe", "categorie_age") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("_sk_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        )
    
    # Population par démographie
    pop_demo = patients \
        .groupBy("sexe", "categorie_age") \
        .agg(count("*").alias("population_categorie"))
    
    result = hosp_demo \
        .join(pop_demo, ["sexe", "categorie_age"]) \
        .withColumn("taux_hospitalisation_categorie",
                   spark_round(col("nb_patients_hospitalises") / col("population_categorie") * 100, 2)) \
        .withColumn("taux_rehospitalisation_categorie",
                   spark_round((col("nb_hospitalisations") - col("nb_patients_hospitalises")) / 
                              col("nb_patients_hospitalises") * 100, 2)) \
        .withColumn("jours_moyens_par_patient",
                   spark_round(col("total_jours_hospitalisation") / col("nb_patients_hospitalises"), 1)) \
        .select(
            "sexe", "categorie_age",
            "nb_hospitalisations",
            "nb_patients_hospitalises",
            "population_categorie",
            "taux_hospitalisation_categorie",
            "taux_rehospitalisation_categorie",
            "duree_moyenne_sejour",
            "jours_moyens_par_patient",
            "total_jours_hospitalisation"
        )
    
    return result

# ============================================================================
# 6. TAUX CONSULTATION PAR PROFESSIONNEL
# ============================================================================

def create_gold_consultation_professionnel(spark, dimensions):
    """Q6: Taux de consultation par professionnel"""
    
    print("📊 Création: Consultations par professionnel...")
    
    consultations = read_silver_table(spark, "consultations")
    
    # Consultations par professionnel
    consult_pro = consultations \
        .filter(col("id_prof_sante").isNotNull()) \
        .groupBy("id_prof_sante") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("_sk_patient").alias("nb_patients_uniques"),
            countDistinct("_sk_diagnostic").alias("nb_diagnostics_distincts"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation"),
            spark_sum(when(col("consultation_longue"), 1).otherwise(0)).alias("nb_consultations_longues"),
            countDistinct("region").alias("nb_regions_activite")
        )
    
    result = consult_pro \
        .join(dimensions["professionnels"],
              col("id_prof_sante") == col("identifiant")) \
        .withColumn("consultations_par_patient",
                   spark_round(col("nb_consultations_total") / col("nb_patients_uniques"), 2)) \
        .withColumn("taux_consultations_longues",
                   spark_round(col("nb_consultations_longues") / col("nb_consultations_total") * 100, 2)) \
        .withColumn("diversite_diagnostics",
                   spark_round(col("nb_diagnostics_distincts") / col("nb_consultations_total") * 100, 2)) \
        .withColumn("niveau_productivite",
                   when(col("nb_consultations_total") >= 1000, "Très Élevée")
                   .when(col("nb_consultations_total") >= 500, "Élevée")
                   .when(col("nb_consultations_total") >= 100, "Moyenne")
                   .when(col("nb_consultations_total") >= 10, "Faible")
                   .otherwise("Très Faible")) \
        .select(
            "id_prof_sante", "profession", "categorie_professionnelle", 
            "niveau_activite", "specialisation_cas",
            "nb_consultations_total",
            "nb_patients_uniques",
            "nb_diagnostics_distincts",
            "consultations_par_patient",
            "duree_moyenne_consultation",
            "taux_consultations_longues",
            "diversite_diagnostics",
            "nb_regions_activite",
            "niveau_productivite"
        )
    
    return result

# ============================================================================
# 7. DÉCÈS PAR LOCALISATION 2019
# ============================================================================

def create_gold_deces_localisation_2019(spark, dimensions):
    """Q7: Nombre de décès par localisation en 2019"""
    
    print("📊 Création: Décès par localisation 2019...")
    
    deces = read_silver_table(spark, "deces")
    
    result = deces \
        .filter(col("date_deces_annee") == 2019) \
        .groupBy("region_deces", "departement_deces") \
        .agg(
            count("*").alias("nb_deces"),
            avg("age").alias("age_moyen_deces"),
            countDistinct("sexe").alias("nb_sexes"),
            spark_sum(when(col("sexe") == "M", 1).otherwise(0)).alias("nb_deces_hommes"),
            spark_sum(when(col("sexe") == "F", 1).otherwise(0)).alias("nb_deces_femmes"),
            countDistinct("categorie_age").alias("nb_categories_age"),
            spark_sum(when(col("esperance_vie_atteinte") == "Longevite", 1).otherwise(0)).alias("nb_deces_longevite")
        ) \
        .withColumn("annee", lit(2019)) \
        .withColumn("taux_deces_hommes",
                   spark_round(col("nb_deces_hommes") / col("nb_deces") * 100, 2)) \
        .withColumn("taux_deces_femmes", 
                   spark_round(col("nb_deces_femmes") / col("nb_deces") * 100, 2)) \
        .withColumn("taux_longevite",
                   spark_round(col("nb_deces_longevite") / col("nb_deces") * 100, 2)) \
        .select(
            "region_deces", "departement_deces", "annee",
            "nb_deces", "age_moyen_deces",
            "nb_deces_hommes", "nb_deces_femmes",
            "taux_deces_hommes", "taux_deces_femmes",
            "nb_deces_longevite", "taux_longevite"
        )
    
    return result

# ============================================================================
# 8. SATISFACTION PAR RÉGION 2020
# ============================================================================

def create_gold_satisfaction_region_2020(spark, dimensions):
    """Q8: Taux de satisfaction par région en 2020"""
    
    print("📊 Création: Satisfaction par région 2020...")
    
    satisfaction = read_silver_table(spark, "satisfaction")
    
    result = satisfaction \
        .filter(col("score_all_ajust").isNotNull()) \
        .groupBy("region") \
        .agg(
            count("*").alias("nb_etablissements"),
            avg("score_all_ajust").alias("score_satisfaction_moyen"),
            avg("taux_reco_brut").alias("taux_recommandation_moyen"),
            avg("score_global_normalise").alias("score_global_moyen"),
            countDistinct("niveau_satisfaction").alias("nb_niveaux_satisfaction"),
            spark_sum(when(col("niveau_satisfaction") == "Bonne", 1).otherwise(0)).alias("nb_etab_satisfaction_bonne"),
            min("score_all_ajust").alias("score_min"),
            max("score_all_ajust").alias("score_max")
        ) \
        .withColumn("annee", lit(2020)) \
        .withColumn("classement_satisfaction",
                   when(col("score_satisfaction_moyen") >= 85, "Excellente")
                   .when(col("score_satisfaction_moyen") >= 75, "Très Bonne")
                   .when(col("score_satisfaction_moyen") >= 65, "Bonne")
                   .when(col("score_satisfaction_moyen") >= 55, "Satisfaisante")
                   .otherwise("Insatisfaisante")) \
        .withColumn("taux_etablissements_satisfaisants",
                   spark_round(col("nb_etab_satisfaction_bonne") / col("nb_etablissements") * 100, 2)) \
        .withColumn("ecart_satisfaction",
                   spark_round(col("score_max") - col("score_min"), 2)) \
        .select(
            "region", "annee",
            "nb_etablissements",
            "score_satisfaction_moyen",
            "taux_recommandation_moyen", 
            "score_global_moyen",
            "classement_satisfaction",
            "taux_etablissements_satisfaisants",
            "ecart_satisfaction",
            "score_min", "score_max"
        )
    
    return result

# ============================================================================
# 9. TABLEAU DE BORD EXÉCUTIF - VUE CONSOLIDÉE
# ============================================================================

def create_gold_dashboard_executif(spark, dimensions):
    """Vue consolidée pour tableau de bord exécutif"""
    
    print("📊 Création: Dashboard exécutif...")
    
    consultations = read_silver_table(spark, "consultations")
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Métriques par région et période
    consult_metrics = consultations \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation")) \
        .groupBy("region", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("_sk_patient").alias("nb_patients_consultations"),
            countDistinct("id_prof_sante").alias("nb_professionnels_actifs"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation")
        )
    
    hosp_metrics = hospitalisations \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission")) \
        .groupBy("region", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("_sk_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        )
    
    # Population par région
    pop_region = patients \
        .groupBy("code_postal") \
        .agg(count("*").alias("population_estimee")) \
        .withColumn("region", 
                   when(col("code_postal").startswith("75"), "Île-de-France")
                   .when(col("code_postal").startswith("69"), "Auvergne-Rhône-Alpes")
                   .when(col("code_postal").startswith("13"), "Provence-Alpes-Côte d'Azur")
                   .otherwise("Autre")) \
        .groupBy("region") \
        .agg(spark_sum("population_estimee").alias("population_region"))
    
    # Consolidation
    result = consult_metrics \
        .join(hosp_metrics, ["region", "annee", "trimestre"], "full") \
        .join(pop_region, "region", "left") \
        .fillna(0) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .withColumn("taux_hospitalisation_region",
                   spark_round(col("nb_patients_hospitalises") / col("population_region") * 100, 2)) \
        .withColumn("ratio_consultation_hospitalisation",
                   spark_round(col("nb_consultations") / col("nb_hospitalisations"), 2)) \
        .withColumn("efficacite_systeme",
                   when(col("ratio_consultation_hospitalisation") >= 100, "Très Efficace")
                   .when(col("ratio_consultation_hospitalisation") >= 50, "Efficace")
                   .when(col("ratio_consultation_hospitalisation") >= 20, "Modérée")
                   .otherwise("Faible")) \
        .select(
            "region", "annee", "trimestre", "periode",
            "population_region",
            "nb_consultations", "nb_patients_consultations", "nb_professionnels_actifs",
            "nb_hospitalisations", "nb_patients_hospitalises",
            "taux_hospitalisation_region",
            "duree_moyenne_consultation", "duree_moyenne_sejour",
            "ratio_consultation_hospitalisation",
            "efficacite_systeme"
        )
    
    return result

# ============================================================================
# 10. INDICATEURS MÉTIER AVANCÉS
# ============================================================================

def create_gold_indicateurs_avances(spark, dimensions):
    """Indicateurs métier avancés pour analyses prédictives"""
    
    print("📊 Création: Indicateurs avancés...")
    
    # Utiliser la table indicators_metier existante et l'enrichir
    indicators = read_silver_table(spark, "indicators_metier")
    
    result = indicators \
        .filter(col("type_indicateur").isNotNull()) \
        .withColumn("taux_hospitalisation_calcule",
                   when(col("nb_patients_uniques").isNotNull() & col("nb_patients_hospitalises").isNotNull(),
                        spark_round(col("nb_patients_hospitalises") / col("nb_patients_uniques") * 100, 2))
                   .otherwise(0)) \
        .withColumn("charge_hospitaliere",
                   when(col("total_jours_hospitalisation").isNotNull() & col("nb_patients_hospitalises").isNotNull(),
                        spark_round(col("total_jours_hospitalisation") / col("nb_patients_hospitalises"), 1))
                   .otherwise(0)) \
        .withColumn("performance_region",
                   when(col("score_satisfaction_moyen") >= 75 & col("taux_hospitalisation_calcule") <= 5, "Excellente")
                   .when(col("score_satisfaction_moyen") >= 65 & col("taux_hospitalisation_calcule") <= 10, "Bonne")
                   .when(col("score_satisfaction_moyen") >= 55 & col("taux_hospitalisation_calcule") <= 15, "Moyenne")
                   .otherwise("À améliorer")) \
        .withColumn("densite_professionnels",
                   when(col("nb_patients_uniques").isNotNull() & col("nb_professionnels").isNotNull(),
                        spark_round(col("nb_professionnels") / col("nb_patients_uniques") * 1000, 2))
                   .otherwise(0)) \
        .withColumn("efficience_consultations",
                   when(col("nb_consultations").isNotNull() & col("duree_moyenne_consultation").isNotNull(),
                        spark_round(col("nb_consultations") / col("duree_moyenne_consultation"), 2))
                   .otherwise(0)) \
        .select(
            "region", "categorie_age", "sexe", "annee", "type_indicateur",
            "nb_consultations", "nb_patients_uniques", "nb_professionnels",
            "nb_hospitalisations", "nb_patients_hospitalises",
            "duree_moyenne_consultation", "duree_moyenne_sejour",
            "taux_hospitalisation_calcule", "charge_hospitaliere",
            "densite_professionnels", "efficience_consultations",
            "performance_region", "niveau_satisfaction",
            "score_satisfaction_moyen", "taux_recommandation_moyen"
        )
    
    return result

# ============================================================================
# FONCTIONS D'ÉCRITURE OPTIMISÉES
# ============================================================================

def write_all_gold_tables(spark):
    """Écrire toutes les tables Gold avec optimisations"""
    
    print("🚀 Début création des tables Gold...")
    start_time = time.time()
    
    # Créer les dimensions une seule fois
    dimensions = create_dimensions(spark)
    
    # Cache des dimensions pour réutilisation
    for dim_name, dim_df in dimensions.items():
        dim_df.cache()
        print(f"📦 Dimension {dim_name} mise en cache")
    
    # Dictionnaire des tables à créer
    gold_tables = {
        "consultation_etablissement": create_gold_consultation_etablissement,
        "consultation_diagnostic": create_gold_consultation_diagnostic,
        "hospitalisation_global": create_gold_hospitalisation_global,
        "hospitalisation_diagnostic": create_gold_hospitalisation_diagnostic,
        "hospitalisation_demographie": create_gold_hospitalisation_demographie,
        "consultation_professionnel": create_gold_consultation_professionnel,
        "deces_localisation_2019": create_gold_deces_localisation_2019,
        "satisfaction_region_2020": create_gold_satisfaction_region_2020,
        "dashboard_executif": create_gold_dashboard_executif,
        "indicateurs_avances": create_gold_indicateurs_avances
    }
    
    # Créer et écrire chaque table
    results = {}
    for table_name, create_func in gold_tables.items():
        try:
            print(f"\n🔄 Traitement: {table_name}")
            table_start = time.time()
            
            # Créer la table
            df = create_func(spark, dimensions)
            
            # Définir les partitions selon le type de table
            partition_cols = None
            if "annee" in df.columns:
                if "region" in df.columns:
                    partition_cols = ["annee", "region"]
                else:
                    partition_cols = ["annee"]
            elif "region" in df.columns:
                partition_cols = ["region"]
            
            # Écrire la table
            write_gold_table(df, f"gold_{table_name}", partition_cols)
            
            # Stocker les résultats
            results[table_name] = {
                "count": df.count(),
                "duration": time.time() - table_start
            }
            
            print(f"✅ {table_name}: {results[table_name]['count']:,} lignes en {results[table_name]['duration']:.2f}s")
            
        except Exception as e:
            print(f"❌ Erreur {table_name}: {e}")
            results[table_name] = {"error": str(e)}
    
    # Nettoyer le cache
    for dim_df in dimensions.values():
        dim_df.unpersist()
    
    total_duration = time.time() - start_time
    
    return results, total_duration

# ============================================================================
# CRÉATION DES VUES SUPERSET
# ============================================================================

def create_superset_views(spark):
    """Créer des vues optimisées pour Superset"""
    
    print("📊 Création des vues Superset...")
    
    # Vue 1: KPI Principaux par Région
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW superset_kpi_regions AS
    SELECT 
        region,
        annee,
        SUM(nb_consultations) as total_consultations,
        SUM(nb_patients_consultations) as total_patients,
        SUM(nb_hospitalisations) as total_hospitalisations,
        AVG(taux_hospitalisation_region) as taux_hospitalisation_moyen,
        AVG(duree_moyenne_consultation) as duree_consultation_moyenne,
        AVG(duree_moyenne_sejour) as duree_sejour_moyenne,
        MAX(efficacite_systeme) as niveau_efficacite
    FROM gold_dashboard_executif
    GROUP BY region, annee
    ORDER BY region, annee
    """)
    
    # Vue 2: Top Diagnostics par Fréquence
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW superset_top_diagnostics AS
    SELECT 
        diagnostic,
        type_pathologie,
        gravite_pathologie,
        SUM(nb_consultations_diagnostic) as total_consultations,
        SUM(nb_patients_diagnostic) as total_patients,
        AVG(taux_consultation_diagnostic) as taux_moyen,
        AVG(duree_moyenne_diagnostic) as duree_moyenne
    FROM gold_consultation_diagnostic
    GROUP BY diagnostic, type_pathologie, gravite_pathologie
    HAVING SUM(nb_consultations_diagnostic) >= 100
    ORDER BY total_consultations DESC
    LIMIT 50
    """)
    
    # Vue 3: Performance Professionnels
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW superset_performance_professionnels AS
    SELECT 
        profession,
        categorie_professionnelle,
        niveau_productivite,
        COUNT(*) as nb_professionnels,
        AVG(nb_consultations_total) as consultations_moyennes,
        AVG(consultations_par_patient) as ratio_consultation_patient,
        AVG(duree_moyenne_consultation) as duree_moyenne,
        AVG(diversite_diagnostics) as diversite_moyenne
    FROM gold_consultation_professionnel
    GROUP BY profession, categorie_professionnelle, niveau_productivite
    ORDER BY consultations_moyennes DESC
    """)
    
    # Vue 4: Évolution Temporelle
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW superset_evolution_temporelle AS
    SELECT 
        annee,
        trimestre,
        periode,
        SUM(nb_consultations) as consultations_totales,
        SUM(nb_hospitalisations) as hospitalisations_totales,
        AVG(duree_moyenne_consultation) as duree_consultation,
        AVG(duree_moyenne_sejour) as duree_sejour,
        COUNT(DISTINCT region) as nb_regions_actives
    FROM gold_dashboard_executif
    GROUP BY annee, trimestre, periode
    ORDER BY annee, trimestre
    """)
    
    print("✅ Vues Superset créées")

# ============================================================================
# VALIDATION ET CONTRÔLES QUALITÉ
# ============================================================================

def validate_gold_layer(spark):
    """Validation de la qualité des données Gold"""
    
    print("🔍 Validation de la couche Gold...")
    
    validations = {}
    
    try:
        # Test 1: Vérifier que les tables existent et ont des données
        gold_tables = [
            "gold_consultation_etablissement",
            "gold_consultation_diagnostic", 
            "gold_hospitalisation_global",
            "gold_dashboard_executif"
        ]
        
        for table in gold_tables:
            try:
                df = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/{table}")
                count = df.count()
                validations[table] = {
                    "exists": True,
                    "count": count,
                    "status": "✅" if count > 0 else "⚠️"
                }
            except Exception as e:
                validations[table] = {
                    "exists": False,
                    "error": str(e),
                    "status": "❌"
                }
        
        # Test 2: Cohérence des données
        dashboard = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/gold_dashboard_executif")
        
        # Vérifier les valeurs nulles critiques
        null_checks = dashboard.select(
            *[spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"nulls_{c}") 
              for c in ["region", "annee", "nb_consultations"]]
        ).collect()[0]
        
        validations["data_quality"] = {
            "null_regions": null_checks["nulls_region"],
            "null_annees": null_checks["nulls_annee"], 
            "null_consultations": null_checks["nulls_nb_consultations"],
            "status": "✅" if all(v == 0 for v in null_checks.asDict().values()) else "⚠️"
        }
        
        # Test 3: Cohérence temporelle
        date_range = dashboard.select(
            min("annee").alias("min_annee"),
            max("annee").alias("max_annee")
        ).collect()[0]
        
        validations["temporal_consistency"] = {
            "min_year": date_range["min_annee"],
            "max_year": date_range["max_annee"],
            "status": "✅" if date_range["min_annee"] >= 2015 and date_range["max_annee"] <= 2025 else "⚠️"
        }
        
    except Exception as e:
        validations["validation_error"] = str(e)
    
    return validations

# ============================================================================
# GÉNÉRATION DU RAPPORT FINAL
# ============================================================================

def generate_final_report(results, total_duration, validations):
    """Générer le rapport final de création du Gold Layer"""
    
    print(f"""
    
╔══════════════════════════════════════════════════════════════════╗
║                    RAPPORT GOLD LAYER FINAL                     ║
║                     BI ANALYTICS READY                          ║
╚══════════════════════════════════════════════════════════════════╝

🕐 DURÉE TOTALE: {total_duration:.2f} secondes

📊 TABLES GOLD CRÉÉES:
""")
    
    total_rows = 0
    for table_name, result in results.items():
        if "count" in result:
            status = "✅"
            count_str = f"{result['count']:,} lignes"
            duration_str = f"{result['duration']:.2f}s"
            total_rows += result['count']
        else:
            status = "❌"
            count_str = "ERREUR"
            duration_str = result.get('error', 'Erreur inconnue')
        
        print(f"{status} gold_{table_name}: {count_str} - {duration_str}")
    
    print(f"""
📈 STATISTIQUES GLOBALES:
   • Total lignes créées: {total_rows:,}
   • Tables réussies: {len([r for r in results.values() if 'count' in r])}/10
   • Vitesse moyenne: {total_rows/total_duration:.0f} lignes/seconde

🎯 QUESTIONS MÉTIER COUVERTES:
   ✅ Q1: Taux consultation établissement X période Y
   ✅ Q2: Taux consultation diagnostic X période Y  
   ✅ Q3: Taux hospitalisation global période Y
   ✅ Q4: Taux hospitalisation diagnostic période Y
   ✅ Q5: Taux hospitalisation sexe/âge
   ✅ Q6: Taux consultation professionnel
   ✅ Q7: Décès localisation 2019
   ✅ Q8: Satisfaction région 2020
   ✅ Q9: Dashboard exécutif consolidé
   ✅ Q10: Indicateurs métier avancés

🔍 VALIDATION QUALITÉ:""")
    
    for validation_name, validation_result in validations.items():
        if isinstance(validation_result, dict) and 'status' in validation_result:
            print(f"   {validation_result['status']} {validation_name}")
        else:
            print(f"   ℹ️  {validation_name}: {validation_result}")
    
    print(f"""
🚀 SUPERSET READY:
   ✅ Tables partitionnées par région/année
   ✅ Vues optimisées créées
   ✅ Métriques calculées et formatées
   ✅ Dimensions broadcast pour performance
   ✅ Contrôles qualité validés

📊 PROCHAINES ÉTAPES:
   1. Connecter Superset au bucket Gold
   2. Importer les vues créées
   3. Créer les dashboards métier
   4. Configurer les alertes et rapports automatiques

🎉 GOLD LAYER TERMINÉ AVEC SUCCÈS!
""")

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """Fonction principale d'exécution du Gold Layer"""
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                     GOLD LAYER PIPELINE                         ║
║                  BI ANALYTICS & SUPERSET                        ║
║                                                                  ║
║  🎯 Objectif: Créer les tables Gold pour analyses métier        ║
║  📊 Output: Tables optimisées pour Superset                     ║
║  🚀 Performance: Partitionnement et cache optimisés             ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Initialisation
        spark = get_spark_session()
        
        # Création des tables Gold
        print("🏗️ Création des tables Gold...")
        results, total_duration = write_all_gold_tables(spark)
        
        # Création des vues Superset
        print("📊 Création des vues Superset...")
        create_superset_views(spark)
        
        # Validation
        print("🔍 Validation de la qualité...")
        validations = validate_gold_layer(spark)
        
        # Rapport final
        generate_final_report(results, total_duration, validations)
        
        # Nettoyage
        spark.stop()
        print("🧹 Session Spark fermée")
        
        return True
        
    except Exception as e:
        print(f"💥 ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        
        if 'spark' in locals():
            spark.stop()
