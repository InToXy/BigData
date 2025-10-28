#!/usr/bin/env python3
"""
gold_aggregation.py
===================
Agrégations Gold - KPIs métiers pour le CHU Data Warehouse

KPIs générés (8 tables Gold):
1. kpi_consultation_rate - Taux de consultation par établissement/période
2. kpi_hospitalisation_metrics - Métriques hospitalisation (durée, taux)
3. kpi_deces_by_region - Décès par région/département
4. kpi_satisfaction_global - Satisfaction patients agrégée
5. kpi_activite_mensuelle - Activité mensuelle tous services
6. kpi_patient_demographics - Démographie patients
7. kpi_etablissement_performance - Performance établissements
8. kpi_temporal_trends - Tendances temporelles

Architecture:
- Input: s3a://silver/* (star schema)
- Output: s3a://gold/* (KPIs agrégés)

Usage:
    docker exec chu_jupyter spark-submit \
      --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
      /home/jovyan/jobs/main_jobs/gold_aggregation.py
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min, max as spark_max,
    when, lit, year, month, quarter, to_date, current_timestamp, round as spark_round
)

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

# ============================================================
# SESSION SPARK
# ============================================================
def get_spark_session():
    """Crée la session Spark avec configuration S3A."""
    try:
        builder = SparkSession.builder \
            .appName("Gold KPI Aggregation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.parquet.compression.codec", "snappy")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Gold session créée")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur création Spark: {e}")
        raise

# ============================================================
# LECTURE/ÉCRITURE
# ============================================================
def read_silver_table(spark, table_name):
    """Lit une table du layer Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df = spark.read.parquet(silver_path)
        count_rows = df.count()
        print(f"✅ Silver '{table_name}' lu: {count_rows:,} lignes")
        return df
    except Exception as e:
        print(f"⚠️  Table Silver '{table_name}' non trouvée: {e}")
        return None

def write_gold_kpi(df, kpi_name):
    """Écrit un KPI dans le layer Gold."""
    try:
        gold_path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{kpi_name}"
        df.write.mode("overwrite").parquet(gold_path)
        count_rows = df.count()
        print(f"✅ Gold KPI '{kpi_name}' écrit: {count_rows:,} lignes")
    except Exception as e:
        print(f"❌ Erreur écriture Gold '{kpi_name}': {e}")
        raise

# ============================================================
# KPI 1: TAUX DE CONSULTATION
# ============================================================
def kpi_consultation_rate(spark):
    """KPI 1: Taux de consultation par période."""
    print("\n🎯 KPI 1: Consultation Rate...")
    
    fact_consultation = read_silver_table(spark, "fact_consultation")
    if fact_consultation is None:
        return
    
    # Agrégation par année/mois
    kpi = fact_consultation \
        .filter(col("date_consultation").isNotNull()) \
        .withColumn("annee", year(col("date_consultation"))) \
        .withColumn("mois", month(col("date_consultation"))) \
        .groupBy("annee", "mois") \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("sk_patient").alias("nb_patients_uniques"),
            spark_sum("montant").alias("montant_total"),
            avg("montant").alias("montant_moyen"),
            avg("duree_minutes").alias("duree_moyenne_minutes")
        ) \
        .withColumn("taux_consultation_patient", 
                    spark_round(col("nb_consultations") / col("nb_patients_uniques"), 2)) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("annee", "mois")
    
    write_gold_kpi(kpi, "kpi_consultation_rate")
    return kpi

# ============================================================
# KPI 2: MÉTRIQUES HOSPITALISATION
# ============================================================
def kpi_hospitalisation_metrics(spark):
    """KPI 2: Métriques d'hospitalisation."""
    print("\n🎯 KPI 2: Hospitalisation Metrics...")
    
    fact_hosp = read_silver_table(spark, "fact_hospitalisation")
    if fact_hosp is None:
        return
    
    # Agrégation par année
    kpi = fact_hosp \
        .filter(col("date_entree").isNotNull()) \
        .withColumn("annee", year(col("date_entree"))) \
        .groupBy("annee") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("sk_patient").alias("nb_patients_hospitalises"),
            avg("duree_sejour_jours").alias("duree_moyenne_sejour"),
            spark_min("duree_sejour_jours").alias("duree_min_sejour"),
            spark_max("duree_sejour_jours").alias("duree_max_sejour")
        ) \
        .withColumn("taux_hospit_patient", 
                    spark_round(col("nb_hospitalisations") / col("nb_patients_hospitalises"), 2)) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("annee")
    
    write_gold_kpi(kpi, "kpi_hospitalisation_metrics")
    return kpi

# ============================================================
# KPI 3: DÉCÈS PAR RÉGION
# ============================================================
def kpi_deces_by_region(spark):
    """KPI 3: Décès par région et démographie."""
    print("\n🎯 KPI 3: Décès by Region...")
    
    fact_deces = read_silver_table(spark, "fact_deces")
    if fact_deces is None:
        return
    
    # Agrégation par lieu et sexe
    kpi = fact_deces \
        .filter(col("date_deces").isNotNull()) \
        .withColumn("annee", year(col("date_deces"))) \
        .groupBy("annee", "lieu_deces", "sexe") \
        .agg(
            count("*").alias("nb_deces"),
            avg("age").alias("age_moyen_deces"),
            spark_min("age").alias("age_min_deces"),
            spark_max("age").alias("age_max_deces")
        ) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("annee", col("nb_deces").desc())
    
    write_gold_kpi(kpi, "kpi_deces_by_region")
    return kpi

# ============================================================
# KPI 4: SATISFACTION GLOBALE
# ============================================================
def kpi_satisfaction_global(spark):
    """KPI 4: Satisfaction patients agrégée."""
    print("\n🎯 KPI 4: Satisfaction Global...")
    
    metrique_sat = read_silver_table(spark, "metrique_satisfaction")
    if metrique_sat is None:
        return
    
    # Agrégation par source d'enquête
    kpi = metrique_sat \
        .groupBy("source_enquete") \
        .agg(
            count("*").alias("nb_reponses_enquete")
        ) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy(col("nb_reponses_enquete").desc())
    
    write_gold_kpi(kpi, "kpi_satisfaction_global")
    return kpi

# ============================================================
# KPI 5: ACTIVITÉ MENSUELLE
# ============================================================
def kpi_activite_mensuelle(spark):
    """KPI 5: Activité mensuelle tous services."""
    print("\n🎯 KPI 5: Activité Mensuelle...")
    
    fact_consultation = read_silver_table(spark, "fact_consultation")
    fact_hosp = read_silver_table(spark, "fact_hospitalisation")
    
    if fact_consultation is None and fact_hosp is None:
        return
    
    # Consultations mensuelles
    consult_monthly = None
    if fact_consultation is not None:
        consult_monthly = fact_consultation \
            .filter(col("date_consultation").isNotNull()) \
            .withColumn("annee", year(col("date_consultation"))) \
            .withColumn("mois", month(col("date_consultation"))) \
            .groupBy("annee", "mois") \
            .agg(count("*").alias("nb_consultations"))
    
    # Hospitalisations mensuelles
    hosp_monthly = None
    if fact_hosp is not None:
        hosp_monthly = fact_hosp \
            .filter(col("date_entree").isNotNull()) \
            .withColumn("annee", year(col("date_entree"))) \
            .withColumn("mois", month(col("date_entree"))) \
            .groupBy("annee", "mois") \
            .agg(count("*").alias("nb_hospitalisations"))
    
    # Fusion
    if consult_monthly is not None and hosp_monthly is not None:
        kpi = consult_monthly.join(hosp_monthly, ["annee", "mois"], "outer")
    elif consult_monthly is not None:
        kpi = consult_monthly.withColumn("nb_hospitalisations", lit(0))
    else:
        kpi = hosp_monthly.withColumn("nb_consultations", lit(0))
    
    kpi = kpi \
        .fillna(0) \
        .withColumn("activite_totale", col("nb_consultations") + col("nb_hospitalisations")) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("annee", "mois")
    
    write_gold_kpi(kpi, "kpi_activite_mensuelle")
    return kpi

# ============================================================
# KPI 6: DÉMOGRAPHIE PATIENTS
# ============================================================
def kpi_patient_demographics(spark):
    """KPI 6: Démographie des patients."""
    print("\n🎯 KPI 6: Patient Demographics...")
    
    dim_patient = read_silver_table(spark, "dim_patient")
    if dim_patient is None:
        return
    
    # Agrégation par tranche d'âge et sexe
    kpi = dim_patient \
        .filter(col("is_active") == 1) \
        .groupBy("tranche_age", "sexe") \
        .agg(
            count("*").alias("nb_patients")
        ) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("tranche_age", "sexe")
    
    write_gold_kpi(kpi, "kpi_patient_demographics")
    return kpi

# ============================================================
# KPI 7: PERFORMANCE ÉTABLISSEMENTS
# ============================================================
def kpi_etablissement_performance(spark):
    """KPI 7: Performance des établissements."""
    print("\n🎯 KPI 7: Établissement Performance...")
    
    dim_etab = read_silver_table(spark, "dim_etablissement")
    if dim_etab is None:
        return
    
    # Statistiques par type et région
    kpi = dim_etab \
        .filter(col("is_active") == 1) \
        .groupBy("region", "type_etablissement") \
        .agg(
            count("*").alias("nb_etablissements")
        ) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("region", col("nb_etablissements").desc())
    
    write_gold_kpi(kpi, "kpi_etablissement_performance")
    return kpi

# ============================================================
# KPI 8: TENDANCES TEMPORELLES
# ============================================================
def kpi_temporal_trends(spark):
    """KPI 8: Tendances temporelles globales."""
    print("\n🎯 KPI 8: Temporal Trends...")
    
    fact_consultation = read_silver_table(spark, "fact_consultation")
    fact_hosp = read_silver_table(spark, "fact_hospitalisation")
    fact_deces = read_silver_table(spark, "fact_deces")
    
    # Agrégation consultations
    consult_agg = None
    if fact_consultation is not None:
        consult_agg = fact_consultation \
            .filter(col("date_consultation").isNotNull()) \
            .groupBy(
                year(col("date_consultation")).alias("annee"),
                quarter(col("date_consultation")).alias("trimestre")
            ) \
            .agg(count("*").alias("nb_consultations"))
    
    # Agrégation hospitalisations
    hosp_agg = None
    if fact_hosp is not None:
        hosp_agg = fact_hosp \
            .filter(col("date_entree").isNotNull()) \
            .groupBy(
                year(col("date_entree")).alias("annee"),
                quarter(col("date_entree")).alias("trimestre")
            ) \
            .agg(count("*").alias("nb_hospitalisations"))
    
    # Agrégation décès
    deces_agg = None
    if fact_deces is not None:
        deces_agg = fact_deces \
            .filter(col("date_deces").isNotNull()) \
            .groupBy(
                year(col("date_deces")).alias("annee"),
                quarter(col("date_deces")).alias("trimestre")
            ) \
            .agg(count("*").alias("nb_deces"))
    
    # Joindre toutes les agrégations
    if consult_agg is not None:
        kpi = consult_agg
        if hosp_agg is not None:
            kpi = kpi.join(hosp_agg, ["annee", "trimestre"], "full")
        if deces_agg is not None:
            kpi = kpi.join(deces_agg, ["annee", "trimestre"], "full")
    elif hosp_agg is not None:
        kpi = hosp_agg
        if deces_agg is not None:
            kpi = kpi.join(deces_agg, ["annee", "trimestre"], "full")
    elif deces_agg is not None:
        kpi = deces_agg
    else:
        return None
    
    # Remplir les valeurs nulles et calculer le total
    kpi = kpi \
        .fillna(0, subset=["nb_consultations", "nb_hospitalisations", "nb_deces"]) \
        .withColumn("activite_totale", 
                   col("nb_consultations") + col("nb_hospitalisations") + col("nb_deces")) \
        .withColumn("calcul_date", current_timestamp()) \
        .orderBy("annee", "trimestre")
    
    write_gold_kpi(kpi, "kpi_temporal_trends")
    return kpi

# ============================================================
# MAIN
# ============================================================
def main():
    """Pipeline complet Gold."""
    print("=" * 70)
    print("🏆 AGRÉGATION GOLD - KPIs CHU DATA WAREHOUSE")
    print("=" * 70)
    print(f"📅 Démarrage: {datetime.now()}")
    print()
    
    spark = get_spark_session()
    
    try:
        print("\n" + "=" * 70)
        print("🎯 GÉNÉRATION DES 8 KPIs")
        print("=" * 70)
        
        kpi_consultation_rate(spark)
        kpi_hospitalisation_metrics(spark)
        kpi_deces_by_region(spark)
        kpi_satisfaction_global(spark)
        kpi_activite_mensuelle(spark)
        kpi_patient_demographics(spark)
        kpi_etablissement_performance(spark)
        kpi_temporal_trends(spark)
        
        # RÉSUMÉ
        print("\n" + "=" * 70)
        print("✅ AGRÉGATION GOLD TERMINÉE")
        print("=" * 70)
        print(f"📅 Fin: {datetime.now()}")
        print()
        print("KPIs créés (8 tables):")
        print("  1. kpi_consultation_rate - Taux consultations")
        print("  2. kpi_hospitalisation_metrics - Métriques hospitalisation")
        print("  3. kpi_deces_by_region - Décès par région")
        print("  4. kpi_satisfaction_global - Satisfaction agrégée")
        print("  5. kpi_activite_mensuelle - Activité mensuelle")
        print("  6. kpi_patient_demographics - Démographie patients")
        print("  7. kpi_etablissement_performance - Performance établissements")
        print("  8. kpi_temporal_trends - Tendances temporelles")
        print()
        print("➡️  Prochaine étape: Visualisation Superset")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
