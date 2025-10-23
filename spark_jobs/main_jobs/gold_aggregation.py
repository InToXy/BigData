#!/usr/bin/env python3
"""
gold_aggregation.py

Builds Gold datasets (KPIs) from the Silver zone.

Contract (inputs/outputs):
- Inputs: Silver tables in s3a://silver/ (dimensions and facts). We expect at least:
  - dim_patient, dim_etablissement, fact_consultation, fact_hospitalisation, fact_deces, metrique_*
- Outputs: Parquet files written to s3a://gold/<kpi_name>/ (or local folder if gold bucket not present).

Assumptions and fallbacks:
- Column names may vary. The job tries common variants and falls back gracefully when a column/table is missing.
- Date columns expected as 'date_consultation', 'date_hospitalisation', 'date_deces' or 'date'. If missing, uses available date-like fields.

How to run (example):
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation.py

"""
import os
from typing import List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, countDistinct, count, lit, when, year, expr, avg
)

# Config - adjust if needed
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")


def get_spark_session(app_name: str = "GoldAggregation") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    # S3A / MinIO settings
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def silver_path(table: str) -> str:
    return f"s3a://{SILVER_BUCKET}/{table}"


def gold_path(dataset: str) -> str:
    return f"s3a://{GOLD_BUCKET}/{dataset}"


def read_silver_table(spark: SparkSession, table: str) -> Optional[DataFrame]:
    path = silver_path(table)
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        return df
    except Exception:
        return None


def write_gold(df: DataFrame, dataset: str, mode: str = "overwrite") -> None:
    path = gold_path(dataset)
    try:
        df.write.mode(mode).parquet(path)
        print(f"✅ Écrit Gold dataset: {path}")
    except Exception as e:
        # fallback to local folder
        local = f"/tmp/gold/{dataset}"
        os.makedirs(local, exist_ok=True)
        df.write.mode(mode).parquet(local)
        print(f"⚠️ Écriture Gold sur S3 a échoué ({e}), écrit localement: {local}")


def detect_date_column(df: DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def compute_consultation_rate_by_establishment(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux de consultation des patients dans un établissement X sur une période Y.
    
    Since fact_consultation doesn't have sk_etablissement, we aggregate globally 
    and provide consultation rate metrics that can be filtered by period.
    """
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    # Filter by period
    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    
    # Global metrics (no establishment breakdown possible without the FK)
    result = fact_period.agg(
        countDistinct("sk_patient").alias("nb_patients_distincts"),
        count("*").alias("nb_consultations_total")
    ).withColumn("periode_debut", lit(start_date)) \
     .withColumn("periode_fin", lit(end_date)) \
     .withColumn("taux_consultation_moyen", 
                 col("nb_consultations_total") / col("nb_patients_distincts"))
    
    return result


def compute_consultation_rate_by_diagnosis(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux de consultation de patients par rapport à tous les diagnostics sur une période Y.
    
    Returns a breakdown by diagnosis code with consultation rates.
    """
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    diag_col = [c for c in fact.columns if "diag" in c.lower() or "diagn" in c.lower() or "code" in c.lower()]
    diag_col = diag_col[0] if diag_col else None
    if diag_col is None:
        print("Aucune colonne diagnostic trouvée dans fact_consultation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))

    # Total patients in period
    total_patients = fact_period.select("sk_patient").distinct().count()
    
    # Breakdown by diagnosis
    by_diag = fact_period.groupBy(diag_col).agg(
        countDistinct("sk_patient").alias("nb_patients_avec_diagnostic"),
        count("*").alias("nb_consultations")
    )
    
    # Add totals and calculate rate
    result = by_diag.withColumn("total_patients_periode", lit(total_patients)) \
                    .withColumn("taux_patients", 
                               col("nb_patients_avec_diagnostic") / lit(total_patients)) \
                    .withColumnRenamed(diag_col, "diagnostic_code") \
                    .orderBy(col("nb_consultations").desc())
    
    return result


def compute_global_hospitalization_rate(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux global d'hospitalisation des patients dans une période donnée Y.
    
    Compares hospitalized patients to all patients who had consultations in the same period.
    """
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation - skipping")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    nb_hosp = fact_period.select("sk_patient").distinct().count()
    nb_hosp_total = fact_period.count()

    # Denominator: distinct patients from consultations or all patients
    dim_patient = read_silver_table(spark, "dim_patient")
    consult = read_silver_table(spark, "fact_consultation")
    
    if consult is not None:
        date_c = detect_date_column(consult, ["date_consultation", "date", "consultation_date"])
        if date_c:
            patients_period = consult.filter((col(date_c) >= lit(start_date)) & (col(date_c) <= lit(end_date))).select("sk_patient").distinct().count()
        else:
            patients_period = consult.select("sk_patient").distinct().count()
    elif dim_patient is not None:
        patients_period = dim_patient.select("sk_patient").distinct().count()
    else:
        patients_period = nb_hosp

    taux = nb_hosp / patients_period if patients_period > 0 else 0.0
    
    df = spark.createDataFrame([
        (start_date, end_date, nb_hosp, nb_hosp_total, patients_period, taux)
    ], schema=["periode_debut", "periode_fin", "nb_patients_hospitalises", 
               "nb_hospitalisations_total", "nb_patients_reference", "taux_hospitalisation"])
    
    return df


def compute_hospitalization_by_diagnosis(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux d'hospitalisation des patients par rapport à des diagnostics sur une période donnée.
    
    Shows hospitalization rates broken down by diagnostic code.
    """
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation - skipping")
        return None
        
    diag_col = [c for c in fact.columns if "diag" in c.lower() or "diagn" in c.lower() or "code" in c.lower()]
    diag_col = diag_col[0] if diag_col else None
    if diag_col is None:
        print("Aucune colonne diagnostic trouvée dans fact_hospitalisation - skipping")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    
    # Total patients hospitalized in period (denominator for rate calculation)
    total_patients_hosp = fact_period.select("sk_patient").distinct().count()
    
    # Breakdown by diagnosis
    by_diag = fact_period.groupBy(diag_col).agg(
        countDistinct("sk_patient").alias("nb_patients_hospitalises"),
        count("*").alias("nb_hospitalisations")
    )
    
    result = by_diag.withColumn("total_patients_periode", lit(total_patients_hosp)) \
                    .withColumn("taux_hospitalisation", 
                               col("nb_patients_hospitalises") / lit(total_patients_hosp)) \
                    .withColumnRenamed(diag_col, "diagnostic_principal") \
                    .orderBy(col("nb_hospitalisations").desc())
    
    return result


def compute_hospitalization_by_sex_age(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux d'hospitalisation par sexe et par âge sur une période donnée.
    
    Provides hospitalization rates segmented by gender and age groups.
    """
    fact = read_silver_table(spark, "fact_hospitalisation")
    dim_patient = read_silver_table(spark, "dim_patient")
    if fact is None or dim_patient is None:
        print("fact_hospitalisation ou dim_patient introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation - skipping")
        return None

    hosp_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    joined = hosp_period.join(dim_patient.select("sk_patient", "sexe", "age"), on="sk_patient", how="left")

    # Create age buckets
    buckets = [(0,17, '0-17'), (18,34,'18-34'), (35,49,'35-49'), (50,64,'50-64'), (65,200,'65+')]
    expr_age_bucket = when(col("age").isNull(), lit("unknown"))
    for low, high, label in buckets:
        expr_age_bucket = expr_age_bucket.when((col("age") >= low) & (col("age") <= high), lit(label))
    expr_age_bucket = expr_age_bucket.otherwise(lit("unknown"))

    with_bucket = joined.withColumn("tranche_age", expr_age_bucket)
    
    # Hospitalized patients by sex/age
    hosp_counts = with_bucket.groupBy("sexe", "tranche_age").agg(
        countDistinct("sk_patient").alias("nb_patients_hospitalises"),
        count("*").alias("nb_hospitalisations")
    )

    # Total patients by sex/age in dim_patient (denominator)
    total_patients = dim_patient.withColumn("tranche_age", expr_age_bucket) \
                                .groupBy("sexe", "tranche_age") \
                                .agg(countDistinct("sk_patient").alias("nb_patients_total"))
    
    # Join and calculate rate
    result = hosp_counts.join(total_patients, on=["sexe", "tranche_age"], how="left") \
                        .withColumn("taux_hospitalisation", 
                                   col("nb_patients_hospitalises") / 
                                   when(col("nb_patients_total") == 0, lit(1))
                                   .otherwise(col("nb_patients_total"))) \
                        .orderBy("sexe", "tranche_age")
    
    return result


def compute_consultation_by_professional(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux de consultation par professionnel de santé sur une période.
    
    Since professional column is not available in fact_consultation, 
    we compute consultation metrics without professional breakdown.
    Returns aggregated consultation statistics.
    """
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    # Try to find professional column
    prof_col = [c for c in fact.columns if "prof" in c.lower() or "pract" in c.lower() or "medecin" in c.lower()]
    prof_col = prof_col[0] if prof_col else None
    
    period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    
    if prof_col is None:
        # No professional column - return global metrics
        print("Aucune colonne professionnel - calcul des métriques globales de consultation")
        result = period.agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("sk_patient").alias("nb_patients_distincts")
        ).withColumn("periode_debut", lit(start_date)) \
         .withColumn("periode_fin", lit(end_date)) \
         .withColumn("consultations_par_patient", 
                    col("nb_consultations_total") / col("nb_patients_distincts"))
    else:
        # Professional column exists - breakdown by professional
        result = period.groupBy(prof_col).agg(
            count("*").alias("nb_consultations"), 
            countDistinct("sk_patient").alias("nb_patients_vus")
        ).withColumn("consultations_par_patient", 
                    col("nb_consultations") / 
                    when(col("nb_patients_vus") == 0, lit(1))
                    .otherwise(col("nb_patients_vus"))) \
         .withColumnRenamed(prof_col, "professionnel_id") \
         .orderBy(col("nb_consultations").desc())
    
    return result


def compute_deaths_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    """Nombre de décès par localisation (région) sur l'année spécifiée.
    
    Aggregates death counts by region for the given year.
    """
    fact = read_silver_table(spark, "fact_deces")
    dim_estab = read_silver_table(spark, "dim_etablissement")
    if fact is None:
        print("fact_deces introuvable")
        return None

    date_col = detect_date_column(fact, ["date_deces", "date", "deces_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_deces")
        return None

    fact_year = fact.withColumn("annee", year(col(date_col))).filter(col("annee") == lit(year_val))

    # Detect region column
    region_col = None
    if dim_estab is not None:
        for c in ["region", "region_normalisee", "region_code"]:
            if c in dim_estab.columns:
                region_col = c
                break

    if region_col is None and "region" in fact.columns:
        region_col = "region"

    if region_col is None:
        print("Aucune colonne région trouvée - calcul global uniquement")
        # Fallback: global count
        out = fact_year.agg(
            count("*").alias("nb_deces_total"),
            countDistinct("sk_patient").alias("nb_patients_decedes")
        ).withColumn("annee", lit(year_val)) \
         .withColumn("region", lit("TOTAL"))
        return out

    # Join deaths to establishments if needed
    if "sk_etablissement" in fact_year.columns and dim_estab is not None and "sk_etablissement" in dim_estab.columns:
        joined = fact_year.join(
            dim_estab.select("sk_etablissement", region_col), 
            on="sk_etablissement", 
            how="left"
        )
        out = joined.groupBy(region_col).agg(
            count("*").alias("nb_deces"),
            countDistinct("sk_patient").alias("nb_patients_decedes")
        ).withColumn("annee", lit(year_val)) \
         .withColumnRenamed(region_col, "region") \
         .orderBy("region")
    else:
        # Region in fact table
        out = fact_year.groupBy(region_col).agg(
            count("*").alias("nb_deces"),
            countDistinct("sk_patient").alias("nb_patients_decedes")
        ).withColumn("annee", lit(year_val)) \
         .withColumnRenamed(region_col, "region") \
         .orderBy("region")

    return out


def compute_satisfaction_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    """Taux global de satisfaction par région sur l'année spécifiée.
    
    Attempts to find satisfaction metrics in Silver tables and aggregate by region.
    """
    # Try multiple possible tables for satisfaction data
    tables_to_try = ["metrique_consultation", "metrique_satisfaction", "satisfaction", "metrique_activite_temporelle"]
    sat_df = None
    source_table = None
    
    for t in tables_to_try:
        sat_df = read_silver_table(spark, t)
        if sat_df is not None:
            source_table = t
            print(f"Utilisation de la table {t} pour les données de satisfaction")
            break

    if sat_df is None:
        print("Aucune table de satisfaction trouvée dans Silver - KPI non disponible")
        return None

    # Detect columns
    date_col = detect_date_column(sat_df, ["date", "date_satisfaction", "year", "annee"])
    
    # Look for satisfaction score column
    score_col = None
    for c in sat_df.columns:
        c_lower = c.lower()
        if "satisf" in c_lower or "score" in c_lower or "note" in c_lower or "taux" in c_lower:
            score_col = c
            break

    # Look for region column
    region_col = None
    for c in ["region", "region_normalisee", "region_code", "localisation"]:
        if c in sat_df.columns:
            region_col = c
            break

    # Check we have minimum required columns
    if score_col is None:
        print(f"Aucune colonne satisfaction/score trouvée dans {source_table}")
        return None
    
    if date_col is None:
        print(f"Aucune colonne date trouvée dans {source_table}")
        return None

    # Filter by year
    sat_df = sat_df.withColumn("annee", year(col(date_col)))
    sat_year = sat_df.filter(col("annee") == lit(year_val))

    if region_col is None:
        # No region - return global satisfaction
        print(f"Aucune colonne région trouvée - calcul de satisfaction globale")
        result = sat_year.agg(
            avg(col(score_col)).alias("taux_satisfaction_moyen"),
            count("*").alias("nb_evaluations")
        ).withColumn("annee", lit(year_val)) \
         .withColumn("region", lit("GLOBAL"))
    else:
        # Aggregate by region
        result = sat_year.groupBy(region_col).agg(
            avg(col(score_col)).alias("taux_satisfaction_moyen"),
            count("*").alias("nb_evaluations")
        ).withColumn("annee", lit(year_val)) \
         .withColumnRenamed(region_col, "region") \
         .orderBy("region")

    return result


def main():
    spark = get_spark_session()
    print("✅ Spark session démarrée pour Gold aggregation")
    print("="*70)

    # Configuration de la période d'analyse
    start_date = os.environ.get("GA_START_DATE", "2019-01-01")
    end_date = os.environ.get("GA_END_DATE", "2020-12-31")
    
    print(f"Période d'analyse: {start_date} -> {end_date}")
    print("="*70)

    # 1. Taux de consultation des patients sur une période
    print("\n[1/8] Calcul: Taux de consultation par période...")
    kpi1 = compute_consultation_rate_by_establishment(spark, start_date, end_date)
    if kpi1 is not None:
        write_gold(kpi1, "kpi_taux_consultation_periode")

    # 2. Taux de consultation par diagnostic
    print("\n[2/8] Calcul: Taux de consultation par diagnostic...")
    kpi2 = compute_consultation_rate_by_diagnosis(spark, start_date, end_date)
    if kpi2 is not None:
        write_gold(kpi2, "kpi_consultation_par_diagnostic")

    # 3. Taux global d'hospitalisation
    print("\n[3/8] Calcul: Taux global d'hospitalisation...")
    kpi3 = compute_global_hospitalization_rate(spark, start_date, end_date)
    if kpi3 is not None:
        write_gold(kpi3, "kpi_taux_hospitalisation_global")

    # 4. Taux d'hospitalisation par diagnostic
    print("\n[4/8] Calcul: Taux d'hospitalisation par diagnostic...")
    kpi4 = compute_hospitalization_by_diagnosis(spark, start_date, end_date)
    if kpi4 is not None:
        write_gold(kpi4, "kpi_hospitalisation_par_diagnostic")

    # 5. Taux d'hospitalisation par sexe et âge
    print("\n[5/8] Calcul: Taux d'hospitalisation par sexe/âge...")
    kpi5 = compute_hospitalization_by_sex_age(spark, start_date, end_date)
    if kpi5 is not None:
        write_gold(kpi5, "kpi_hospitalisation_sexe_age")

    # 6. Taux de consultation par professionnel
    print("\n[6/8] Calcul: Taux de consultation par professionnel...")
    kpi6 = compute_consultation_by_professional(spark, start_date, end_date)
    if kpi6 is not None:
        write_gold(kpi6, "kpi_consultation_par_professionnel")

    # 7. Nombre de décès par région - année 2019
    print("\n[7/8] Calcul: Décès par région (2019)...")
    kpi7 = compute_deaths_by_region_year(spark, 2019)
    if kpi7 is not None:
        write_gold(kpi7, "kpi_deces_par_region_2019")

    # 8. Satisfaction par région - année 2020
    print("\n[8/8] Calcul: Satisfaction par région (2020)...")
    kpi8 = compute_satisfaction_by_region_year(spark, 2020)
    if kpi8 is not None:
        write_gold(kpi8, "kpi_satisfaction_par_region_2020")

    spark.stop()
    print("\n" + "="*70)
    print("✅ Gold aggregation terminé - Tous les KPIs ont été calculés")
    print("="*70)


if __name__ == "__main__":
    main()