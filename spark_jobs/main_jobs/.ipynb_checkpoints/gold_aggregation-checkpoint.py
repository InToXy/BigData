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

    Produces a dataset with columns: sk_etablissement, nb_patients_consulted, nb_patients_total, rate
    Assumptions: fact_consultation contains sk_patient, sk_etablissement, date_consultation (or date)
    dim_patient may contain sk_patient with sk_etablissement association.
    """
    fact = read_silver_table(spark, "fact_consultation")
    dim_patient = read_silver_table(spark, "dim_patient")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    # filter by period
    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))

    # Numerator: distinct patients per establishment in period
    num_df = fact_period.groupBy("sk_etablissement").agg(countDistinct("sk_patient").alias("nb_patients_consulted"))

    # Denominator: total patients per establishment from dim_patient if available, else distinct patients in fact overall
    if dim_patient is not None and "sk_etablissement" in dim_patient.columns and "sk_patient" in dim_patient.columns:
        denom_df = dim_patient.groupBy("sk_etablissement").agg(countDistinct("sk_patient").alias("nb_patients_total"))
    else:
        denom_df = fact.groupBy("sk_etablissement").agg(countDistinct("sk_patient").alias("nb_patients_total"))

    result = num_df.join(denom_df, on="sk_etablissement", how="left")
    result = result.withColumn("rate", (col("nb_patients_consulted") / col("nb_patients_total")))
    return result


def compute_consultation_rate_by_diagnosis(spark: SparkSession, diagnosis_code: str, start_date: str, end_date: str) -> Optional[DataFrame]:
    """Taux de consultation de patients par rapport à un diagnostic X sur une période Y."""
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

    # patients with diagnosis
    with_diag = fact_period.filter(col(diag_col) == lit(diagnosis_code)).select("sk_patient").distinct()
    nb_with_diag = with_diag.count()

    total_patients = fact_period.select("sk_patient").distinct().count()

    # assemble result as single-row DataFrame
    df = spark.createDataFrame([(diagnosis_code, nb_with_diag, total_patients, nb_with_diag / (total_patients if total_patients else 1.0))],
                               schema=["diagnosis_code", "nb_patients_with_diag", "nb_patients_total", "rate"])
    return df


def compute_global_hospitalization_rate(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    nb_hosp = fact_period.select("sk_patient").distinct().count()

    # denominator: distinct patients in same period from consultations or patients
    consult = read_silver_table(spark, "fact_consultation")
    if consult is not None:
        date_c = detect_date_column(consult, ["date_consultation", "date", "consultation_date"])
        if date_c:
            patients_period = consult.filter((col(date_c) >= lit(start_date)) & (col(date_c) <= lit(end_date))).select("sk_patient").distinct().count()
        else:
            patients_period = consult.select("sk_patient").distinct().count()
    else:
        patients_period = fact.select("sk_patient").distinct().count()

    df = spark.createDataFrame([("global", nb_hosp, patients_period, nb_hosp / (patients_period if patients_period else 1.0))],
                               schema=["metric", "nb_hospitalized", "nb_patients", "rate"])
    return df


def compute_hospitalization_by_diagnosis(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date"])
    diag_col = [c for c in fact.columns if "diag" in c.lower() or "diagn" in c.lower() or "code" in c.lower()]
    diag_col = diag_col[0] if diag_col else None
    if diag_col is None:
        print("Aucune colonne diagnostic trouvée dans fact_hospitalisation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    by_diag = fact_period.groupBy(diag_col).agg(countDistinct("sk_patient").alias("nb_hospitalized"))

    # total patients hospitalized in period
    total = fact_period.select("sk_patient").distinct().count()
    by_diag = by_diag.withColumn("nb_patients_total", lit(total)).withColumn("rate", col("nb_hospitalized") / when(col("nb_patients_total") == 0, lit(1)).otherwise(col("nb_patients_total")))
    return by_diag


def compute_hospitalization_by_sex_age(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    fact = read_silver_table(spark, "fact_hospitalisation")
    dim_patient = read_silver_table(spark, "dim_patient")
    if fact is None or dim_patient is None:
        print("fact_hospitalisation ou dim_patient introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation")
        return None

    hosp_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    joined = hosp_period.join(dim_patient.select("sk_patient", "sexe", "age"), on="sk_patient", how="left")

    # Create age buckets
    buckets = [(0,17, '0-17'), (18,34,'18-34'), (35,49,'35-49'), (50,64,'50-64'), (65,200,'65+')]
    expr_age_bucket = when(col("age").isNull(), lit("unknown"))
    for low, high, label in buckets:
        expr_age_bucket = expr_age_bucket.when((col("age") >= low) & (col("age") <= high), lit(label))
    expr_age_bucket = expr_age_bucket.otherwise(lit("unknown"))

    with_bucket = joined.withColumn("age_bucket", expr_age_bucket)
    result = with_bucket.groupBy("sexe", "age_bucket").agg(countDistinct("sk_patient").alias("nb_hospitalized"))

    # denominator: total patients by sexe/age in dim_patient
    denom = dim_patient.withColumn("age_bucket", expr_age_bucket).groupBy("sexe", "age_bucket").agg(countDistinct("sk_patient").alias("nb_patients"))
    out = result.join(denom, on=["sexe","age_bucket"], how="left").withColumn("rate", col("nb_hospitalized")/when(col("nb_patients")==0, lit(1)).otherwise(col("nb_patients")))
    return out


def compute_consultation_by_professional(spark: SparkSession, start_date: str, end_date: str) -> Optional[DataFrame]:
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    prof_col = [c for c in fact.columns if "prof" in c.lower() or "pract" in c.lower() or "pro" in c.lower()]
    prof_col = prof_col[0] if prof_col else None
    if prof_col is None:
        print("Aucune colonne professionnel trouvée dans fact_consultation")
        return None

    period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    by_prof = period.groupBy(prof_col).agg(count("*").alias("nb_consultations"), countDistinct("sk_patient").alias("nb_patients_seen"))
    by_prof = by_prof.withColumn("consult_per_patient", col("nb_consultations")/when(col("nb_patients_seen")==0, lit(1)).otherwise(col("nb_patients_seen")))
    return by_prof


def compute_deaths_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    fact = read_silver_table(spark, "fact_deces")
    dim_estab = read_silver_table(spark, "dim_etablissement")
    if fact is None:
        print("fact_deces introuvable")
        return None

    date_col = detect_date_column(fact, ["date_deces", "date", "deces_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_deces")
        return None

    fact_year = fact.withColumn("year", year(col(date_col))).filter(col("year") == lit(year_val))

    # region column detection
    region_col = None
    if dim_estab is not None:
        for c in ["region", "region_normalisee", "region_code"]:
            if c in dim_estab.columns:
                region_col = c
                break

    if region_col is None and "region" in fact.columns:
        region_col = "region"

    if region_col is None:
        print("Aucune colonne région trouvée (fact_deces ou dim_etablissement)")
        # fallback: count by nothing
        out = fact_year.groupBy().agg(count("*").alias("nb_deces"))
        return out

    # join deaths to establishments if needed
    if "sk_etablissement" in fact_year.columns and dim_estab is not None and "sk_etablissement" in dim_estab.columns:
        joined = fact_year.join(dim_estab.select("sk_etablissement", region_col), on="sk_etablissement", how="left")
        out = joined.groupBy(region_col).agg(count("*").alias("nb_deces")).orderBy(region_col)
    else:
        out = fact_year.groupBy(region_col).agg(count("*").alias("nb_deces")).orderBy(region_col)

    return out


def compute_satisfaction_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    # try to find a satisfaction table or metric
    tables_to_try = ["metrique_consultation", "metrique_satisfaction", "satisfaction", "metrique_activite_temporelle"]
    sat_df = None
    for t in tables_to_try:
        sat_df = read_silver_table(spark, t)
        if sat_df is not None:
            break

    if sat_df is None:
        print("Aucune table de satisfaction trouvée dans Silver")
        return None

    # detect date and region and score columns
    date_col = detect_date_column(sat_df, ["date", "date_satisfaction", "year", "annee"]) or ("date" if "date" in sat_df.columns else None)
    score_col = None
    for c in sat_df.columns:
        if "satisf" in c.lower() or "score" in c.lower() or "note" in c.lower():
            score_col = c
            break

    region_col = None
    for c in ["region", "region_normalisee", "region_code"]:
        if c in sat_df.columns:
            region_col = c
            break

    if score_col is None or region_col is None or date_col is None:
        print("Impossible de détecter les colonnes score/region/date pour la satisfaction")
        return None

    sat_df = sat_df.withColumn("year", year(col(date_col)))
    sat_year = sat_df.filter(col("year") == lit(year_val))

    out = sat_year.groupBy(region_col).agg(avg(col(score_col)).alias("avg_satisfaction"), count("*").alias("nb_records")).orderBy(region_col)
    return out


def main():
    spark = get_spark_session()
    print("✅ Spark session démarrée pour Gold aggregation")

    # Example period, in practice accept params or env vars
    start_date = os.environ.get("GA_START_DATE", "2019-01-01")
    end_date = os.environ.get("GA_END_DATE", "2020-12-31")

    # 1. consultation rate by establishment
    c_by_est = compute_consultation_rate_by_establishment(spark, start_date, end_date)
    if c_by_est is not None:
        write_gold(c_by_est, "consultation_rate_by_establishment")

    # 2. consultation rate by diagnosis (example: pass diagnosis via env)
    diag = os.environ.get("GA_DIAGNOSIS_CODE", "I10")
    c_by_diag = compute_consultation_rate_by_diagnosis(spark, diag, start_date, end_date)
    if c_by_diag is not None:
        write_gold(c_by_diag, f"consultation_rate_diag_{diag}")

    # 3. global hospitalization rate
    gh = compute_global_hospitalization_rate(spark, start_date, end_date)
    if gh is not None:
        write_gold(gh, "global_hospitalization_rate")

    # 4. hospitalization by diagnosis
    hosp_diag = compute_hospitalization_by_diagnosis(spark, start_date, end_date)
    if hosp_diag is not None:
        write_gold(hosp_diag, "hospitalization_by_diagnosis")

    # 5. hospitalization by sex/age
    hosp_sex_age = compute_hospitalization_by_sex_age(spark, start_date, end_date)
    if hosp_sex_age is not None:
        write_gold(hosp_sex_age, "hospitalization_by_sex_age")

    # 6. consultation by professional
    cons_prof = compute_consultation_by_professional(spark, start_date, end_date)
    if cons_prof is not None:
        write_gold(cons_prof, "consultation_by_professional")

    # 7. deaths by region and year 2019
    deaths_2019 = compute_deaths_by_region_year(spark, 2019)
    if deaths_2019 is not None:
        write_gold(deaths_2019, "deaths_by_region_2019")

    # 8. satisfaction by region year 2020
    sat_2020 = compute_satisfaction_by_region_year(spark, 2020)
    if sat_2020 is not None:
        write_gold(sat_2020, "satisfaction_by_region_2020")

    spark.stop()
    print("✅ Gold aggregation terminé")


if __name__ == "__main__":
    main()

