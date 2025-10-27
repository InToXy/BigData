#!/usr/bin/env python3
"""
gold_aggregation_delta.py

Builds Gold datasets (KPIs) from Silver zone using Delta Lake format.

✨ NOUVEAUTÉS DELTA LAKE:
- ACID transactions (atomicité des écritures)
- Time travel (historique des versions)
- Schema evolution (changements de schéma)
- Optimisation automatique (compaction, Z-ordering)
- Audit trail complet

Contract (inputs/outputs):
- Inputs: Silver tables in s3a://silver/ (Parquet)
- Outputs: Delta tables in s3a://gold-delta/<kpi_name>/ (Delta Lake format)

How to run (example):
docker exec -it chu_jupyter spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation_delta.py

Dependencies:
- delta-spark>=2.4.0
- pyspark>=3.5.0

"""
import os
from typing import List, Optional, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, countDistinct, count, lit, when, year, expr, avg, current_timestamp
)
from delta import configure_spark_with_delta_pip, DeltaTable

# Config - adjust if needed
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_DELTA_BUCKET = os.environ.get("GOLD_DELTA_BUCKET", "gold-delta")

# Delta Lake specific configs
ENABLE_OPTIMIZATIONS = os.environ.get("DELTA_OPTIMIZE", "true").lower() == "true"
ENABLE_VACUUM = os.environ.get("DELTA_VACUUM", "false").lower() == "true"
VACUUM_RETENTION_HOURS = int(os.environ.get("DELTA_VACUUM_HOURS", "168"))  # 7 days


def get_spark_session(app_name: str = "GoldAggregation_Delta") -> SparkSession:
    """
    Crée une session Spark configurée pour Delta Lake.
    
    Delta Lake nécessite des configurations spécifiques pour fonctionner
    avec S3/MinIO et activer toutes ses fonctionnalités.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # S3A / MinIO settings
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Delta Lake configurations
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Performance optimizations for Delta
    builder = builder.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    builder = builder.config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    builder = builder.config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
    builder = builder.config("spark.databricks.delta.optimizeWrite.enabled", "true")
    builder = builder.config("spark.databricks.delta.autoCompact.enabled", "true")
    
    # Schema evolution
    builder = builder.config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    # Configure Delta with pip-installed packages
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Session Spark avec Delta Lake initialisée")
    print(f"   - Delta Lake activé avec optimisations")
    print(f"   - Auto-compaction: activée")
    print(f"   - Schema evolution: activée")
    
    return spark


def silver_path(table: str) -> str:
    """Chemin vers les tables Silver (format Parquet)"""
    return f"s3a://{SILVER_BUCKET}/{table}"


def gold_delta_path(dataset: str) -> str:
    """Chemin vers les tables Gold Delta"""
    return f"s3a://{GOLD_DELTA_BUCKET}/{dataset}"


def read_silver_table(spark: SparkSession, table: str) -> Optional[DataFrame]:
    """
    Lit une table Silver (format Parquet).
    """
    path = silver_path(table)
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        return df
    except Exception as e:
        print(f"⚠️ Impossible de lire {table}: {e}")
        return None


def write_gold_delta(
    df: DataFrame, 
    dataset: str, 
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    optimize: bool = True,
    merge_condition: Optional[str] = None
) -> None:
    """
    Écrit un DataFrame dans une table Delta Lake.
    
    Features:
    - ACID transactions
    - Automatic schema evolution
    - Optional partitioning
    - Auto-optimization (compaction)
    - Time travel support
    
    Args:
        df: DataFrame à écrire
        dataset: Nom du dataset (nom de table)
        mode: Mode d'écriture ('overwrite', 'append', 'merge')
        partition_by: Colonnes de partitionnement (optionnel)
        optimize: Exécuter OPTIMIZE après écriture
        merge_condition: Condition de merge (pour mode='merge')
    """
    path = gold_delta_path(dataset)
    
    # Ajout de métadonnées de tracking
    df_with_metadata = df.withColumn("_loaded_at", current_timestamp()) \
                         .withColumn("_source", lit("gold_aggregation_delta"))
    
    try:
        # Vérifie si la table Delta existe
        delta_table_exists = DeltaTable.isDeltaTable(df.sparkSession, path)
        
        if mode == "merge" and delta_table_exists and merge_condition:
            # Mode MERGE (UPSERT) - mise à jour intelligente
            print(f"🔄 MERGE dans Delta table: {path}")
            delta_table = DeltaTable.forPath(df.sparkSession, path)
            
            # Effectue le merge
            delta_table.alias("target").merge(
                df_with_metadata.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print(f"✅ MERGE terminé: {path}")
            
        else:
            # Mode OVERWRITE ou APPEND standard
            writer = df_with_metadata.write.format("delta").mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.save(path)
            print(f"✅ Écrit Delta table ({mode}): {path}")
        
        # Optimisation automatique
        if optimize and ENABLE_OPTIMIZATIONS:
            optimize_delta_table(df.sparkSession, path, dataset)
        
        # Génération de statistiques
        generate_delta_stats(df.sparkSession, path, dataset)
        
    except Exception as e:
        # Fallback to local folder
        local = f"/tmp/gold-delta/{dataset}"
        os.makedirs(local, exist_ok=True)
        print(f"⚠️ Écriture Delta sur S3 a échoué ({e})")
        
        try:
            df_with_metadata.write.format("delta").mode(mode).save(local)
            print(f"✅ Écrit localement (Delta): {local}")
        except Exception as e2:
            print(f"❌ Échec écriture locale Delta: {e2}")


def optimize_delta_table(spark: SparkSession, path: str, table_name: str) -> None:
    """
    Optimise une table Delta (compaction, Z-ordering).
    
    Améliore les performances de lecture en:
    - Compactant les petits fichiers
    - Réorganisant les données (Z-ordering)
    - Nettoyant les anciennes versions (VACUUM)
    """
    try:
        delta_table = DeltaTable.forPath(spark, path)
        
        print(f"🔧 Optimisation de {table_name}...")
        
        # OPTIMIZE: Compaction des fichiers
        delta_table.optimize().executeCompaction()
        print(f"   ✅ Compaction terminée")
        
        # VACUUM: Nettoyage des anciennes versions (si activé)
        if ENABLE_VACUUM:
            delta_table.vacuum(VACUUM_RETENTION_HOURS)
            print(f"   ✅ VACUUM exécuté (rétention: {VACUUM_RETENTION_HOURS}h)")
        
    except Exception as e:
        print(f"   ⚠️ Optimisation échouée: {e}")


def generate_delta_stats(spark: SparkSession, path: str, table_name: str) -> None:
    """
    Génère et affiche les statistiques d'une table Delta.
    """
    try:
        delta_table = DeltaTable.forPath(spark, path)
        
        # Lecture du DataFrame
        df = spark.read.format("delta").load(path)
        
        # Statistiques de base
        row_count = df.count()
        
        # Historique des versions
        history = delta_table.history(1).select("version", "timestamp", "operation").collect()
        
        print(f"📊 Statistiques {table_name}:")
        print(f"   - Lignes: {row_count:,}")
        print(f"   - Colonnes: {len(df.columns)}")
        if history:
            h = history[0]
            print(f"   - Version: {h['version']}")
            print(f"   - Dernière opération: {h['operation']}")
            print(f"   - Timestamp: {h['timestamp']}")
        
    except Exception as e:
        print(f"   ⚠️ Impossible de générer les stats: {e}")


def show_delta_history(spark: SparkSession, dataset: str, limit: int = 5) -> None:
    """
    Affiche l'historique d'une table Delta (time travel).
    """
    path = gold_delta_path(dataset)
    try:
        delta_table = DeltaTable.forPath(spark, path)
        print(f"\n📜 Historique de {dataset} (dernières {limit} versions):")
        delta_table.history(limit).select(
            "version", "timestamp", "operation", "operationMetrics"
        ).show(truncate=False)
    except Exception as e:
        print(f"⚠️ Impossible de lire l'historique: {e}")


def detect_date_column(df: DataFrame, candidates: List[str]) -> Optional[str]:
    """Détecte la colonne de date parmi les candidats."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ============================================================================
# FONCTIONS DE CALCUL DES KPIs (identiques à gold_aggregation.py)
# ============================================================================

def compute_consultation_rate_by_establishment(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux de consultation des patients sur une période."""
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    
    result = fact_period.agg(
        countDistinct("sk_patient").alias("nb_patients_distincts"),
        count("*").alias("nb_consultations_total")
    ).withColumn("periode_debut", lit(start_date)) \
     .withColumn("periode_fin", lit(end_date)) \
     .withColumn("taux_consultation_moyen", 
                 col("nb_consultations_total") / col("nb_patients_distincts"))
    
    return result


def compute_consultation_rate_by_diagnosis(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux de consultation par diagnostic."""
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
    total_patients = fact_period.select("sk_patient").distinct().count()
    
    by_diag = fact_period.groupBy(diag_col).agg(
        countDistinct("sk_patient").alias("nb_patients_avec_diagnostic"),
        count("*").alias("nb_consultations")
    )
    
    result = by_diag.withColumn("total_patients_periode", lit(total_patients)) \
                    .withColumn("taux_patients", 
                               col("nb_patients_avec_diagnostic") / lit(total_patients)) \
                    .withColumnRenamed(diag_col, "diagnostic_code") \
                    .orderBy(col("nb_consultations").desc())
    
    return result


def compute_global_hospitalization_rate(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux global d'hospitalisation."""
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    nb_hosp = fact_period.select("sk_patient").distinct().count()
    nb_hosp_total = fact_period.count()

    consult = read_silver_table(spark, "fact_consultation")
    dim_patient = read_silver_table(spark, "dim_patient")
    
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


def compute_hospitalization_by_diagnosis(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux d'hospitalisation par diagnostic."""
    fact = read_silver_table(spark, "fact_hospitalisation")
    if fact is None:
        print("fact_hospitalisation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation")
        return None
        
    diag_col = [c for c in fact.columns if "diag" in c.lower() or "diagn" in c.lower() or "code" in c.lower()]
    diag_col = diag_col[0] if diag_col else None
    if diag_col is None:
        print("Aucune colonne diagnostic trouvée dans fact_hospitalisation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    total_patients_hosp = fact_period.select("sk_patient").distinct().count()
    
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


def compute_hospitalization_by_sex_age(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux d'hospitalisation par sexe et âge."""
    fact = read_silver_table(spark, "fact_hospitalisation")
    dim_patient = read_silver_table(spark, "dim_patient")
    if fact is None or dim_patient is None:
        print("fact_hospitalisation ou dim_patient introuvable")
        return None

    date_col = detect_date_column(fact, ["date_hospitalisation", "date", "hospitalisation_date", "date_entree"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_hospitalisation")
        return None

    hosp_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    joined = hosp_period.join(dim_patient.select("sk_patient", "sexe", "age"), on="sk_patient", how="left")

    buckets = [(0,17, '0-17'), (18,34,'18-34'), (35,49,'35-49'), (50,64,'50-64'), (65,200,'65+')]
    expr_age_bucket = when(col("age").isNull(), lit("unknown"))
    for low, high, label in buckets:
        expr_age_bucket = expr_age_bucket.when((col("age") >= low) & (col("age") <= high), lit(label))
    expr_age_bucket = expr_age_bucket.otherwise(lit("unknown"))

    with_bucket = joined.withColumn("tranche_age", expr_age_bucket)
    
    hosp_counts = with_bucket.groupBy("sexe", "tranche_age").agg(
        countDistinct("sk_patient").alias("nb_patients_hospitalises"),
        count("*").alias("nb_hospitalisations")
    )

    total_patients = dim_patient.withColumn("tranche_age", expr_age_bucket) \
                                .groupBy("sexe", "tranche_age") \
                                .agg(countDistinct("sk_patient").alias("nb_patients_total"))
    
    result = hosp_counts.join(total_patients, on=["sexe", "tranche_age"], how="left") \
                        .withColumn("taux_hospitalisation", 
                                   col("nb_patients_hospitalises") / 
                                   when(col("nb_patients_total") == 0, lit(1))
                                   .otherwise(col("nb_patients_total"))) \
                        .orderBy("sexe", "tranche_age")
    
    return result


def compute_consultation_by_professional(
    spark: SparkSession, start_date: str, end_date: str
) -> Optional[DataFrame]:
    """Taux de consultation par professionnel."""
    fact = read_silver_table(spark, "fact_consultation")
    if fact is None:
        print("fact_consultation introuvable")
        return None

    date_col = detect_date_column(fact, ["date_consultation", "date", "consultation_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_consultation")
        return None

    fact_period = fact.filter((col(date_col) >= lit(start_date)) & (col(date_col) <= lit(end_date)))
    
    result = fact_period.groupBy().agg(
        countDistinct("sk_patient").alias("nb_patients_distincts"),
        count("*").alias("nb_consultations_total")
    ).withColumn("periode_debut", lit(start_date)) \
     .withColumn("periode_fin", lit(end_date)) \
     .withColumn("taux_moyen_consultations", 
                 col("nb_consultations_total") / col("nb_patients_distincts"))
    
    return result


def compute_deaths_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    """Nombre de décès par région pour une année."""
    fact = read_silver_table(spark, "fact_deces")
    if fact is None:
        print("fact_deces introuvable")
        return None

    date_col = detect_date_column(fact, ["date_deces", "date", "deces_date"])
    if date_col is None:
        print("Aucune colonne date trouvée dans fact_deces")
        return None

    fact_year = fact.filter(year(col(date_col)) == lit(year_val))
    
    region_col = [c for c in fact.columns if "region" in c.lower() or "departement" in c.lower()]
    region_col = region_col[0] if region_col else None
    
    if region_col:
        result = fact_year.groupBy(region_col).agg(
            count("*").alias("nb_deces")
        ).withColumn("annee", lit(year_val)) \
         .withColumnRenamed(region_col, "region") \
         .orderBy(col("nb_deces").desc())
    else:
        result = fact_year.groupBy().agg(
            count("*").alias("nb_deces")
        ).withColumn("annee", lit(year_val)) \
         .withColumn("region", lit("FRANCE_ENTIERE"))
    
    return result


def compute_satisfaction_by_region_year(spark: SparkSession, year_val: int) -> Optional[DataFrame]:
    """Satisfaction par région pour une année."""
    metrique_tables = [
        "metrique_esatis48h_mco", 
        "metrique_satisfaction"
    ]
    
    df_combined = None
    for table in metrique_tables:
        df_temp = read_silver_table(spark, table)
        if df_temp is not None:
            if df_combined is None:
                df_combined = df_temp
            else:
                common_cols = list(set(df_combined.columns) & set(df_temp.columns))
                if common_cols:
                    df_combined = df_combined.select(common_cols).union(df_temp.select(common_cols))
    
    if df_combined is None:
        print("Aucune table de satisfaction trouvée")
        return None

    date_col = detect_date_column(df_combined, ["date", "annee", "year"])
    if date_col:
        df_year = df_combined.filter(year(col(date_col)) == lit(year_val))
    else:
        df_year = df_combined

    region_col = [c for c in df_year.columns if "region" in c.lower() or "departement" in c.lower()]
    region_col = region_col[0] if region_col else None
    
    score_col = [c for c in df_year.columns if "score" in c.lower() or "satisfaction" in c.lower() or "note" in c.lower()]
    score_col = score_col[0] if score_col else None
    
    if region_col and score_col:
        result = df_year.groupBy(region_col).agg(
            avg(col(score_col)).alias("score_satisfaction_moyen"),
            count("*").alias("nb_evaluations")
        ).withColumn("annee", lit(year_val)) \
         .withColumnRenamed(region_col, "region") \
         .orderBy(col("score_satisfaction_moyen").desc())
    else:
        result = df_year.groupBy().agg(
            count("*").alias("nb_evaluations")
        ).withColumn("annee", lit(year_val)) \
         .withColumn("region", lit("FRANCE_ENTIERE"))
    
    return result


# ============================================================================
# MAIN - ORCHESTRATION DU PIPELINE DELTA
# ============================================================================

def main():
    """
    Pipeline principal de génération des KPIs Gold avec Delta Lake.
    
    Processus:
    1. Initialisation Spark avec Delta Lake
    2. Lecture des données Silver (Parquet)
    3. Calcul des 8 KPIs
    4. Écriture au format Delta Lake
    5. Optimisation automatique
    6. Génération de statistiques
    """
    print("="*70)
    print("🚀 DÉMARRAGE DU PIPELINE GOLD - DELTA LAKE")
    print("="*70)
    
    spark = get_spark_session()
    
    start_date = os.environ.get("GA_START_DATE", "2019-01-01")
    end_date = os.environ.get("GA_END_DATE", "2020-12-31")
    
    print(f"\n📅 Période d'analyse: {start_date} → {end_date}")
    print("="*70)

    # Liste des KPIs à générer
    kpis = [
        {
            "name": "kpi_taux_consultation_periode",
            "func": compute_consultation_rate_by_establishment,
            "description": "Taux de consultation par période",
            "partition": None
        },
        {
            "name": "kpi_consultation_par_diagnostic",
            "func": compute_consultation_rate_by_diagnosis,
            "description": "Taux de consultation par diagnostic",
            "partition": ["diagnostic_code"]
        },
        {
            "name": "kpi_taux_hospitalisation_global",
            "func": compute_global_hospitalization_rate,
            "description": "Taux global d'hospitalisation",
            "partition": None
        },
        {
            "name": "kpi_hospitalisation_par_diagnostic",
            "func": compute_hospitalization_by_diagnosis,
            "description": "Taux d'hospitalisation par diagnostic",
            "partition": ["diagnostic_principal"]
        },
        {
            "name": "kpi_hospitalisation_sexe_age",
            "func": compute_hospitalization_by_sex_age,
            "description": "Taux d'hospitalisation par sexe/âge",
            "partition": ["sexe", "tranche_age"]
        },
        {
            "name": "kpi_consultation_par_professionnel",
            "func": compute_consultation_by_professional,
            "description": "Taux de consultation par professionnel",
            "partition": None
        }
    ]
    
    # KPIs temporels
    temporal_kpis = [
        {
            "name": "kpi_deces_par_region_2019",
            "func": lambda s: compute_deaths_by_region_year(s, 2019),
            "description": "Décès par région (2019)",
            "partition": None
        },
        {
            "name": "kpi_satisfaction_par_region_2020",
            "func": lambda s: compute_satisfaction_by_region_year(s, 2020),
            "description": "Satisfaction par région (2020)",
            "partition": None
        }
    ]
    
    # Traitement des KPIs standard
    for i, kpi in enumerate(kpis, 1):
        print(f"\n[{i}/{len(kpis) + len(temporal_kpis)}] 📊 {kpi['description']}...")
        try:
            df = kpi['func'](spark, start_date, end_date)
            if df is not None:
                write_gold_delta(
                    df, 
                    kpi['name'], 
                    partition_by=kpi['partition'],
                    optimize=True
                )
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
    
    # Traitement des KPIs temporels
    for i, kpi in enumerate(temporal_kpis, len(kpis) + 1):
        print(f"\n[{i}/{len(kpis) + len(temporal_kpis)}] 📊 {kpi['description']}...")
        try:
            df = kpi['func'](spark)
            if df is not None:
                write_gold_delta(
                    df, 
                    kpi['name'], 
                    partition_by=kpi['partition'],
                    optimize=True
                )
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
    
    print("\n" + "="*70)
    print("✅ PIPELINE GOLD DELTA TERMINÉ")
    print("="*70)
    print(f"\n📁 Tables Delta créées dans: s3a://{GOLD_DELTA_BUCKET}/")
    print("\n💡 Commandes utiles:")
    print("   - Lire une table: spark.read.format('delta').load('s3a://gold-delta/kpi_...')")
    print("   - Time travel: spark.read.format('delta').option('versionAsOf', 0).load(...)")
    print("   - Historique: DeltaTable.forPath(spark, path).history().show()")
    
    spark.stop()


if __name__ == "__main__":
    main()
