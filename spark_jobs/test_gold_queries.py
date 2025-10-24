#!/usr/bin/env python3
"""
test_gold_queries.py

Suite de tests de performance et requêtes analytiques sur la zone Gold.
Génère des métriques de performance et des résultats pour le rapport.
"""
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min

# Config MinIO/S3A
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")


def get_spark_session(app_name: str = "TestGoldQueries") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Optimisations pour performance
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def time_query(func):
    """Décorateur pour mesurer le temps d'exécution d'une requête."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        return result, duration
    return wrapper


def print_section(title):
    """Affiche un titre de section formaté."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def print_results(df, query_name, duration, max_rows=10):
    """Affiche les résultats d'une requête avec métriques."""
    print(f"\n📊 {query_name}")
    print(f"⏱️  Temps d'exécution: {duration:.3f} secondes")
    print(f"📝 Résultats:")
    df.show(max_rows, truncate=False)
    print(f"💾 Nombre total de lignes: {df.count()}")


# ============================================================================
# 1. REQUÊTES ANALYTIQUES KPI
# ============================================================================

@time_query
def query_top_diagnostics_hospitalisation(spark):
    """Top 10 des diagnostics conduisant à des hospitalisations."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    result = df.orderBy(col("nb_hospitalisations").desc()).limit(10)
    return result


@time_query
def query_taux_hospitalisation_par_sexe(spark):
    """Comparaison des taux d'hospitalisation entre hommes et femmes."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    result = df.groupBy("sexe").agg(
        spark_sum("nb_patients_hospitalises").alias("total_patients_hospitalises"),
        spark_sum("nb_hospitalisations").alias("total_hospitalisations"),
        avg("taux_hospitalisation").alias("taux_moyen")
    )
    return result


@time_query
def query_taux_hospitalisation_par_age(spark):
    """Distribution des hospitalisations par tranche d'âge."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    result = df.groupBy("tranche_age").agg(
        spark_sum("nb_patients_hospitalises").alias("total_patients"),
        spark_sum("nb_hospitalisations").alias("total_hospitalisations"),
        avg("taux_hospitalisation").alias("taux_moyen")
    ).orderBy("tranche_age")
    return result


@time_query
def query_statistiques_deces_2019(spark):
    """Statistiques globales sur les décès en 2019."""
    df = spark.read.parquet("s3a://gold/kpi_deces_par_region_2019")
    result = df.agg(
        spark_sum("nb_deces").alias("total_deces"),
        spark_sum("nb_patients_decedes").alias("total_patients_decedes"),
        count("*").alias("nb_regions")
    )
    return result


@time_query
def query_kpi_global_hospitalisation(spark):
    """KPI global d'hospitalisation."""
    df = spark.read.parquet("s3a://gold/kpi_taux_hospitalisation_global")
    return df


# ============================================================================
# 2. REQUÊTES DE COMPARAISON TEMPORELLE
# ============================================================================

@time_query
def query_evolution_diagnostics_top5(spark):
    """Évolution des 5 diagnostics les plus fréquents."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    
    # Simuler une comparaison temporelle (en l'absence de données multi-périodes)
    top5 = df.orderBy(col("nb_hospitalisations").desc()).limit(5)
    
    result = top5.select(
        "diagnostic_principal",
        "nb_hospitalisations",
        "nb_patients_hospitalises",
        "taux_hospitalisation"
    ).withColumn("periode", col("total_patients_periode"))
    
    return result


@time_query
def query_tendance_hospitalisation_age(spark):
    """Tendance d'hospitalisation par tranche d'âge."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    
    result = df.select(
        "tranche_age",
        "sexe",
        "nb_hospitalisations",
        "taux_hospitalisation"
    ).orderBy("tranche_age", "sexe")
    
    return result


@time_query
def query_comparaison_periodes_consultation(spark):
    """Comparaison des consultations sur différentes périodes."""
    df = spark.read.parquet("s3a://gold/kpi_taux_consultation_periode")
    
    result = df.select(
        "periode_debut",
        "periode_fin",
        "nb_patients_distincts",
        "nb_consultations_total",
        "taux_consultation_moyen"
    )
    
    return result


# ============================================================================
# 3. REQUÊTES DE PERFORMANCE TECHNIQUE
# ============================================================================

@time_query
def query_scan_complet_diagnostics(spark):
    """Scan complet de la table des diagnostics (test I/O)."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    result = df.select("*")
    # Force l'évaluation
    count_val = result.count()
    return result


@time_query
def query_aggregation_complexe(spark):
    """Agrégation complexe multi-niveaux."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    
    result = df.groupBy("sexe").agg(
        count("*").alias("nb_tranches_age"),
        spark_sum("nb_hospitalisations").alias("total_hospitalisations"),
        avg("taux_hospitalisation").alias("taux_moyen"),
        spark_max("taux_hospitalisation").alias("taux_max"),
        spark_min("taux_hospitalisation").alias("taux_min")
    )
    
    return result


@time_query
def query_jointure_kpis(spark):
    """Jointure entre plusieurs KPIs (test de jointure)."""
    hosp_diag = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    hosp_sexe_age = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    
    # Agrégation pour obtenir des cardinalités compatibles
    hosp_diag_agg = hosp_diag.agg(
        spark_sum("nb_hospitalisations").alias("total_hosp_diag")
    ).withColumn("key", col("total_hosp_diag") * 0 + 1)
    
    hosp_sexe_age_agg = hosp_sexe_age.agg(
        spark_sum("nb_hospitalisations").alias("total_hosp_sexe_age")
    ).withColumn("key", col("total_hosp_sexe_age") * 0 + 1)
    
    result = hosp_diag_agg.join(hosp_sexe_age_agg, on="key")
    
    return result


@time_query
def query_cache_test(spark):
    """Test de l'impact du cache Spark."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    
    # Cache la DataFrame
    df.cache()
    
    # Première passe (charge en cache)
    count1 = df.count()
    
    # Deuxième passe (depuis le cache)
    count2 = df.count()
    
    result = df.agg(
        spark_sum("nb_hospitalisations").alias("total")
    )
    
    return result


@time_query
def query_filter_performance(spark):
    """Test de performance des filtres."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    
    # Filtre sur taux > seuil
    result = df.filter(col("taux_hospitalisation") > 0.001) \
               .orderBy(col("nb_hospitalisations").desc())
    
    return result


# ============================================================================
# 4. REQUÊTES AVANCÉES POUR DATA SCIENCE
# ============================================================================

@time_query
def query_feature_engineering_hospitalisation(spark):
    """Préparation de features pour modèle prédictif d'hospitalisation."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    
    # Création de features
    result = df.select(
        "sexe",
        "tranche_age",
        "nb_hospitalisations",
        "nb_patients_hospitalises",
        "taux_hospitalisation",
        (col("nb_hospitalisations") / col("nb_patients_hospitalises")).alias("ratio_rehospitalisation"),
        (col("taux_hospitalisation") * 100).alias("taux_pourcent"),
        # Encodage sexe
        (col("sexe") == "M").cast("int").alias("sexe_masculin")
    )
    
    return result


@time_query
def query_clustering_diagnostics(spark):
    """Préparation données pour clustering des diagnostics."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    
    # Normalisation et features pour clustering
    max_hosp = df.agg(spark_max("nb_hospitalisations")).collect()[0][0]
    max_patients = df.agg(spark_max("nb_patients_hospitalises")).collect()[0][0]
    
    result = df.select(
        "diagnostic_principal",
        (col("nb_hospitalisations") / max_hosp).alias("nb_hosp_normalized"),
        (col("nb_patients_hospitalises") / max_patients).alias("nb_patients_normalized"),
        "taux_hospitalisation"
    ).filter(col("nb_hospitalisations") > 1)  # Filtrer diagnostics rares
    
    return result


@time_query
def query_correlation_sexe_age(spark):
    """Analyse de corrélation entre sexe, âge et hospitalisation."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
    
    # Pivot pour analyse de corrélation
    result = df.select(
        "sexe",
        "tranche_age",
        "taux_hospitalisation",
        "nb_hospitalisations"
    )
    
    # Statistiques par groupe
    stats = df.groupBy("sexe", "tranche_age").agg(
        avg("taux_hospitalisation").alias("taux_moyen"),
        spark_sum("nb_hospitalisations").alias("total_hosp")
    )
    
    return stats


@time_query
def query_outlier_detection(spark):
    """Détection d'outliers dans les taux d'hospitalisation."""
    df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
    
    # Calcul de statistiques pour détecter outliers
    stats = df.agg(
        avg("taux_hospitalisation").alias("moyenne"),
        spark_max("taux_hospitalisation").alias("max"),
        spark_min("taux_hospitalisation").alias("min")
    )
    
    moyenne = stats.collect()[0]["moyenne"]
    
    # Diagnostics avec taux anormalement élevés (> 2x moyenne)
    result = df.filter(col("taux_hospitalisation") > moyenne * 2) \
               .select("diagnostic_principal", "nb_hospitalisations", "taux_hospitalisation") \
               .orderBy(col("taux_hospitalisation").desc())
    
    return result


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    spark = get_spark_session()
    
    print("\n" + "="*80)
    print("🚀 SUITE DE TESTS DE PERFORMANCE - ZONE GOLD")
    print("="*80)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚙️  Spark version: {spark.version}")
    print("="*80)
    
    # Stocker les résultats de performance
    performance_results = []
    
    # ========================================================================
    # 1. REQUÊTES ANALYTIQUES KPI
    # ========================================================================
    print_section("1️⃣  REQUÊTES ANALYTIQUES KPI")
    
    result, duration = query_top_diagnostics_hospitalisation(spark)
    print_results(result, "Top 10 diagnostics d'hospitalisation", duration)
    performance_results.append(("KPI - Top diagnostics", duration))
    
    result, duration = query_taux_hospitalisation_par_sexe(spark)
    print_results(result, "Taux d'hospitalisation par sexe", duration)
    performance_results.append(("KPI - Taux par sexe", duration))
    
    result, duration = query_taux_hospitalisation_par_age(spark)
    print_results(result, "Taux d'hospitalisation par âge", duration)
    performance_results.append(("KPI - Taux par âge", duration))
    
    result, duration = query_statistiques_deces_2019(spark)
    print_results(result, "Statistiques décès 2019", duration)
    performance_results.append(("KPI - Statistiques décès", duration))
    
    result, duration = query_kpi_global_hospitalisation(spark)
    print_results(result, "KPI global d'hospitalisation", duration)
    performance_results.append(("KPI - Global hospitalisation", duration))
    
    # ========================================================================
    # 2. REQUÊTES DE COMPARAISON TEMPORELLE
    # ========================================================================
    print_section("2️⃣  REQUÊTES DE COMPARAISON TEMPORELLE")
    
    result, duration = query_evolution_diagnostics_top5(spark)
    print_results(result, "Évolution Top 5 diagnostics", duration)
    performance_results.append(("Temporel - Évolution diagnostics", duration))
    
    result, duration = query_tendance_hospitalisation_age(spark)
    print_results(result, "Tendance par âge", duration)
    performance_results.append(("Temporel - Tendance âge", duration))
    
    result, duration = query_comparaison_periodes_consultation(spark)
    print_results(result, "Comparaison périodes consultation", duration)
    performance_results.append(("Temporel - Comparaison périodes", duration))
    
    # ========================================================================
    # 3. REQUÊTES DE PERFORMANCE TECHNIQUE
    # ========================================================================
    print_section("3️⃣  REQUÊTES DE PERFORMANCE TECHNIQUE")
    
    result, duration = query_scan_complet_diagnostics(spark)
    print_results(result, "Scan complet table diagnostics", duration, max_rows=5)
    performance_results.append(("Perf - Scan complet", duration))
    
    result, duration = query_aggregation_complexe(spark)
    print_results(result, "Agrégation complexe multi-niveaux", duration)
    performance_results.append(("Perf - Agrégation complexe", duration))
    
    result, duration = query_jointure_kpis(spark)
    print_results(result, "Jointure entre KPIs", duration)
    performance_results.append(("Perf - Jointure KPIs", duration))
    
    result, duration = query_cache_test(spark)
    print_results(result, "Test cache Spark", duration)
    performance_results.append(("Perf - Cache test", duration))
    
    result, duration = query_filter_performance(spark)
    print_results(result, "Performance filtres", duration, max_rows=5)
    performance_results.append(("Perf - Filtres", duration))
    
    # ========================================================================
    # 4. REQUÊTES AVANCÉES POUR DATA SCIENCE
    # ========================================================================
    print_section("4️⃣  REQUÊTES AVANCÉES POUR DATA SCIENCE")
    
    result, duration = query_feature_engineering_hospitalisation(spark)
    print_results(result, "Feature engineering - Hospitalisation", duration)
    performance_results.append(("ML - Feature engineering", duration))
    
    result, duration = query_clustering_diagnostics(spark)
    print_results(result, "Préparation clustering diagnostics", duration, max_rows=5)
    performance_results.append(("ML - Clustering prep", duration))
    
    result, duration = query_correlation_sexe_age(spark)
    print_results(result, "Analyse corrélation sexe/âge", duration)
    performance_results.append(("ML - Corrélation", duration))
    
    result, duration = query_outlier_detection(spark)
    print_results(result, "Détection outliers", duration, max_rows=5)
    performance_results.append(("ML - Outlier detection", duration))
    
    # ========================================================================
    # RÉSUMÉ DES PERFORMANCES
    # ========================================================================
    print_section("📊 RÉSUMÉ DES PERFORMANCES")
    
    print(f"\n{'Catégorie de requête':<45} {'Temps (s)':>12}")
    print("-"*58)
    
    total_time = 0
    for query_name, duration in performance_results:
        print(f"{query_name:<45} {duration:>12.3f}")
        total_time += duration
    
    print("-"*58)
    print(f"{'TEMPS TOTAL':<45} {total_time:>12.3f}")
    print(f"{'TEMPS MOYEN PAR REQUÊTE':<45} {total_time/len(performance_results):>12.3f}")
    
    # Statistiques
    durations = [d for _, d in performance_results]
    fastest = min(durations)
    slowest = max(durations)
    
    print(f"\n📈 Statistiques:")
    print(f"  ⚡ Requête la plus rapide : {fastest:.3f} s")
    print(f"  🐌 Requête la plus lente  : {slowest:.3f} s")
    print(f"  📊 Ratio lent/rapide      : {slowest/fastest:.2f}x")
    
    print("\n" + "="*80)
    print("✅ TESTS DE PERFORMANCE TERMINÉS")
    print("="*80 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
