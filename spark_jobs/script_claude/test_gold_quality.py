#!/usr/bin/env python3
"""
Script de Tests de Qualité pour le Layer Gold
==============================================
Vérifie l'intégrité et la qualité des données dans le modèle dimensionnel

Date: 2025-10-24
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "gold_bucket": "gold"
}

def get_spark_session():
    """Session Spark pour les tests"""
    spark = SparkSession.builder \
        .appName("Gold Quality Tests") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_gold_table(spark, table_name):
    """Lecture des tables Gold"""
    try:
        path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"
        return spark.read.parquet(path)
    except Exception as e:
        print(f"  ❌ Erreur lecture {table_name}: {e}")
        return None

def test_table_existence(spark):
    """Test 1: Vérification de l'existence de toutes les tables"""
    print("\n" + "="*70)
    print("TEST 1: EXISTENCE DES TABLES")
    print("="*70)

    expected_tables = {
        "Dimensions": [
            "dim_temps",
            "dim_patient",
            "dim_diagnostic",
            "dim_etablissement",
            "dim_professionnel",
            "dim_localisation"
        ],
        "Faits": [
            "fact_consultation",
            "fact_hospitalisation",
            "fact_deces"
        ],
        "Data Marts": [
            "mart_performance_etablissement",
            "mart_diagnostic_epidemio",
            "mart_demographie",
            "mart_professionnel",
            "mart_deces_localisation_2019",
            "mart_satisfaction_region_2020"
        ]
    }

    all_passed = True
    for category, tables in expected_tables.items():
        print(f"\n{category}:")
        for table in tables:
            df = read_gold_table(spark, table)
            if df is not None:
                count = df.count()
                print(f"  ✅ {table}: {count:,} lignes")
            else:
                print(f"  ❌ {table}: NON TROUVÉE")
                all_passed = False

    return all_passed

def test_data_quality(spark):
    """Test 2: Qualité des données"""
    print("\n" + "="*70)
    print("TEST 2: QUALITÉ DES DONNÉES")
    print("="*70)

    tests_passed = 0
    tests_total = 0

    # Test 2.1: Dim_Temps doit couvrir 2000-2030
    print("\n2.1 - DIM_TEMPS: Couverture temporelle")
    dim_temps = read_gold_table(spark, "dim_temps")
    if dim_temps:
        min_year = dim_temps.agg(min("annee")).collect()[0][0]
        max_year = dim_temps.agg(max("annee")).collect()[0][0]
        if min_year == 2000 and max_year == 2030:
            print(f"  ✅ Couverture: {min_year}-{max_year}")
            tests_passed += 1
        else:
            print(f"  ❌ Couverture incorrecte: {min_year}-{max_year}")
        tests_total += 1

    # Test 2.2: Fact_Consultation - Clés étrangères non nulles critiques
    print("\n2.2 - FACT_CONSULTATION: Clés étrangères")
    fact_consultation = read_gold_table(spark, "fact_consultation")
    if fact_consultation:
        total = fact_consultation.count()
        null_date = fact_consultation.filter(col("date_consultation_fk").isNull()).count()
        null_patient = fact_consultation.filter(col("patient_fk").isNull()).count()

        if null_date == 0 and null_patient == 0:
            print(f"  ✅ Toutes les clés critiques sont renseignées")
            tests_passed += 1
        else:
            print(f"  ❌ Clés nulles - date: {null_date}, patient: {null_patient}")
        tests_total += 1

    # Test 2.3: Fact_Hospitalisation - Durées de séjour valides
    print("\n2.3 - FACT_HOSPITALISATION: Durées de séjour")
    fact_hosp = read_gold_table(spark, "fact_hospitalisation")
    if fact_hosp:
        total = fact_hosp.count()
        invalid_duree = fact_hosp.filter((col("duree_sejour_jours") < 0) | (col("duree_sejour_jours") > 365)).count()

        if invalid_duree == 0:
            print(f"  ✅ Toutes les durées sont valides (0-365 jours)")
            tests_passed += 1
        else:
            print(f"  ❌ Durées invalides: {invalid_duree}/{total}")
        tests_total += 1

    # Test 2.4: Fact_Deces - Uniquement 2019
    print("\n2.4 - FACT_DECES: Filtrage année 2019")
    fact_deces = read_gold_table(spark, "fact_deces")
    if fact_deces:
        years = fact_deces.select(year("date_deces")).distinct().collect()
        years_list = [row[0] for row in years]

        if years_list == [2019]:
            print(f"  ✅ Uniquement 2019")
            tests_passed += 1
        else:
            print(f"  ❌ Années présentes: {years_list}")
        tests_total += 1

    print(f"\n📊 Résultat: {tests_passed}/{tests_total} tests réussis")
    return tests_passed == tests_total

def test_business_requirements(spark):
    """Test 3: Couverture des exigences métier"""
    print("\n" + "="*70)
    print("TEST 3: COUVERTURE DES EXIGENCES MÉTIER")
    print("="*70)

    tests_passed = 0
    tests_total = 8

    # Exigence 1: Taux consultation par établissement
    print("\n3.1 - Exigence 1: Consultation par établissement")
    mart_perf = read_gold_table(spark, "mart_performance_etablissement")
    if mart_perf:
        required_cols = ["finess_site", "region", "annee", "nb_consultations", "taux_consultation_par_patient"]
        if all(col_name in mart_perf.columns for col_name in required_cols):
            sample = mart_perf.filter(col("nb_consultations") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 2: Taux consultation par diagnostic
    print("\n3.2 - Exigence 2: Consultation par diagnostic")
    mart_diag = read_gold_table(spark, "mart_diagnostic_epidemio")
    if mart_diag:
        required_cols = ["code_diag", "annee", "taux_consultation_diagnostic_pct"]
        if all(col_name in mart_diag.columns for col_name in required_cols):
            sample = mart_diag.filter(col("nb_consultations") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 3: Taux global hospitalisation
    print("\n3.3 - Exigence 3: Taux global hospitalisation")
    if mart_perf:
        required_cols = ["nb_hospitalisations", "taux_hospitalisation_pct"]
        if all(col_name in mart_perf.columns for col_name in required_cols):
            print(f"  ✅ Données disponibles")
            tests_passed += 1
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 4: Taux hospitalisation par diagnostic
    print("\n3.4 - Exigence 4: Hospitalisation par diagnostic")
    if mart_diag:
        required_cols = ["code_diag", "taux_hospitalisation_diagnostic_pct"]
        if all(col_name in mart_diag.columns for col_name in required_cols):
            sample = mart_diag.filter(col("nb_hospitalisations") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 5: Taux hospitalisation par sexe/âge
    print("\n3.5 - Exigence 5: Hospitalisation par démographie")
    mart_demo = read_gold_table(spark, "mart_demographie")
    if mart_demo:
        required_cols = ["sexe", "categorie_age", "nb_hospitalisations"]
        if all(col_name in mart_demo.columns for col_name in required_cols):
            sample = mart_demo.filter(col("nb_hospitalisations") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 6: Taux consultation par professionnel
    print("\n3.6 - Exigence 6: Consultation par professionnel")
    mart_prof = read_gold_table(spark, "mart_professionnel")
    if mart_prof:
        required_cols = ["professionnel_id", "nb_consultations_total", "taux_consultation_par_patient"]
        if all(col_name in mart_prof.columns for col_name in required_cols):
            sample = mart_prof.filter(col("nb_consultations_total") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 7: Décès par localisation 2019
    print("\n3.7 - Exigence 7: Décès par localisation 2019")
    mart_deces = read_gold_table(spark, "mart_deces_localisation_2019")
    if mart_deces:
        required_cols = ["region", "nb_deces_total", "age_moyen_deces"]
        if all(col_name in mart_deces.columns for col_name in required_cols):
            sample = mart_deces.filter(col("nb_deces_total") > 0).limit(1).collect()
            if sample:
                print(f"  ✅ Données disponibles")
                tests_passed += 1
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    # Exigence 8: Satisfaction par région 2020
    print("\n3.8 - Exigence 8: Satisfaction par région 2020")
    mart_sat = read_gold_table(spark, "mart_satisfaction_region_2020")
    if mart_sat:
        required_cols = ["region", "score_satisfaction_moyen", "annee"]
        if all(col_name in mart_sat.columns for col_name in required_cols):
            sample = mart_sat.filter(col("score_satisfaction_moyen") > 0).limit(1).collect()
            if sample:
                year_check = mart_sat.select("annee").distinct().collect()[0][0]
                if year_check == 2020:
                    print(f"  ✅ Données 2020 disponibles")
                    tests_passed += 1
                else:
                    print(f"  ❌ Année incorrecte: {year_check}")
            else:
                print(f"  ❌ Aucune donnée")
        else:
            print(f"  ❌ Colonnes manquantes")

    print(f"\n📊 Résultat: {tests_passed}/{tests_total} exigences couvertes")
    return tests_passed == tests_total

def test_star_schema_integrity(spark):
    """Test 4: Intégrité du modèle en étoile"""
    print("\n" + "="*70)
    print("TEST 4: INTÉGRITÉ DU MODÈLE EN ÉTOILE")
    print("="*70)

    tests_passed = 0
    tests_total = 0

    # Test 4.1: Clés primaires uniques dans les dimensions
    print("\n4.1 - Unicité des clés primaires")

    dimensions = {
        "dim_patient": "patient_sk",
        "dim_diagnostic": "diagnostic_sk",
        "dim_etablissement": "etablissement_sk",
        "dim_professionnel": "professionnel_sk"
    }

    for dim_name, pk_col in dimensions.items():
        dim_df = read_gold_table(spark, dim_name)
        if dim_df and pk_col in dim_df.columns:
            total = dim_df.count()
            distinct = dim_df.select(pk_col).distinct().count()

            if total == distinct:
                print(f"  ✅ {dim_name}.{pk_col}: {total} clés uniques")
                tests_passed += 1
            else:
                print(f"  ❌ {dim_name}.{pk_col}: {distinct}/{total} uniques (doublons!)")
            tests_total += 1

    # Test 4.2: Intégrité référentielle (échantillon)
    print("\n4.2 - Intégrité référentielle (échantillon)")

    fact_consultation = read_gold_table(spark, "fact_consultation")
    dim_patient = read_gold_table(spark, "dim_patient")

    if fact_consultation and dim_patient:
        # Jointure pour vérifier que toutes les clés existent
        orphans = fact_consultation \
            .filter(col("patient_fk").isNotNull()) \
            .join(dim_patient, fact_consultation.patient_fk == dim_patient.patient_sk, "left_anti") \
            .count()

        if orphans == 0:
            print(f"  ✅ Toutes les FK patient_fk ont une correspondance")
            tests_passed += 1
        else:
            print(f"  ❌ {orphans} FK patient_fk orphelines")
        tests_total += 1

    print(f"\n📊 Résultat: {tests_passed}/{tests_total} tests réussis")
    return tests_passed == tests_total

def generate_summary_report(spark):
    """Génère un rapport récapitulatif"""
    print("\n" + "="*70)
    print("RAPPORT RÉCAPITULATIF")
    print("="*70)

    tables = [
        ("dim_temps", "Dimension"),
        ("dim_patient", "Dimension"),
        ("dim_diagnostic", "Dimension"),
        ("dim_etablissement", "Dimension"),
        ("dim_professionnel", "Dimension"),
        ("dim_localisation", "Dimension"),
        ("fact_consultation", "Fait"),
        ("fact_hospitalisation", "Fait"),
        ("fact_deces", "Fait"),
        ("mart_performance_etablissement", "Mart"),
        ("mart_diagnostic_epidemio", "Mart"),
        ("mart_demographie", "Mart"),
        ("mart_professionnel", "Mart"),
        ("mart_deces_localisation_2019", "Mart"),
        ("mart_satisfaction_region_2020", "Mart")
    ]

    print(f"\n{'Table':<40} {'Type':<12} {'Lignes':>15}")
    print("-" * 70)

    total_rows = 0
    for table_name, table_type in tables:
        df = read_gold_table(spark, table_name)
        if df:
            count = df.count()
            total_rows += count
            print(f"{table_name:<40} {table_type:<12} {count:>15,}")

    print("-" * 70)
    print(f"{'TOTAL':<40} {'':<12} {total_rows:>15,}")

def main():
    """Exécution de tous les tests"""
    print("""
╔════════════════════════════════════════════════════════════════════════╗
║              TESTS DE QUALITÉ - GOLD LAYER                            ║
║                   Modèle Dimensionnel en Étoile                       ║
╚════════════════════════════════════════════════════════════════════════╝
    """)

    spark = get_spark_session()

    all_tests_passed = True

    # Test 1: Existence
    if not test_table_existence(spark):
        all_tests_passed = False

    # Test 2: Qualité
    if not test_data_quality(spark):
        all_tests_passed = False

    # Test 3: Exigences métier
    if not test_business_requirements(spark):
        all_tests_passed = False

    # Test 4: Intégrité du modèle
    if not test_star_schema_integrity(spark):
        all_tests_passed = False

    # Rapport récapitulatif
    generate_summary_report(spark)

    # Résultat final
    print("\n" + "="*70)
    if all_tests_passed:
        print("✅ TOUS LES TESTS RÉUSSIS !")
        print("="*70)
        spark.stop()
        return 0
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
        print("="*70)
        spark.stop()
        return 1

if __name__ == "__main__":
    sys.exit(main())
