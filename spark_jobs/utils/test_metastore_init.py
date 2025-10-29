#!/usr/bin/env python3
"""
Script de test pour l'initialisation du Hive Metastore
"""

import os
import sys
from pyspark.sql import SparkSession

# Import du module d'initialisation
from metastore_init import initialize_metastore_schemas, initialize_for_layer, verify_schema_exists

def create_test_spark_session():
    """Crée une SparkSession pour les tests"""
    jars_dir = "/home/jovyan/jars"
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')] if os.path.exists(jars_dir) else []
    jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])

    builder = SparkSession.builder \
        .appName("Test Metastore Init") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

    if jars_path:
        builder = builder.config("spark.jars", jars_path)

    return builder.getOrCreate()


def test_initialize_all_schemas():
    """Test: Initialiser tous les schémas"""
    print("\n" + "="*70)
    print("TEST 1: Initialisation de tous les schémas")
    print("="*70)

    spark = create_test_spark_session()

    try:
        success = initialize_metastore_schemas(spark)
        print(f"\n{'✅ SUCCÈS' if success else '❌ ÉCHEC'}: Initialisation de tous les schémas")
        return success
    finally:
        spark.stop()


def test_initialize_single_schema():
    """Test: Initialiser un seul schéma"""
    print("\n" + "="*70)
    print("TEST 2: Initialisation du schéma bronze uniquement")
    print("="*70)

    spark = create_test_spark_session()

    try:
        success = initialize_for_layer(spark, "bronze")
        print(f"\n{'✅ SUCCÈS' if success else '❌ ÉCHEC'}: Initialisation du schéma bronze")
        return success
    finally:
        spark.stop()


def test_verify_schemas():
    """Test: Vérifier que les schémas existent"""
    print("\n" + "="*70)
    print("TEST 3: Vérification de l'existence des schémas")
    print("="*70)

    spark = create_test_spark_session()

    try:
        schemas_to_check = ["bronze", "silver", "gold", "warehouse"]
        all_exist = True

        for schema in schemas_to_check:
            exists = verify_schema_exists(spark, schema)
            status = "✅" if exists else "❌"
            print(f"   {status} Schéma '{schema}' {'existe' if exists else 'n\\'existe pas'}")

            if not exists:
                all_exist = False

        print(f"\n{'✅ SUCCÈS' if all_exist else '❌ ÉCHEC'}: Tous les schémas existent")
        return all_exist

    finally:
        spark.stop()


def test_show_all_databases():
    """Test: Afficher tous les schémas disponibles"""
    print("\n" + "="*70)
    print("TEST 4: Liste de tous les schémas dans le metastore")
    print("="*70)

    spark = create_test_spark_session()

    try:
        databases = spark.sql("SHOW DATABASES").collect()
        print("\n📊 Schémas disponibles:")
        for db in databases:
            print(f"   - {db[0]}")

        print(f"\n✅ Total: {len(databases)} schémas")
        return True

    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        return False

    finally:
        spark.stop()


def main():
    """Exécute tous les tests"""
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║            TESTS D'INITIALISATION DU HIVE METASTORE                 ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)

    results = []

    # Test 1: Initialiser tous les schémas
    results.append(("Initialisation complète", test_initialize_all_schemas()))

    # Test 2: Initialiser un seul schéma
    results.append(("Initialisation bronze", test_initialize_single_schema()))

    # Test 3: Vérifier les schémas
    results.append(("Vérification", test_verify_schemas()))

    # Test 4: Afficher tous les schémas
    results.append(("Liste des schémas", test_show_all_databases()))

    # Rapport final
    print("\n" + "="*70)
    print("📊 RÉSUMÉ DES TESTS")
    print("="*70)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status} {test_name}")

    all_success = all(result[1] for result in results)

    print("\n" + "="*70)
    if all_success:
        print("🎉 TOUS LES TESTS ONT RÉUSSI !")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
    print("="*70 + "\n")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
