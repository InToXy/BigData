"""
Utilitaire d'initialisation du Hive Metastore
Crée automatiquement les schémas bronze, silver et gold via Spark SQL
"""

import os
from typing import List
from pyspark.sql import SparkSession

# Configuration
SCHEMAS = {
    "bronze": {
        "bucket": "bronze",
        "description": "Raw data ingestion layer"
    },
    "silver": {
        "bucket": "silver",
        "description": "Cleaned and validated data layer"
    },
    "gold": {
        "bucket": "gold",
        "description": "Business-ready data layer / Data Marts"
    },
    "warehouse": {
        "bucket": "warehouse",
        "description": "General purpose warehouse"
    }
}


def create_schema_in_metastore(spark: SparkSession, schema_name: str, bucket: str) -> bool:
    """
    Crée un schéma dans le metastore via Spark SQL

    Args:
        spark: SparkSession active
        schema_name: Nom du schéma à créer
        bucket: Nom du bucket S3/MinIO correspondant

    Returns:
        True si succès, False sinon
    """
    try:
        location = f"s3a://{bucket}/"

        # Créer le schéma s'il n'existe pas
        sql = f"CREATE DATABASE IF NOT EXISTS {schema_name} LOCATION '{location}'"

        print(f"   🔧 Création du schéma '{schema_name}' → {location}")

        spark.sql(sql)

        print(f"   ✅ Schéma '{schema_name}' prêt dans le metastore")
        return True

    except Exception as e:
        print(f"   ❌ Erreur création schéma '{schema_name}': {e}")
        return False


def verify_schema_exists(spark: SparkSession, schema_name: str) -> bool:
    """
    Vérifie qu'un schéma existe dans le metastore

    Args:
        spark: SparkSession active
        schema_name: Nom du schéma à vérifier

    Returns:
        True si le schéma existe, False sinon
    """
    try:
        databases = spark.sql("SHOW DATABASES").collect()
        existing_schemas = [row[0] for row in databases]
        return schema_name in existing_schemas
    except Exception as e:
        print(f"   ⚠️  Erreur vérification schéma '{schema_name}': {e}")
        return False


def initialize_metastore_schemas(spark: SparkSession, schemas_to_create: List[str] = None) -> bool:
    """
    Initialise tous les schémas nécessaires dans le metastore

    Args:
        spark: SparkSession active
        schemas_to_create: Liste des schémas à créer (par défaut: tous)

    Returns:
        True si tous les schémas ont été créés/vérifiés, False sinon
    """
    print("\n" + "="*70)
    print("🗄️  INITIALISATION DU HIVE METASTORE")
    print("="*70)

    if schemas_to_create is None:
        schemas_to_create = list(SCHEMAS.keys())

    all_success = True

    for schema_name in schemas_to_create:
        if schema_name not in SCHEMAS:
            print(f"   ⚠️  Schéma inconnu: {schema_name}")
            continue

        schema_config = SCHEMAS[schema_name]
        success = create_schema_in_metastore(
            spark=spark,
            schema_name=schema_name,
            bucket=schema_config["bucket"]
        )

        if not success:
            all_success = False

    print("\n📊 VÉRIFICATION DES SCHÉMAS...")

    # Vérification finale
    for schema_name in schemas_to_create:
        exists = verify_schema_exists(spark, schema_name)
        status = "✅" if exists else "❌"
        print(f"   {status} {schema_name}")

    print("="*70 + "\n")

    return all_success


def initialize_for_layer(spark: SparkSession, layer: str) -> bool:
    """
    Initialise le schéma pour une couche spécifique (bronze, silver ou gold)

    Args:
        spark: SparkSession active
        layer: Nom de la couche (bronze, silver, gold)

    Returns:
        True si succès, False sinon
    """
    if layer.lower() not in SCHEMAS:
        print(f"❌ Couche invalide: {layer}")
        return False

    print(f"\n🔧 Initialisation du schéma {layer.upper()}...")
    return initialize_metastore_schemas(spark, [layer.lower()])
