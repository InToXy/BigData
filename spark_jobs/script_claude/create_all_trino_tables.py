#!/usr/bin/env python3
"""
Script automatique pour créer toutes les tables Trino à partir des Parquet Gold
================================================================================
- Lit le schéma de chaque fichier Parquet
- Convertit les types Spark vers Trino
- Génère et exécute les CREATE TABLE statements
- Vérifie que les tables sont accessibles

Date: 2025-10-26
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import subprocess

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "gold_bucket": "gold"
}

TRINO_CONTAINER = "chu_trino"

# Tables à exclure (metastore, delta, etc.)
EXCLUDED_FOLDERS = ['metastore', 'delta', '_SUCCESS']

# ============================================================================
# MAPPING DES TYPES SPARK -> TRINO
# ============================================================================

def spark_to_trino_type(spark_type):
    """Convertit un type Spark en type Trino"""
    type_mapping = {
        'StringType': 'VARCHAR',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'DoubleType': 'DOUBLE',
        'FloatType': 'REAL',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'DecimalType': 'DECIMAL',
        'BinaryType': 'VARBINARY',
        'ByteType': 'TINYINT',
        'ShortType': 'SMALLINT'
    }

    spark_type_name = type(spark_type).__name__

    # Gestion des types complexes
    if isinstance(spark_type, DecimalType):
        return f"DECIMAL({spark_type.precision}, {spark_type.scale})"
    elif isinstance(spark_type, ArrayType):
        element_type = spark_to_trino_type(spark_type.elementType)
        return f"ARRAY({element_type})"
    elif isinstance(spark_type, MapType):
        key_type = spark_to_trino_type(spark_type.keyType)
        value_type = spark_to_trino_type(spark_type.valueType)
        return f"MAP({key_type}, {value_type})"

    return type_mapping.get(spark_type_name, 'VARCHAR')

# ============================================================================
# INITIALISATION SPARK
# ============================================================================

def get_spark_session():
    """Crée une session Spark optimisée pour lire les Parquet"""
    print("🔧 Initialisation de Spark...")

    # Charger les JARs locaux
    jars_dir = "/home/jovyan/jars"
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
    jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])

    spark = SparkSession.builder \
        .appName("Trino Table Generator") \
        .config("spark.jars", jars_path) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark initialisé\n")
    return spark

# ============================================================================
# DÉCOUVERTE DES TABLES
# ============================================================================

def discover_tables(spark):
    """Découvre toutes les tables Parquet dans le bucket gold"""
    print("🔍 Recherche des tables Parquet dans gold/...\n")

    try:
        # Utiliser boto3 pour lister les dossiers
        import boto3

        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_CONFIG["endpoint"],
            aws_access_key_id=MINIO_CONFIG["access_key"],
            aws_secret_access_key=MINIO_CONFIG["secret_key"]
        )

        # Lister les "dossiers" (préfixes) dans gold
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=MINIO_CONFIG['gold_bucket'], Delimiter='/')

        folders = []
        for page in pages:
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    folder = prefix['Prefix'].rstrip('/')
                    if folder not in EXCLUDED_FOLDERS:
                        # Vérifier qu'il y a des fichiers Parquet
                        response = s3_client.list_objects_v2(
                            Bucket=MINIO_CONFIG['gold_bucket'],
                            Prefix=folder + '/',
                            MaxKeys=10
                        )
                        if 'Contents' in response:
                            has_parquet = any(obj['Key'].endswith('.parquet') for obj in response['Contents'])
                            if has_parquet:
                                folders.append(folder)

        print(f"✅ {len(folders)} tables trouvées:\n")
        for i, table in enumerate(folders, 1):
            print(f"   {i:2d}. {table}")
        print()

        return folders

    except Exception as e:
        print(f"❌ Erreur lors de la découverte des tables: {e}")
        return []

# ============================================================================
# GÉNÉRATION DES SCHEMAS
# ============================================================================

def get_parquet_schema(spark, table_name):
    """Lit le schéma d'une table Parquet"""
    try:
        path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"
        df = spark.read.parquet(path)
        return df.schema, df.count()
    except Exception as e:
        print(f"   ⚠️  Erreur lecture {table_name}: {str(e)[:100]}")
        return None, 0

def generate_create_table_sql(table_name, schema):
    """Génère le statement CREATE TABLE pour Trino"""
    columns = []

    for field in schema.fields:
        col_name = field.name
        col_type = spark_to_trino_type(field.dataType)
        columns.append(f"    {col_name} {col_type}")

    columns_sql = ",\n".join(columns)

    sql = f"""CREATE TABLE IF NOT EXISTS parquet.gold.{table_name} (
{columns_sql}
)
WITH (
    external_location = 's3a://{MINIO_CONFIG['gold_bucket']}/{table_name}',
    format = 'PARQUET'
);"""

    return sql

# ============================================================================
# PAS D'EXÉCUTION DIRECTE - GÉNÉRATION SQL SEULEMENT
# ============================================================================
# Note: Docker n'est pas disponible depuis Jupyter, donc on génère juste le SQL

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Fonction principale"""

    print("=" * 80)
    print("  🚀 CRÉATION AUTOMATIQUE DES TABLES TRINO")
    print("=" * 80)
    print()

    # Initialisation
    spark = get_spark_session()

    # Découverte des tables
    tables = discover_tables(spark)

    if not tables:
        print("⚠️  Aucune table trouvée dans gold/")
        spark.stop()
        return

    # Traitement de chaque table
    print("=" * 80)
    print("  📊 GÉNÉRATION DES TABLES")
    print("=" * 80)
    print()

    results = []
    sql_script = f"""-- Script de création des tables Trino
-- Généré automatiquement le {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Catalogue: parquet.gold
--
-- EXÉCUTION:
--   Depuis votre machine hôte (pas depuis Jupyter):
--   docker cp create_all_gold_tables.sql chu_trino:/tmp/
--   docker exec chu_trino bash -c "trino < /tmp/create_all_gold_tables.sql"

"""

    for i, table_name in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Table: {table_name}")
        print("-" * 80)

        # Lire le schéma
        print(f"   📖 Lecture du schéma Parquet...")
        schema, row_count = get_parquet_schema(spark, table_name)

        if schema is None:
            results.append({
                'table': table_name,
                'status': 'error',
                'message': 'Impossible de lire le schéma',
                'rows': 0
            })
            print(f"   ❌ Échec\n")
            continue

        print(f"   ✓ Schéma lu: {len(schema.fields)} colonnes, {row_count:,} lignes")

        # Générer le SQL
        print(f"   🔨 Génération du CREATE TABLE...")
        sql = generate_create_table_sql(table_name, schema)
        sql_script += f"\n-- {table_name} ({row_count:,} lignes, {len(schema.fields)} colonnes)\n{sql}\n"

        # Afficher un aperçu du schéma
        print(f"   📋 Colonnes:")
        for j, field in enumerate(schema.fields[:5], 1):
            trino_type = spark_to_trino_type(field.dataType)
            print(f"      {j}. {field.name}: {trino_type}")
        if len(schema.fields) > 5:
            print(f"      ... et {len(schema.fields) - 5} autres colonnes")

        print(f"   ✅ SQL généré")
        results.append({
            'table': table_name,
            'status': 'generated',
            'rows': row_count,
            'columns': len(schema.fields)
        })

        print()

    # Sauvegarder le script SQL complet
    output_file = "/home/jovyan/jobs/tmp/create_all_gold_tables.sql"

    # Créer le dossier tmp s'il n'existe pas
    import os
    os.makedirs("/home/jovyan/jobs/tmp", exist_ok=True)

    with open(output_file, 'w') as f:
        f.write(sql_script)

    # Résumé
    print("=" * 80)
    print("  📊 RÉSUMÉ")
    print("=" * 80)
    print()

    generated_count = sum(1 for r in results if r['status'] == 'generated')
    error_count = sum(1 for r in results if r['status'] == 'error')
    total_rows = sum(r.get('rows', 0) for r in results if r['status'] == 'generated')

    print(f"✅ Schémas générés: {generated_count}/{len(tables)}")
    print(f"❌ Erreurs: {error_count}/{len(tables)}")
    print(f"📊 Total de lignes: {total_rows:,}")
    print()

    # Détails par table
    print(f"{'Table':<35} {'Status':<12} {'Lignes':<15} {'Colonnes':<10}")
    print("-" * 80)

    for result in results:
        status_icon = {
            'generated': '✅',
            'error': '❌'
        }.get(result['status'], '?')

        table = result['table'][:33]
        status = result['status']
        rows = f"{result.get('rows', 0):,}" if result.get('rows') else 'N/A'
        cols = str(result.get('columns', 'N/A'))

        print(f"{table:<35} {status_icon} {status:<10} {rows:<15} {cols:<10}")

        if result['status'] == 'error' and 'message' in result:
            print(f"   ↳ {result['message'][:70]}")

    print()
    print("=" * 80)
    print("  📄 FICHIER GÉNÉRÉ")
    print("=" * 80)
    print()
    print(f"✅ Script SQL complet: {output_file}")
    print()
    print("📥 Téléchargez le fichier depuis Jupyter:")
    print(f"   http://localhost:8888/tree/jobs/tmp")
    print(f"   Puis cliquez sur 'create_all_gold_tables.sql' > Download")
    print()
    print("💾 OU copiez-le depuis votre machine hôte:")
    print(f"   /home/matheo/BigData/spark_jobs/tmp/create_all_gold_tables.sql")
    print()

    # Instructions d'exécution
    print("=" * 80)
    print("  ⚡ EXÉCUTION DU SCRIPT SQL")
    print("=" * 80)
    print()
    print("🖥️  DEPUIS VOTRE MACHINE HÔTE (recommandé):")
    print()
    print("   cd /home/matheo/BigData/spark_jobs/tmp")
    print("   docker cp create_all_gold_tables.sql chu_trino:/tmp/")
    print("   docker exec chu_trino bash -c \"trino < /tmp/create_all_gold_tables.sql\"")
    print()
    print("OU manuellement table par table:")
    print()
    print("   docker exec -it chu_trino trino")
    print("   # Puis coller les CREATE TABLE statements un par un")
    print()

    # Commandes de vérification
    print("=" * 80)
    print("  🔍 VÉRIFICATION APRÈS EXÉCUTION")
    print("=" * 80)
    print()
    print("Une fois le script SQL exécuté, vérifiez:")
    print()
    print("   # Lister toutes les tables")
    print("   docker exec chu_trino trino --execute \"SHOW TABLES FROM parquet.gold;\"")
    print()
    print("   # Compter les lignes d'une table")
    print("   docker exec chu_trino trino --execute \"SELECT COUNT(*) FROM parquet.gold.dim_patient;\"")
    print()

    # Instructions Superset
    if generated_count > 0:
        print("=" * 80)
        print("  🎨 PROCHAINE ÉTAPE: SUPERSET")
        print("=" * 80)
        print()
        print("Après avoir exécuté le script SQL, connectez Superset à Trino:")
        print()
        print("1. Ouvrir http://localhost:8088")
        print("2. Login: admin / admin123 (ou superadmin / SuperAdmin123!)")
        print("3. Settings > Database Connections > + Database")
        print("4. Sélectionner 'Trino'")
        print("5. SQLAlchemy URI: trino://trino@chu_trino:8080/parquet/gold")
        print("6. Test Connection > Connect")
        print()
        print("Vous pourrez ensuite créer des datasets et des visualisations!")
        print()

    print("=" * 80)
    print("  ✅ TERMINÉ!")
    print("=" * 80)
    print()

    # Arrêt de Spark
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
