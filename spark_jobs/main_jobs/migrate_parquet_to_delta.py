#!/usr/bin/env python3
"""
migrate_parquet_to_delta.py

Migre les tables Gold existantes du format Parquet vers Delta Lake.

✨ Fonctionnalités:
- Migration automatique de toutes les tables Gold
- Préservation des données et du schéma
- Historique initial avec métadonnées
- Validation post-migration
- Rollback possible via time travel

How to run:
docker exec -it chu_jupyter spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/migrate_parquet_to_delta.py

"""
import os
from typing import List, Tuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip, DeltaTable

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")
GOLD_PARQUET_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
GOLD_DELTA_BUCKET = os.environ.get("GOLD_DELTA_BUCKET", "gold-delta")

# Tables à migrer
TABLES_TO_MIGRATE = [
    "kpi_taux_consultation_periode",
    "kpi_consultation_par_diagnostic",
    "kpi_taux_hospitalisation_global",
    "kpi_hospitalisation_par_diagnostic",
    "kpi_hospitalisation_sexe_age",
    "kpi_consultation_par_professionnel",
    "kpi_deces_par_region_2019",
    "kpi_satisfaction_par_region_2020"
]


def get_spark_session() -> SparkSession:
    """Crée une session Spark avec Delta Lake."""
    builder = SparkSession.builder.appName("Parquet_to_Delta_Migration")
    
    # S3A / MinIO
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Delta Lake
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    builder = builder.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def migrate_table(
    spark: SparkSession, 
    table_name: str,
    dry_run: bool = False
) -> Tuple[bool, str]:
    """
    Migre une table Parquet vers Delta.
    
    Returns:
        (success: bool, message: str)
    """
    parquet_path = f"s3a://{GOLD_PARQUET_BUCKET}/{table_name}"
    delta_path = f"s3a://{GOLD_DELTA_BUCKET}/{table_name}"
    
    try:
        # 1. Vérifier si la table Parquet existe
        print(f"\n{'='*70}")
        print(f"📦 Migration: {table_name}")
        print(f"{'='*70}")
        print(f"  Source (Parquet): {parquet_path}")
        print(f"  Destination (Delta): {delta_path}")
        
        try:
            df = spark.read.parquet(parquet_path)
        except Exception as e:
            return False, f"❌ Table Parquet introuvable: {e}"
        
        # 2. Statistiques source
        row_count = df.count()
        col_count = len(df.columns)
        print(f"\n  📊 Données source:")
        print(f"     - Lignes: {row_count:,}")
        print(f"     - Colonnes: {col_count}")
        print(f"     - Schéma: {', '.join(df.columns)}")
        
        if dry_run:
            return True, "✅ Validation OK (dry-run mode)"
        
        # 3. Ajouter métadonnées de migration
        df_with_meta = df.withColumn("_migrated_at", current_timestamp()) \
                         .withColumn("_migration_source", lit("parquet")) \
                         .withColumn("_original_path", lit(parquet_path))
        
        # 4. Écrire en Delta Lake
        print(f"\n  🔄 Écriture Delta Lake en cours...")
        df_with_meta.write.format("delta").mode("overwrite").save(delta_path)
        
        # 5. Vérification post-migration
        df_delta = spark.read.format("delta").load(delta_path)
        delta_row_count = df_delta.count()
        
        if delta_row_count != row_count:
            return False, f"❌ Nombre de lignes différent: {row_count} → {delta_row_count}"
        
        # 6. Optimisation initiale
        print(f"  🔧 Optimisation Delta...")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.optimize().executeCompaction()
        
        # 7. Afficher l'historique
        history = delta_table.history(1).collect()
        if history:
            version = history[0]["version"]
            operation = history[0]["operation"]
            print(f"\n  ✅ Migration réussie!")
            print(f"     - Version Delta: {version}")
            print(f"     - Opération: {operation}")
            print(f"     - Lignes migrées: {delta_row_count:,}")
        
        return True, f"✅ Migration réussie: {row_count:,} lignes"
        
    except Exception as e:
        return False, f"❌ Erreur migration: {str(e)}"


def validate_migration(spark: SparkSession, table_name: str) -> bool:
    """
    Valide qu'une migration s'est bien déroulée.
    
    Vérifie:
    - Table Delta existe
    - Nombre de lignes correct
    - Schéma préservé
    """
    parquet_path = f"s3a://{GOLD_PARQUET_BUCKET}/{table_name}"
    delta_path = f"s3a://{GOLD_DELTA_BUCKET}/{table_name}"
    
    try:
        # Lire les deux versions
        df_parquet = spark.read.parquet(parquet_path)
        df_delta = spark.read.format("delta").load(delta_path)
        
        # Compter les lignes
        count_parquet = df_parquet.count()
        count_delta = df_delta.count()
        
        # Colonnes originales (sans les métadonnées ajoutées)
        original_cols = df_parquet.columns
        delta_original_cols = [c for c in df_delta.columns if not c.startswith("_")]
        
        # Validation
        if count_delta < count_parquet:
            print(f"  ⚠️ Nombre de lignes inférieur: {count_delta} < {count_parquet}")
            return False
        
        if set(original_cols) != set(delta_original_cols):
            print(f"  ⚠️ Schéma différent")
            return False
        
        print(f"  ✅ Validation OK: {count_delta:,} lignes")
        return True
        
    except Exception as e:
        print(f"  ❌ Erreur validation: {e}")
        return False


def main():
    """Pipeline principal de migration."""
    print("="*70)
    print("🚀 MIGRATION PARQUET → DELTA LAKE")
    print("="*70)
    print(f"\nSource: s3a://{GOLD_PARQUET_BUCKET}/")
    print(f"Destination: s3a://{GOLD_DELTA_BUCKET}/")
    print(f"\nTables à migrer: {len(TABLES_TO_MIGRATE)}")
    
    # Dry-run mode (optionnel)
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    if dry_run:
        print("\n⚠️ MODE DRY-RUN ACTIVÉ (aucune écriture)")
    
    spark = get_spark_session()
    
    # Statistiques de migration
    results = {
        "success": [],
        "failed": [],
        "skipped": []
    }
    
    # Migration de chaque table
    for i, table_name in enumerate(TABLES_TO_MIGRATE, 1):
        print(f"\n{'='*70}")
        print(f"[{i}/{len(TABLES_TO_MIGRATE)}] {table_name}")
        
        success, message = migrate_table(spark, table_name, dry_run=dry_run)
        
        if success:
            results["success"].append(table_name)
            if not dry_run:
                # Validation post-migration
                if validate_migration(spark, table_name):
                    print(f"  ✅ Validation réussie")
                else:
                    print(f"  ⚠️ Validation échouée")
        else:
            results["failed"].append(table_name)
            print(f"  {message}")
    
    # Résumé final
    print("\n" + "="*70)
    print("📊 RÉSUMÉ DE LA MIGRATION")
    print("="*70)
    print(f"\n✅ Succès: {len(results['success'])}/{len(TABLES_TO_MIGRATE)}")
    for table in results['success']:
        print(f"   - {table}")
    
    if results['failed']:
        print(f"\n❌ Échecs: {len(results['failed'])}")
        for table in results['failed']:
            print(f"   - {table}")
    
    if results['skipped']:
        print(f"\n⏭️ Ignorées: {len(results['skipped'])}")
        for table in results['skipped']:
            print(f"   - {table}")
    
    print("\n" + "="*70)
    print("✅ MIGRATION TERMINÉE")
    print("="*70)
    
    if not dry_run:
        print("\n💡 Prochaines étapes:")
        print("   1. Vérifier les tables Delta dans MinIO")
        print("   2. Tester les requêtes sur les nouvelles tables")
        print("   3. Mettre à jour vos pipelines pour utiliser Delta")
        print("   4. Optionnel: Supprimer les anciennes tables Parquet")
    
    spark.stop()
    
    # Exit code
    return 0 if not results['failed'] else 1


if __name__ == "__main__":
    exit(main())
