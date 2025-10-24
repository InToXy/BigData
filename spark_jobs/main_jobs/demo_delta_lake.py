#!/usr/bin/env python3
"""
demo_delta_lake.py

Script de démonstration des fonctionnalités Delta Lake sur la zone Gold.

Ce script illustre:
1. Création d'une table Delta
2. Lecture et écriture
3. Time travel
4. UPSERT (merge)
5. Optimisation
6. Historique des versions

Usage:
docker exec -it chu_jupyter spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/demo_delta_lake.py

"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta import configure_spark_with_delta_pip, DeltaTable

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")


def get_spark_session():
    """Initialise Spark avec Delta Lake."""
    builder = SparkSession.builder.appName("DeltaLake_Demo")
    
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


def demo_1_create_delta_table(spark):
    """Démo 1: Création d'une table Delta."""
    print("\n" + "="*70)
    print("📊 DÉMO 1: CRÉATION D'UNE TABLE DELTA")
    print("="*70)
    
    # Créer des données de test
    data = [
        ("I10", "Hypertension essentielle", 12500, 15000, 0.083),
        ("E11", "Diabète type 2", 9800, 14500, 0.065),
        ("J44", "BPCO", 7200, 11000, 0.048)
    ]
    
    df = spark.createDataFrame(
        data,
        schema=["diagnostic_code", "diagnostic_libelle", "nb_patients", "nb_hospitalisations", "taux"]
    )
    
    # Ajouter métadonnées
    df = df.withColumn("_created_at", current_timestamp())
    
    # Écrire en Delta
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    df.write.format("delta").mode("overwrite").save(path)
    
    print(f"✅ Table Delta créée: {path}")
    print(f"   Lignes: {df.count()}")
    print("\n📄 Contenu:")
    df.drop("_created_at").show(truncate=False)


def demo_2_read_delta_table(spark):
    """Démo 2: Lecture d'une table Delta."""
    print("\n" + "="*70)
    print("📖 DÉMO 2: LECTURE D'UNE TABLE DELTA")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Méthode 1: Via spark.read
    df = spark.read.format("delta").load(path)
    print(f"✅ Table lue via spark.read")
    print(f"   Lignes: {df.count()}")
    print(f"   Colonnes: {len(df.columns)}")
    
    # Méthode 2: Via DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    df2 = delta_table.toDF()
    print(f"\n✅ Table lue via DeltaTable")
    print(f"   Lignes: {df2.count()}")


def demo_3_append_data(spark):
    """Démo 3: Ajout de données (APPEND)."""
    print("\n" + "="*70)
    print("➕ DÉMO 3: AJOUT DE DONNÉES (APPEND)")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Nouvelles données
    new_data = [
        ("I50", "Insuffisance cardiaque", 6500, 9800, 0.043),
        ("N18", "Insuffisance rénale chronique", 5200, 7800, 0.035)
    ]
    
    df_new = spark.createDataFrame(
        new_data,
        schema=["diagnostic_code", "diagnostic_libelle", "nb_patients", "nb_hospitalisations", "taux"]
    ).withColumn("_created_at", current_timestamp())
    
    # Append
    df_new.write.format("delta").mode("append").save(path)
    
    print(f"✅ {df_new.count()} lignes ajoutées")
    
    # Vérifier
    df_all = spark.read.format("delta").load(path)
    print(f"   Total lignes maintenant: {df_all.count()}")
    print("\n📄 Nouvelles entrées:")
    df_all.filter(col("diagnostic_code").isin(["I50", "N18"])).drop("_created_at").show(truncate=False)


def demo_4_time_travel(spark):
    """Démo 4: Time Travel (voyage temporel)."""
    print("\n" + "="*70)
    print("⏱️ DÉMO 4: TIME TRAVEL (VOYAGE TEMPOREL)")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Version 0 (état initial)
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    print(f"📜 Version 0 (état initial):")
    print(f"   Lignes: {df_v0.count()}")
    
    # Version actuelle
    df_current = spark.read.format("delta").load(path)
    print(f"\n📜 Version actuelle:")
    print(f"   Lignes: {df_current.count()}")
    
    print(f"\n✅ Différence: +{df_current.count() - df_v0.count()} lignes")
    
    # Afficher l'historique
    print("\n📜 Historique complet:")
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)


def demo_5_upsert_merge(spark):
    """Démo 5: UPSERT (merge) - mise à jour intelligente."""
    print("\n" + "="*70)
    print("🔄 DÉMO 5: UPSERT (MERGE) - MISE À JOUR INTELLIGENTE")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Données de mise à jour (mix update + insert)
    updates = [
        ("I10", "Hypertension essentielle", 13000, 16000, 0.087),  # UPDATE
        ("E11", "Diabète type 2", 10200, 15200, 0.068),           # UPDATE
        ("K70", "Maladie alcoolique du foie", 4500, 6700, 0.030)  # INSERT
    ]
    
    df_updates = spark.createDataFrame(
        updates,
        schema=["diagnostic_code", "diagnostic_libelle", "nb_patients", "nb_hospitalisations", "taux"]
    ).withColumn("_created_at", current_timestamp())
    
    print(f"📦 Données à merger:")
    df_updates.drop("_created_at").show(truncate=False)
    
    # MERGE
    delta_table = DeltaTable.forPath(spark, path)
    
    delta_table.alias("target").merge(
        df_updates.alias("source"),
        "target.diagnostic_code = source.diagnostic_code"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"\n✅ MERGE terminé")
    
    # Afficher le résultat
    df_result = spark.read.format("delta").load(path)
    print(f"\n📄 Résultat (mise à jour des codes I10, E11 + ajout de K70):")
    df_result.filter(col("diagnostic_code").isin(["I10", "E11", "K70"])) \
        .drop("_created_at") \
        .orderBy("diagnostic_code") \
        .show(truncate=False)


def demo_6_optimize(spark):
    """Démo 6: Optimisation (OPTIMIZE + VACUUM)."""
    print("\n" + "="*70)
    print("🔧 DÉMO 6: OPTIMISATION (OPTIMIZE + Z-ORDER)")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Statistiques avant optimisation
    df_before = spark.read.format("delta").load(path)
    files_before = df_before.inputFiles()
    print(f"📊 Avant optimisation:")
    print(f"   Fichiers: {len(files_before)}")
    
    # OPTIMIZE
    delta_table = DeltaTable.forPath(spark, path)
    print(f"\n🔧 Exécution OPTIMIZE (compaction)...")
    delta_table.optimize().executeCompaction()
    
    # Statistiques après optimisation
    df_after = spark.read.format("delta").load(path)
    files_after = df_after.inputFiles()
    print(f"\n📊 Après optimisation:")
    print(f"   Fichiers: {len(files_after)}")
    print(f"   Réduction: {len(files_before) - len(files_after)} fichiers")
    
    # Z-Ordering (optionnel)
    print(f"\n🔧 Exécution Z-ORDER sur 'diagnostic_code'...")
    delta_table.optimize().executeZOrderBy("diagnostic_code")
    
    print(f"\n✅ Optimisation terminée - performances améliorées!")


def demo_7_schema_evolution(spark):
    """Démo 7: Evolution du schéma."""
    print("\n" + "="*70)
    print("🔄 DÉMO 7: EVOLUTION DU SCHÉMA (SCHEMA EVOLUTION)")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    # Lire le schéma actuel
    df_current = spark.read.format("delta").load(path)
    print(f"📋 Schéma actuel: {len(df_current.columns)} colonnes")
    print(f"   {', '.join(df_current.columns)}")
    
    # Ajouter une nouvelle colonne
    new_data = [
        ("G30", "Maladie d'Alzheimer", 3800, 5600, 0.025, "NEUROLOGIE")
    ]
    
    df_new_schema = spark.createDataFrame(
        new_data,
        schema=["diagnostic_code", "diagnostic_libelle", "nb_patients", "nb_hospitalisations", "taux", "specialite"]
    ).withColumn("_created_at", current_timestamp())
    
    print(f"\n➕ Ajout d'une nouvelle colonne 'specialite'...")
    
    # Écrire avec mergeSchema
    df_new_schema.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(path)
    
    # Vérifier le nouveau schéma
    df_updated = spark.read.format("delta").load(path)
    print(f"\n📋 Nouveau schéma: {len(df_updated.columns)} colonnes")
    print(f"   {', '.join(df_updated.columns)}")
    print(f"\n✅ Colonne 'specialite' ajoutée automatiquement!")
    
    # Afficher les données avec la nouvelle colonne
    print(f"\n📄 Données (NULL pour anciennes lignes sans 'specialite'):")
    df_updated.select("diagnostic_code", "diagnostic_libelle", "specialite") \
        .orderBy("diagnostic_code") \
        .show(truncate=False)


def demo_8_cleanup(spark):
    """Démo 8: Nettoyage (VACUUM)."""
    print("\n" + "="*70)
    print("🧹 DÉMO 8: NETTOYAGE (VACUUM)")
    print("="*70)
    
    path = "s3a://gold-delta/demo_kpi_hospitalisation"
    
    delta_table = DeltaTable.forPath(spark, path)
    
    # Afficher l'historique avant VACUUM
    print(f"📜 Historique avant VACUUM:")
    history_before = delta_table.history()
    version_count_before = history_before.count()
    print(f"   Versions disponibles: {version_count_before}")
    
    # VACUUM (garder 0 heures pour la démo - ne faites PAS ça en production!)
    print(f"\n🧹 Exécution VACUUM (rétention: 0h)...")
    print(f"   ⚠️ ATTENTION: En production, utilisez au moins 168h (7 jours)")
    
    try:
        delta_table.vacuum(0)
        print(f"   ✅ VACUUM terminé")
    except Exception as e:
        print(f"   ℹ️ VACUUM limité par protection (normal): {e}")
    
    print(f"\n💡 En production:")
    print(f"   - Rétention recommandée: 168h (7 jours)")
    print(f"   - Pour conformité: 720h+ (30 jours)")


def main():
    """Pipeline de démonstration complet."""
    print("="*70)
    print("🚀 DÉMONSTRATION DELTA LAKE - ZONE GOLD")
    print("="*70)
    print("\nCe script démontre les fonctionnalités clés de Delta Lake:")
    print("  1. Création de table")
    print("  2. Lecture")
    print("  3. Ajout de données (APPEND)")
    print("  4. Time travel")
    print("  5. UPSERT (MERGE)")
    print("  6. Optimisation")
    print("  7. Evolution de schéma")
    print("  8. Nettoyage (VACUUM)")
    
    input("\nAppuyez sur Entrée pour commencer...")
    
    spark = get_spark_session()
    
    try:
        # Démos séquentielles
        demo_1_create_delta_table(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_2_read_delta_table(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_3_append_data(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_4_time_travel(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_5_upsert_merge(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_6_optimize(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_7_schema_evolution(spark)
        input("\nAppuyez sur Entrée pour continuer...")
        
        demo_8_cleanup(spark)
        
        # Résumé final
        print("\n" + "="*70)
        print("✅ DÉMONSTRATION TERMINÉE")
        print("="*70)
        print("\n📊 Résumé:")
        
        path = "s3a://gold-delta/demo_kpi_hospitalisation"
        delta_table = DeltaTable.forPath(spark, path)
        df_final = spark.read.format("delta").load(path)
        
        print(f"   - Table: {path}")
        print(f"   - Lignes finales: {df_final.count()}")
        print(f"   - Colonnes: {len(df_final.columns)}")
        print(f"   - Versions: {delta_table.history().count()}")
        
        print("\n💡 Commandes utiles:")
        print(f"   # Lire la table")
        print(f"   df = spark.read.format('delta').load('{path}')")
        print(f"\n   # Time travel (version 0)")
        print(f"   df_v0 = spark.read.format('delta').option('versionAsOf', 0).load('{path}')")
        print(f"\n   # Historique")
        print(f"   delta_table = DeltaTable.forPath(spark, '{path}')")
        print(f"   delta_table.history().show()")
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        raise
    
    finally:
        spark.stop()
    
    print("\n" + "="*70)
    print("👋 Au revoir!")
    print("="*70)


if __name__ == "__main__":
    main()
