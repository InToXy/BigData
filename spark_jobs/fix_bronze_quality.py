#!/usr/bin/env python3
"""
Réingestion des tables Bronze problématiques (etablissements, hospitalisations)
Correction des mappings pour éliminer les valeurs NULL
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, regexp_replace, when, coalesce,
    to_date, year, month, substring, md5, 
    current_timestamp, concat_ws, monotonically_increasing_id
)

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bucket": "bronze"
}

def get_spark_session():
    """Crée une session Spark."""
    jars_dir = "/home/jovyan/jars"
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
    jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
    
    spark = SparkSession.builder \
        .appName("Fix_Bronze_Quality") \
        .config("spark.jars", jars_path) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session créée")
    return spark

def hash_pii(column):
    """Hash MD5 pour anonymisation."""
    return when(col(column).isNotNull(), md5(col(column).cast("string"))).otherwise(lit(None))

def add_metadata(df, source_system, source_table):
    """Ajoute les métadonnées techniques."""
    return df \
        .withColumn("_sk", monotonically_increasing_id()) \
        .withColumn("_hash_record", md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]))) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_source_table", lit(source_table)) \
        .withColumn("_ingestion_date", current_timestamp()) \
        .withColumn("_version", lit(1)) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_is_deleted", lit(False))

def validate_code_postal(cp_col):
    """Valide un code postal français."""
    return when(col(cp_col).rlike("^[0-9]{5}$"), col(cp_col)).otherwise(lit(None))

def normalize_pays(pays_col):
    """Normalise le pays avec défaut FRANCE."""
    return when(
        (col(pays_col).isNotNull()) & (trim(col(pays_col)) != ""),
        upper(trim(col(pays_col)))
    ).otherwise(lit("FRANCE"))

# ============================================================
# FIX 1: ETABLISSEMENTS
# ============================================================

def fix_etablissements(spark):
    """Réingère les établissements avec mapping corrigé."""
    print("\n" + "="*60)
    print("🔧 FIX: etablissement_sante.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Etablissement de SANTE/etablissement_sante.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # CORRECTION: finess_site souvent vide, utiliser identifiant_organisation comme fallback
    df_bronze = df.select(
        # Identifiants - COALESCE pour utiliser identifiant_organisation si finess_site est vide
        coalesce(
            when(trim(col("finess_site")) != "", trim(col("finess_site"))),
            trim(col("identifiant_organisation"))
        ).alias("finess"),
        trim(col("finess_etablissement_juridique")).alias("finess_juridique"),
        trim(col("identifiant_organisation")).alias("identifiant_organisation"),
        
        # Données publiques normalisées
        upper(trim(col("raison_sociale_site"))).alias("raison_sociale"),
        upper(trim(col("enseigne_commerciale_site"))).alias("enseigne_commerciale"),
        trim(col("siren_site")).alias("siren"),
        trim(col("siret_site")).alias("siret"),
        
        # PII géographiques hashées
        hash_pii("adresse").alias("adresse_hash"),
        trim(col("numero_voie")).alias("numero_voie"),
        upper(trim(col("type_voie"))).alias("type_voie"),
        hash_pii("voie").alias("voie_hash"),
        
        validate_code_postal("code_postal").alias("code_postal"),
        trim(col("code_commune")).alias("code_commune"),
        upper(trim(col("commune"))).alias("commune"),
        trim(col("cedex")).alias("cedex"),
        normalize_pays("pays").alias("pays"),
        
        # Contacts hashés
        hash_pii("telephone").alias("telephone_hash"),
        hash_pii("telephone_2").alias("telephone2_hash"),
        hash_pii("telecopie").alias("telecopie_hash"),
        hash_pii("email").alias("email_hash")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "etablissement_sante")
    
    # Vérifier qualité
    total = df_bronze.count()
    finess_null = df_bronze.filter(col("finess").isNull()).count()
    print(f"   ✅ Lignes bronze: {total}")
    print(f"   📊 Finess NULL: {finess_null} ({finess_null/total*100:.2f}%)")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/etablissements/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return total

# ============================================================
# FIX 2: HOSPITALISATIONS
# ============================================================

def fix_hospitalisations(spark):
    """Réingère les hospitalisations avec format de date corrigé."""
    print("\n" + "="*60)
    print("🔧 FIX: Hospitalisations.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Hospitalisation/Hospitalisations.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # CORRECTION: Format de date dd/MM/yyyy au lieu de M/d/yyyy
    df_bronze = df.select(
        # Identifiants non hashés
        trim(col("Num_Hospitalisation")).alias("num_hospitalisation_original"),
        col("Id_patient").cast("integer").alias("id_patient_original"),
        trim(col("identifiant_organisation")).alias("identifiant_organisation"),
        
        # Données médicales
        upper(trim(col("Code_diagnostic"))).alias("code_diagnostic"),
        trim(col("Suite_diagnostic_consultation")).alias("suite_diagnostic_consultation"),
        
        # Dates - FORMAT CORRIGÉ: dd/MM/yyyy
        to_date(col("Date_Entree"), "dd/MM/yyyy").alias("date_entree"),
        year(to_date(col("Date_Entree"), "dd/MM/yyyy")).alias("date_entree_annee"),
        month(to_date(col("Date_Entree"), "dd/MM/yyyy")).alias("date_entree_mois"),
        
        # Durée validée
        when(
            (col("Jour_Hospitalisation") >= 0) & (col("Jour_Hospitalisation") <= 365),
            col("Jour_Hospitalisation").cast("integer")
        ).otherwise(lit(None)).alias("jour_hospitalisation")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "hospitalisations")
    
    # Vérifier qualité
    total = df_bronze.count()
    date_null = df_bronze.filter(col("date_entree").isNull()).count()
    print(f"   ✅ Lignes bronze: {total}")
    print(f"   📊 Date_entree NULL: {date_null} ({date_null/total*100:.2f}%)")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/hospitalisations/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return total

# ============================================================
# MAIN
# ============================================================

def main():
    """Orchestre les corrections."""
    print("\n" + "="*60)
    print("🔧 CORRECTION QUALITÉ DONNÉES BRONZE")
    print("="*60)
    print(f"Début: {datetime.now()}")
    
    spark = get_spark_session()
    
    try:
        results = {}
        results["etablissements"] = fix_etablissements(spark)
        results["hospitalisations"] = fix_hospitalisations(spark)
        
        print("\n" + "="*60)
        print("✅ RÉSUMÉ DES CORRECTIONS")
        print("="*60)
        for table, count in results.items():
            print(f"   {table:25s}: {count:>10,} lignes")
        print("="*60)
        print(f"Fin: {datetime.now()}")
        print("✅ Corrections terminées avec succès!")
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
