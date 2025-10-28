#!/usr/bin/env python3
"""
Fix Silver - Recréer dim_etablissement avec commune et raison_sociale_site
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder \
        .appName("Silver Fix Etablissement") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("=" * 60)
    print("🔧 FIX SILVER - dim_etablissement avec commune")
    print("=" * 60)
    
    spark = get_spark_session()
    batch_id = F.current_timestamp()
    
    # Lire Bronze
    print("\n📖 Lecture Bronze etablissements...")
    df_bronze = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/etablissements/")
    print(f"   ✅ {df_bronze.count()} lignes chargées")
    print(f"   Colonnes: {df_bronze.columns[:10]}...")
    
    # Vérifier commune
    commune_count = df_bronze.filter(F.col("commune").isNotNull()).count()
    print(f"   📊 Communes non nulles: {commune_count}")
    
    # Créer dim_etablissement avec toutes les colonnes nécessaires
    print("\n🔷 Création dim_etablissement...")
    
    # Colonnes à conserver
    cols_to_select = ["_sk"]
    optional_cols = [
        "finess_site",
        "identifiant_organisation",
        "raison_sociale_site",
        "commune",
        "code_commune",
        "code_postal",
        "region",
        "departement",
        "ville",
        "statut_juridique",
        "categorie"
    ]
    
    for col_name in optional_cols:
        if col_name in df_bronze.columns:
            cols_to_select.append(col_name)
            print(f"   ✓ {col_name}")
    
    dim_etab = df_bronze.select(*[F.col(c) for c in cols_to_select])
    
    # Renommer _sk
    dim_etab = dim_etab.withColumnRenamed("_sk", "etablissement_id")
    
    # Métadonnées Silver
    dim_etab = dim_etab \
        .withColumn("_silver_load_date", F.current_timestamp()) \
        .withColumn("_silver_batch_id", batch_id) \
        .withColumn("_silver_table", F.lit("dim_etablissement"))
    
    # Sauvegarder
    output_path = f"s3a://{BUCKET_SILVER}/dim_etablissement/"
    print(f"\n💾 Écriture: {output_path}")
    print(f"   Lignes: {dim_etab.count()}")
    
    # Vérifier commune avant écriture
    commune_output = dim_etab.filter(F.col("commune").isNotNull()).count()
    print(f"   Communes non nulles: {commune_output}")
    
    dim_etab.write.mode("overwrite").parquet(output_path)
    
    print(f"\n✅ dim_etablissement recréée avec succès!")
    
    # Vérification
    print("\n🔍 Vérification...")
    df_check = spark.read.parquet(output_path)
    print(f"   Colonnes: {df_check.columns}")
    print(f"   Total lignes: {df_check.count()}")
    
    if "commune" in df_check.columns:
        commune_check = df_check.filter(F.col("commune").isNotNull()).count()
        print(f"   ✅ Communes non nulles: {commune_check}")
        print("\n   Échantillon:")
        df_check.select("etablissement_id", "raison_sociale_site", "commune").show(10, False)
    else:
        print("   ❌ ERREUR: commune absente!")
    
    spark.stop()
    print("\n✅ TERMINÉ")

if __name__ == "__main__":
    main()
