#!/usr/bin/env python3
"""
Fix Silver - Recréer dim_professionnel avec les bonnes colonnes
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Silver Fix Professionnel") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("=" * 60)
    print("🔧 FIX SILVER - dim_professionnel avec profession et specialite")
    print("=" * 60)
    
    spark = get_spark_session()
    batch_id = F.current_timestamp()
    
    # Lire Bronze
    print("\n📖 Lecture Bronze professionnels_sante...")
    df_bronze = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/professionnels_sante/")
    print(f"   ✅ {df_bronze.count()} lignes")
    print(f"   Colonnes: {df_bronze.columns[:12]}...")
    
    # Créer dim_professionnel
    print("\n🔷 Création dim_professionnel...")
    
    cols_to_select = ["_sk"]
    optional_cols = [
        "civilite",
        "profession",
        "specialite",
        "categorie_professionnelle",
        "identifiant"
    ]
    
    for col_name in optional_cols:
        if col_name in df_bronze.columns:
            cols_to_select.append(col_name)
            print(f"   ✓ {col_name}")
    
    dim_prof = df_bronze.select(*[F.col(c) for c in cols_to_select])
    dim_prof = dim_prof.withColumnRenamed("_sk", "professionnel_id")
    
    # Métadonnées
    dim_prof = dim_prof \
        .withColumn("_silver_load_date", F.current_timestamp()) \
        .withColumn("_silver_batch_id", batch_id) \
        .withColumn("_silver_table", F.lit("dim_professionnel"))
    
    # Sauvegarder
    output_path = f"s3a://{BUCKET_SILVER}/dim_professionnel/"
    print(f"\n💾 Écriture: {output_path}")
    print(f"   Lignes: {dim_prof.count()}")
    
    dim_prof.write.mode("overwrite").parquet(output_path)
    print(f"✅ dim_professionnel recréée!")
    
    # Vérification
    print("\n🔍 Vérification...")
    df_check = spark.read.parquet(output_path)
    print(f"   Colonnes: {df_check.columns}")
    print(f"   Total: {df_check.count()}")
    print("\n   Échantillon:")
    df_check.select("professionnel_id", "profession", "specialite").show(10, False)
    
    spark.stop()
    print("\n✅ TERMINÉ")

if __name__ == "__main__":
    main()
