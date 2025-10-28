#!/usr/bin/env python3
"""Script rapide pour ingérer les décès depuis la table existante dans Bronze."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

# Initialisation Spark
spark = SparkSession.builder \
    .appName("Quick Deces Fix") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("SOLUTION RAPIDE: Copier deces → deces_2019")
print("=" * 70)

try:
    # Lire la table deces qui existe déjà
    print("\n📥 Lecture s3a://bronze/deces...")
    df_deces = spark.read.parquet("s3a://bronze/deces")
    count = df_deces.count()
    print(f"   ✅ {count:,} lignes lues")
    
    # Filtrer pour 2019 si la colonne date_deces existe
    if "date_deces" in df_deces.columns:
        from pyspark.sql.functions import year
        df_2019 = df_deces.filter(year("date_deces") == 2019)
        count_2019 = df_2019.count()
        print(f"   🔍 {count_2019:,} décès en 2019")
        df_to_write = df_2019
    else:
        print("   ⚠️  Colonne date_deces introuvable, copie complète")
        df_to_write = df_deces
    
    # Écrire dans deces_2019
    print("\n💾 Écriture dans s3a://bronze/deces_2019...")
    df_to_write.write.mode("overwrite").parquet("s3a://bronze/deces_2019")
    
    # Vérification
    df_verify = spark.read.parquet("s3a://bronze/deces_2019")
    final_count = df_verify.count()
    
    print(f"\n✅ SUCCÈS!")
    print(f"   📊 {final_count:,} lignes dans s3a://bronze/deces_2019")
    print(f"   📋 Colonnes: {', '.join(df_verify.columns[:10])}")
    
    print("\n" + "=" * 70)
    print("➡️  Vous pouvez maintenant relancer: ./run_silver.sh")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ Erreur: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
