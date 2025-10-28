#!/usr/bin/env python3
"""
Pipeline Bronze - Ingestion CSV uniquement vers MinIO
Filtre décès 2019 uniquement
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, year, monotonically_increasing_id
)
import uuid

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "bronze"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder \
        .appName("Bronze CSV Ingestion") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
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

def add_technical_columns(df, source_name, batch_id):
    """Ajoute les colonnes techniques Bronze."""
    df = df.withColumn("_source_system", lit("CSV"))
    df = df.withColumn("_source_table", lit(source_name))
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_batch_id", lit(batch_id))
    df = df.withColumn("_version", lit(1))
    df = df.withColumn("_is_current", lit(True))
    df = df.withColumn("_is_deleted", lit(False))
    df = df.withColumn("_sk", monotonically_increasing_id())
    return df

def process_deces(spark, batch_id):
    """Charge les décès 2019 uniquement."""
    print("\n🎯 TRAITEMENT: deces (filtre 2019)")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "utf-8") \
            .option("inferSchema", "true") \
            .csv("file:///data/source/DECES EN FRANCE/deces.csv")
        
        print(f"   📊 Lignes totales: {df.count()}")
        
        # Filtrer 2019
        if "date_deces" in df.columns:
            df = df.withColumn("date_deces", col("date_deces").cast("date"))
            df = df.filter(year(col("date_deces")) == 2019)
            count_2019 = df.count()
            print(f"   ✅ Lignes 2019: {count_2019}")
        
        # Anonymisation PII
        pii_cols = ["nom", "prenom", "adresse", "ville"]
        for pii_col in pii_cols:
            if pii_col in df.columns:
                df = df.withColumn(pii_col, sha2(col(pii_col), 256))
        
        df = add_technical_columns(df, "deces", batch_id)
        
        output_path = f"s3a://{BUCKET}/deces/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Écrit dans {output_path}")
        return True
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

def process_etablissements(spark, batch_id):
    """Charge les établissements."""
    print("\n🎯 TRAITEMENT: etablissements")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "utf-8") \
            .option("inferSchema", "true") \
            .csv("file:///data/source/Etablissement de SANTE/etablissement_sante.csv")
        
        count = df.count()
        print(f"   📊 Lignes: {count}")
        
        # Anonymisation
        pii_cols = ["email", "telephone", "telephone_2", "adresse"]
        for pii_col in pii_cols:
            if pii_col in df.columns:
                df = df.withColumn(pii_col, sha2(col(pii_col), 256))
        
        df = add_technical_columns(df, "etablissements", batch_id)
        
        output_path = f"s3a://{BUCKET}/etablissements/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Écrit dans {output_path}")
        return True
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

def process_professionnels(spark, batch_id):
    """Charge les professionnels de santé."""
    print("\n🎯 TRAITEMENT: professionnels_sante")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "utf-8") \
            .option("inferSchema", "true") \
            .csv("file:///data/source/Etablissement de SANTE/professionnel_sante.csv")
        
        count = df.count()
        print(f"   📊 Lignes: {count}")
        
        # Anonymisation
        pii_cols = ["Nom", "Prenom"]
        for pii_col in pii_cols:
            if pii_col in df.columns:
                df = df.withColumn(pii_col, sha2(col(pii_col), 256))
        
        df = add_technical_columns(df, "professionnels_sante", batch_id)
        
        output_path = f"s3a://{BUCKET}/professionnels_sante/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Écrit dans {output_path}")
        return True
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

def process_hospitalisations(spark, batch_id):
    """Charge les hospitalisations."""
    print("\n🎯 TRAITEMENT: hospitalisations")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "ISO-8859-1") \
            .option("inferSchema", "true") \
            .csv("file:///data/source/Hospitalisation/Hospitalisations.csv")
        
        count = df.count()
        print(f"   📊 Lignes: {count}")
        
        df = add_technical_columns(df, "hospitalisations", batch_id)
        
        output_path = f"s3a://{BUCKET}/hospitalisations/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Écrit dans {output_path}")
        return True
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

def process_satisfaction_2019(spark, batch_id):
    """Charge les données de satisfaction (disponibles)."""
    print("\n🎯 TRAITEMENT: satisfaction")
    try:
        # Essayer plusieurs fichiers de satisfaction
        paths = [
            "file:///data/source/Satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv",
            "file:///data/source/Satisfaction/2016/dan_mco_recueil2016_donnee2015_donnees.csv"
        ]
        
        for path in paths:
            try:
                df = spark.read \
                    .option("header", "true") \
                    .option("delimiter", ";") \
                    .option("encoding", "utf-8") \
                    .option("inferSchema", "true") \
                    .csv(path)
                
                count = df.count()
                print(f"   📊 Lignes: {count} ({path.split('/')[-1]})")
                
                df = add_technical_columns(df, "satisfaction", batch_id)
                
                output_path = f"s3a://{BUCKET}/satisfaction/"
                df.write.mode("overwrite").parquet(output_path)
                print(f"   ✅ Écrit dans {output_path}")
                return True
            except:
                continue
        
        print("   ❌ Aucun fichier satisfaction accessible")
        return False
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════╗
    ║    PIPELINE BRONZE - CSV UNIQUEMENT       ║
    ║        DÉCÈS 2019 UNIQUEMENT              ║
    ╚═══════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        batch_id = str(uuid.uuid4())
        print(f"📦 Batch ID: {batch_id}")
        
        # Test MinIO
        print("\n🔍 Test MinIO...")
        test_df = spark.createDataFrame([(1, "test")], ["id", "data"])
        test_df.write.mode("overwrite").parquet(f"s3a://{BUCKET}/test/")
        print("✅ MinIO OK")
        
        # Traitement des sources
        results = []
        results.append(("deces", process_deces(spark, batch_id)))
        results.append(("etablissements", process_etablissements(spark, batch_id)))
        results.append(("professionnels_sante", process_professionnels(spark, batch_id)))
        results.append(("hospitalisations", process_hospitalisations(spark, batch_id)))
        results.append(("satisfaction", process_satisfaction_2019(spark, batch_id)))
        
        # Résumé
        print("\n" + "="*50)
        print("🎉 PIPELINE BRONZE TERMINÉ")
        print("="*50)
        
        success = [r[0] for r in results if r[1]]
        failed = [r[0] for r in results if not r[1]]
        
        print(f"✅ Succès: {len(success)} tables")
        for table in success:
            print(f"   ✅ {table}")
        
        if failed:
            print(f"\n❌ Échecs: {len(failed)} tables")
            for table in failed:
                print(f"   ❌ {table}")
        
        spark.stop()
        sys.exit(0 if not failed else 1)
        
    except Exception as e:
        print(f"\n💥 ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
