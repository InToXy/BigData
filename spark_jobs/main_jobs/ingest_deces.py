#!/usr/bin/env python3
"""
Ingestion spécifique pour les données de décès 2019
Crée la table deces_2019 dans Bronze
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, lit, sha2, concat_ws, when
from datetime import datetime
import os

# Configuration MinIO
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    try:
        jars_dir = "/home/jovyan/jars"
        jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
        jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
        
        spark = SparkSession.builder \
            .appName("Ingestion Deces 2019") \
            .config("spark.jars", jars_path) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Configuration Hadoop
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        
        print("✅ Spark session créée")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def ingest_deces_2019(spark):
    """Ingère les données de décès 2019 depuis PostgreSQL vers Bronze."""
    
    print("""
╔══════════════════════════════════════════════════╗
║     INGESTION DONNÉES DÉCÈS 2019 - BRONZE       ║
╚══════════════════════════════════════════════════╝
    """)
    
    # Configuration PostgreSQL
    pg_config = {
        "url": "jdbc:postgresql://chu_postgres_data:5432/healthcare_data",
        "properties": {
            "user": "admin",
            "password": "admin123",
            "driver": "org.postgresql.Driver"
        }
    }
    
    try:
        # 1. Lecture depuis PostgreSQL avec filtre 2019
        print("📥 Lecture des décès depuis PostgreSQL...")
        
        df_deces = spark.read \
            .jdbc(
                url=pg_config["url"],
                table='(SELECT * FROM "deces" WHERE EXTRACT(YEAR FROM date_deces) = 2019) as deces_2019',
                properties=pg_config["properties"]
            )
        
        count_source = df_deces.count()
        print(f"   ✅ {count_source:,} décès en 2019 lus depuis PostgreSQL")
        
        # 2. Anonymisation RGPD
        print("🔐 Anonymisation RGPD...")
        
        pii_columns = ["nom", "prenom", "adresse", "ville"]
        
        for col_name in pii_columns:
            if col_name in df_deces.columns:
                df_deces = df_deces.withColumn(
                    col_name,
                    sha2(concat_ws("||", col(col_name), lit("CHU_SALT_2024")), 256)
                )
        
        print(f"   ✅ {len([c for c in pii_columns if c in df_deces.columns])} colonnes anonymisées")
        
        # 3. Normalisation des colonnes
        print("🔧 Normalisation des colonnes...")
        
        # Snake case
        for col_name in df_deces.columns:
            new_name = col_name.lower().replace(" ", "_").replace("-", "_")
            if new_name != col_name:
                df_deces = df_deces.withColumnRenamed(col_name, new_name)
        
        # 4. Ajout métadonnées Bronze
        print("📝 Ajout métadonnées Bronze...")
        
        df_deces = df_deces \
            .withColumn("bronze_created_at", lit(datetime.now().isoformat())) \
            .withColumn("source_layer", lit("bronze")) \
            .withColumn("source_type", lit("postgres")) \
            .withColumn("year_filter", lit(2019))
        
        # 5. Écriture dans MinIO
        output_path = "s3a://bronze/deces_2019"
        
        print(f"💾 Écriture dans {output_path}...")
        
        df_deces.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        # 6. Vérification
        print("🔍 Vérification de l'écriture...")
        
        df_verify = spark.read.parquet(output_path)
        count_written = df_verify.count()
        
        print(f"""
╔══════════════════════════════════════════════════╗
║              RÉSULTAT INGESTION                  ║
╠══════════════════════════════════════════════════╣
║  Source PostgreSQL : {count_source:>10,} lignes     ║
║  Écrit Bronze      : {count_written:>10,} lignes     ║
║  Chemin            : {output_path:<30} ║
║  Format            : Parquet Snappy              ║
║  Anonymisation     : ✅ RGPD Compliant            ║
╚══════════════════════════════════════════════════╝
        """)
        
        # Afficher schéma
        print("\n📋 Schéma de la table deces_2019:")
        df_verify.printSchema()
        
        # Exemple de données
        print("\n📊 Aperçu des données (5 premières lignes):")
        df_verify.select(
            "sexe", "date_naissance", "date_deces", 
            "region", "departement", "code_postal"
        ).show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'ingestion: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Point d'entrée principal."""
    spark = get_spark_session()
    
    try:
        success = ingest_deces_2019(spark)
        
        if success:
            print("\n✅ Ingestion des décès 2019 réussie")
            print("➡️  Vous pouvez maintenant relancer le job Silver")
            return 0
        else:
            print("\n❌ Échec de l'ingestion")
            return 1
            
    finally:
        spark.stop()
        print("🛑 Spark session fermée")

if __name__ == "__main__":
    exit(main())
