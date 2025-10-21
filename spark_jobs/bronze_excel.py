import os
import sys
import uuid as uuid_lib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, lower, regexp_replace, when, 
    coalesce, monotonically_increasing_id, row_number,
    length, count as count_agg, md5, udf, to_date, to_timestamp
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window
import re

def get_spark_session():
    """Initialise et retourne une session Spark configurée pour MinIO avec les JARs locaux."""
    try:
        # Lister les JARs disponibles localement
        jars_dir = "/home/jovyan/jars"
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
            jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
            print(f"📦 JARs trouvés: {len(jar_files)} fichiers")
        else:
            raise Exception("Dossier jars non trouvé")

        # Configuration Spark avec S3A
        spark_builder = SparkSession.builder \
            .appName("Bronze Excel Pipeline - MinIO") \
            .config("spark.jars", jars_path) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "100000") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "5") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        spark = spark_builder.getOrCreate()
        
        # Configuration Hadoop pour MinIO
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.connection.timeout", "100000")
        hadoop_conf.set("fs.s3a.attempts.maximum", "5")
        hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")
        hadoop_conf.set("fs.s3a.impl.disable.cache", "false")
        
        print("✅ Session Spark initialisée avec succès avec configuration MinIO")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation de Spark avec MinIO: {e}")
        raise

def test_minio_connection(spark):
    """Teste la connexion à MinIO."""
    try:
        print("🔍 Test de connexion à MinIO...")
        
        conf = spark._jsc.hadoopConfiguration()
        s3a_uri = spark._jvm.java.net.URI.create("s3a://bronze/")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(s3a_uri, conf)
        test_dir = spark._jvm.org.apache.hadoop.fs.Path("s3a://bronze/test_excel")
        
        if not fs.exists(test_dir):
            fs.mkdirs(test_dir)
            print("✅ Dossier test créé dans MinIO")
        
        return True
            
    except Exception as e:
        print(f"❌ Test MinIO échoué: {e}")
        return False

def read_excel_file(spark, excel_path, sheet_name=None):
    """Lit un fichier Excel avec gestion des erreurs."""
    try:
        print(f"📑 Lecture du fichier Excel: {excel_path}")
        
        reader = spark.read.format("com.crealytics.spark.excel") \
            .option("useHeader", "true") \
            .option("treatEmptyValuesAsNulls", "true") \
            .option("inferSchema", "true") \
            .option("addColorColumns", "false") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            
        if sheet_name:
            reader = reader.option("sheetName", sheet_name)
            
        df = reader.load(excel_path)
        
        print(f"✅ Fichier Excel lu avec succès")
        return df
        
    except Exception as e:
        print(f"❌ Erreur lecture Excel: {e}")
        raise

def write_to_minio_direct(df, output_table_name):
    """Écrit directement dans MinIO en forçant S3A."""
    bronze_path = f"s3a://bronze/{output_table_name}"
    
    try:
        print(f"   💾 Écriture dans MinIO...")
        
        df.write \
            .mode("overwrite") \
            .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .option("fs.s3a.endpoint", "http://minio:9000") \
            .option("fs.s3a.access.key", "minioadmin") \
            .option("fs.s3a.secret.key", "minioadmin123") \
            .option("fs.s3a.path.style.access", "true") \
            .parquet(bronze_path)
        
        final_count = df.count()
        print(f"   ✅ {final_count} lignes écrites dans MinIO: {bronze_path}")
        return True, final_count
        
    except Exception as e:
        print(f"   ❌ Échec écriture MinIO: {e}")
        raise

def clean_col_names(df):
    """Nettoie et standardise les noms de colonnes."""
    def clean_name(name):
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        return name

    new_cols = [clean_name(c) for c in df.columns]
    return df.toDF(*new_cols)

def normalize_dates(df):
    """Normalise les colonnes de dates."""
    date_formats = [
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "MM/dd/yyyy",
        "yyyy/MM/dd",
        "dd-MM-yyyy",
        "yyyyMMdd"
    ]
    
    for column in df.columns:
        if column.startswith("_"):
            continue
            
        col_lower = column.lower()
        if ("date" in col_lower or 
            col_lower.endswith("_dt") or 
            "naissance" in col_lower or
            "deces" in col_lower):
            
            new_col_name = f"{column}_normalized"
            temp_col = col(column)
            
            for date_format in date_formats:
                temp_col = coalesce(
                    temp_col,
                    to_date(col(column), date_format)
                )
            
            df = df.withColumn(new_col_name, temp_col)
    
    return df

def normalize_data(df):
    """Normalise les données."""
    for column in df.columns:
        if column.startswith("_"):
            continue
        
        col_lower = column.lower()
        
        if "email" in col_lower:
            df = df.withColumn(column, lower(col(column)))
        elif "tel" in col_lower or "phone" in col_lower:
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9+]", ""))
        elif "nom" in col_lower or "prenom" in col_lower:
            df = df.withColumn(
                column,
                when(col(column).isNotNull(), 
                     regexp_replace(upper(col(column)), r"\s+", " "))
                .otherwise(None)
            )
    
    df = normalize_dates(df)
    return df

def add_technical_columns(df, source_name, output_table_name):
    """Ajoute les colonnes techniques."""
    generate_uuid_udf = udf(lambda: str(uuid_lib.uuid4()), StringType())
    
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_source", lit(source_name))
    df = df.withColumn("_table_name", lit(output_table_name))
    df = df.withColumn("_record_uuid", generate_uuid_udf())
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df

def anonymize_pii(df, pii_columns):
    """Anonymise les données sensibles (PII) en utilisant SHA-256."""
    for pii_col in pii_columns:
        if pii_col in df.columns:
            df = df.withColumn(
                pii_col,
                when(col(pii_col).isNotNull(),
                     sha2(col(pii_col).cast("string"), 256))
                .otherwise(None)
            )
            print(f"  🔒 Colonne '{pii_col}' anonymisée")
    return df

def add_surrogate_key(df, sk_columns):
    """Ajoute une clé de substitution (SK) basée sur les colonnes métier."""
    if not sk_columns:
        return df
    
    available_sk_cols = [c for c in sk_columns if c in df.columns]
    
    if not available_sk_cols:
        print(f"  ⚠️ Aucune colonne SK disponible")
        return df
    
    df = df.withColumn(
        "sk_id",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) 
                              for c in available_sk_cols]), 256)
    )
    
    print(f"  🔑 Clé SK créée à partir de : {', '.join(available_sk_cols)}")
    return df

def process_excel_file(spark, config):
    """Traite un fichier Excel."""
    excel_path = config["path"]
    output_table_name = config["output_table"]
    sheet_name = config.get("sheet_name")
    pii_columns = config.get("pii_columns", [])
    sk_columns = config.get("sk_columns", [])
    
    print(f"\n{'='*80}")
    print(f"🚀 Traitement Excel: {output_table_name}")
    print(f"{'='*80}")

    try:
        # 1. Lecture Excel
        df = read_excel_file(spark, excel_path, sheet_name)
        initial_count = df.count()
        print(f"📥 {initial_count} lignes lues")

        # 2. Nettoyage colonnes
        df = clean_col_names(df)
        print("🧹 Colonnes nettoyées")

        # 3. Normalisation
        df = normalize_data(df)
        print("📐 Données normalisées")

        # 4. Anonymisation PII
        if pii_columns:
            df = anonymize_pii(df, pii_columns)
            print("🔒 Données sensibles anonymisées")
        
        # 5. Ajout clé de substitution
        df = add_surrogate_key(df, sk_columns)
        print("🔑 Clé de substitution ajoutée")

        # 6. Colonnes techniques
        df = add_technical_columns(df, config["source_name"], output_table_name)
        print("⚙️ Colonnes techniques ajoutées")

        # 5. Écriture MinIO
        success, final_count = write_to_minio_direct(df, output_table_name)
        
        print("\n✅ Traitement terminé!")
        print(f"   - Lignes initiales : {initial_count}")
        print(f"   - Lignes finales : {final_count}")
        print(f"   - Destination : s3a://bronze/{output_table_name}")
        
        return True

    except Exception as e:
        print(f"\n❌ ERREUR: {str(e)}")
        raise

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║             BRONZE EXCEL PIPELINE - MinIO                     ║
    ║    Traitement des fichiers Excel vers la couche Bronze       ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        if not test_minio_connection(spark):
            raise Exception("Échec connexion MinIO")
            
        # Configuration des fichiers Excel
        excel_configs = [
            {
                "type": "excel",
                "source_name": "dpa_had_recueil2016_donnee2015_donnees.xlsx",
                "path": "file:///data/source/xlsx/dpa_had_recueil2016_donnee2015_donnees.xlsx",
                "sheet_name": "dpa_had_recueil2016_donnee2015_",
                "output_table": "satisfaction_2015_dpa_had_excel",
                "pii_columns": [],
                "sk_columns": ["finess"]
            },
            {
                "type": "excel",
                "source_name": "resultats-esatisca-mco-open-data-2020.xlsx",
                "path": "file:///data/source/xlsx/resultats-esatisca-mco-open-data-2020.xlsx",
                "sheet_name": "Resultats",
                "output_table": "satisfaction_2020_esatisca",
                "pii_columns": [],
                "sk_columns": ["finess"]
            },
            {
                "type": "excel",
                "source_name": "resultats-esatis48h-mco-open-data-2020.xlsx",
                "path": "file:///data/source/xlsx/resultats-esatis48h-mco-open-data-2020.xlsx",
                "sheet_name": "Resultats",
                "output_table": "satisfaction_2020_esatis48h",
                "pii_columns": [],
                "sk_columns": ["finess"]
            },
            {
                "type": "excel",
                "source_name": "resultats-iqss-open-data-2020.xlsx",
                "path": "file:///data/source/xlsx/resultats-iqss-open-data-2020.xlsx",
                "sheet_name": "Resultats",
                "output_table": "satisfaction_2020_iqss",
                "pii_columns": [],
                "sk_columns": ["finess"]
            }
        ]

        successful_files = []
        
        for config in excel_configs:
            try:
                process_excel_file(spark, config)
                successful_files.append(config["output_table"])
            except Exception as e:
                print(f"❌ Échec pour {config['output_table']}: {e}")
        
        print("\n" + "="*80)
        print("🎉 PIPELINE EXCEL TERMINÉ!")
        print("="*80)
        print(f"✅ Fichiers traités: {len(successful_files)}")
        for file in successful_files:
            print(f"   - {file}")
        
        spark.stop()
        sys.exit(0 if len(successful_files) == len(excel_configs) else 1)
        
    except Exception as e:
        print(f"💥 Erreur critique: {e}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)