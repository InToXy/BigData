import os
import sys
import uuid as uuid_lib
import time
import gc
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, concat_ws, 
    trim, upper, lower, regexp_replace, when, 
    coalesce, monotonically_increasing_id, row_number,
    length, md5, udf, initcap, expr, sha2, to_date
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window
import re

def get_spark_session_optimized():
    """Session Spark ultra-optimisée pour deces.csv (2GO)."""
    try:
        # Configuration S3A comme dans bronze_ingestion.py
        jars_dir = "/home/jovyan/jars"
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
            jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
            print(f"📦 JARs trouvés: {len(jar_files)} fichiers")
        else:
            raise Exception("Dossier jars non trouvé")

        # Configuration comme dans bronze_ingestion.py
        spark_builder = SparkSession.builder \
            .appName("Bronze Pipeline - DECES 2GO") \
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
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.maxResultSize", "512m") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4")

        spark = spark_builder.getOrCreate()
        
        # Configuration Hadoop agressive comme dans bronze_ingestion.py
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
        
        print("✅ Session Spark initialisée avec succès")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark optimisée: {e}")
        raise

def test_minio_connection(spark):
    """Teste la connexion à MinIO comme dans bronze_ingestion.py."""
    try:
        print("🔍 Test de connexion à MinIO...")
        
        # Utiliser directement l'API Hadoop pour tester
        conf = spark._jsc.hadoopConfiguration()
        
        # Créer un système de fichiers S3A explicitement
        s3a_uri = spark._jvm.java.net.URI.create("s3a://bronze/")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(s3a_uri, conf)
        
        # Tester en créant un dossier test
        test_dir = spark._jvm.org.apache.hadoop.fs.Path("s3a://bronze/test_deces")
        
        if not fs.exists(test_dir):
            fs.mkdirs(test_dir)
        
        return True
            
    except Exception as e:
        print(f"❌ Test MinIO échoué: {e}")
        return False

def read_deces_csv_optimized(spark):
    """Lecture optimisée des fichiers décès."""
    print("📖 LECTURE DES FICHIERS DÉCÈS PAR LOTS...")
    
    start_time = time.time()
    total_count = 0
    
    # Lire le répertoire des fichiers découpés
    input_dir = "/data/source/csv/deces_parts"
    
    try:
        # Lister tous les fichiers CSV
        csv_files = sorted([f for f in os.listdir(input_dir) if f.endswith('.csv')])
        total_files = len(csv_files)
        print(f"📁 {total_files} fichiers à traiter...")
        
        # DataFrame final
        final_df = None
        
        # Traiter chaque fichier
        for i, csv_file in enumerate(csv_files, 1):
            file_path = os.path.join(input_dir, csv_file)
            print(f"\n[{i}/{total_files}] Traitement de {csv_file}...")
            
            # Lire le fichier
            current_df = spark.read \
                .option("header", "true") \
                .option("delimiter", ",") \
                .option("encoding", "UTF-8") \
                .option("mode", "PERMISSIVE") \
                .csv(f"file://{file_path}")
            
            # Compter les lignes
            current_count = current_df.count()
            total_count += current_count
            print(f"✓ {current_count:,} lignes lues")
            
            # Union avec le DataFrame final
            if final_df is None:
                final_df = current_df
            else:
                final_df = final_df.unionByName(current_df)
            
            # Forcer le garbage collection
            del current_df
            gc.collect()
            
        # Statistiques finales
        read_time = time.time() - start_time
        print(f"\n✅ Total: {total_count:,} lignes lues en {read_time:.1f}s")
        print(f"   - Débit: {total_count/read_time:,.0f} lignes/sec")
        
        return final_df
        
    except Exception as e:
        print(f"❌ Erreur lecture fichiers: {e}")
        raise

def normalize_deces_data(df):
    """Normalisation selon les règles métier."""
    print("🔁 NORMALISATION DES DONNÉES...")
    
    # 🔒 ANONYMISATION RGPD - Champs sensibles
    print("   🔒 Anonymisation des données sensibles...")
    
    # Noms et prénoms hashés avec MD5
    df = df.withColumn(
        "nom_hash",
        when(col("nom").isNotNull(), md5(trim(upper(col("nom"))))) \
        .otherwise(lit(""))
    )
    
    df = df.withColumn(
        "prenom_hash", 
        when(col("prenom").isNotNull(), md5(trim(initcap(col("prenom"))))) \
        .otherwise(lit(""))
    )
    
    # Numéro acte de décès hashé
    df = df.withColumn(
        "numero_acte_deces_hash",
        when(col("numero_acte_deces").isNotNull(), md5(trim(col("numero_acte_deces")))) \
        .otherwise(lit(""))
    )
    
    # 🔁 NORMALISATION - Chaînes de texte
    print("   🔁 Normalisation des chaînes de texte...")
    
    # Sexe normalisé (uppercase + trim)
    df = df.withColumn(
        "sexe_normalise",
        when(col("sexe").isNotNull(), trim(upper(col("sexe")))) \
        .otherwise(lit(None))
    )
    
    # Lieu de naissance en uppercase + trim
    df = df.withColumn(
        "lieu_naissance_normalise",
        when(col("lieu_naissance").isNotNull(), trim(upper(col("lieu_naissance")))) \
        .otherwise(lit(None))
    )
    
    # 🔁 NORMALISATION - Pays (défaut à FRANCE si vide)
    print("   🌍 Normalisation des pays...")
    
    df = df.withColumn(
        "pays_naissance_normalise",
        when(
            (col("pays_naissance").isNull()) | 
            (trim(col("pays_naissance")) == "") |
            (upper(trim(col("pays_naissance"))).isin("", "NULL", "NONE")),
            lit("FRANCE")
        ).otherwise(trim(upper(col("pays_naissance"))))
    )
    
    # 🔁 NORMALISATION - Dates
    print("   📅 Conversion des dates...")
    
    df = df.withColumn(
        "date_naissance_date",
        to_date(col("date_naissance"), "yyyy-MM-dd")
    )
    
    df = df.withColumn(
        "date_deces_date",
        to_date(col("date_deces"), "yyyy-MM-dd")
    )
    
    # 🔑 CLÉ TECHNIQUE - SK de substitution
    print("   🔑 Création de la clé technique...")
    
    # Création d'une clé technique auto-incrémentée
    window_spec = Window.orderBy(monotonically_increasing_id())
    df = df.withColumn("sk_deces", row_number().over(window_spec))
    
    # 📌 MÉTADONNÉES AUTOMATIQUES
    print("   📌 Ajout des métadonnées...")
    
    df = df.withColumn("_source", lit("deces"))
    df = df.withColumn("_version", lit(1))
    df = df.withColumn("_ingestion_date", current_timestamp())
    
    print("✅ Normalisation terminée")
    return df

def write_to_minio_optimized(df, table_name):
    """Écriture optimisée dans MinIO."""
    bronze_path = f"s3a://bronze/{table_name}"
    
    print(f"💾 ÉCRITURE OPTIMISÉE DANS MINIO...")
    start_time = time.time()
    
    try:
        print(f"   - Destination: {bronze_path}")
        
        # Écriture avec toutes les optimisations S3A
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("parquet.block.size", "33554432") \
            .option("parquet.page.size", "524288") \
            .option("parquet.dictionary.enabled", "true") \
            .option("parquet.enable.dictionary", "true") \
            .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .option("fs.s3a.fast.upload", "true") \
            .option("fs.s3a.multipart.size", "33554432") \
            .option("fs.s3a.threads.max", "4") \
            .option("maxRecordsPerFile", "50000") \
            .parquet(bronze_path)
        
        write_time = time.time() - start_time
        written_count = df.count()
        
        print(f"✅ SUCCÈS ÉCRITURE MINIO!")
        print(f"   - {written_count:,} lignes écrites")
        print(f"   - Temps d'écriture: {write_time:.1f}s")
        print(f"   - Débit: {written_count/write_time:,.0f} lignes/sec")
        
        return True, written_count
        
    except Exception as e:
        print(f"❌ ÉCHEC ÉCRITURE MINIO: {e}")
        raise

def process_deces_large_file(spark):
    """PIPELINE COMPLET OPTIMISÉ POUR deces.csv (2GO)."""
    print("\n" + "="*80)
    print("🚀 DÉMARRAGE PIPELINE OPTIMISÉ - deces.csv (2GO)")
    print("="*80)
    
    global_start_time = time.time()
    
    try:
        # 1. LECTURE OPTIMISÉE
        df = read_deces_csv_optimized(spark)
        
        # 2. NORMALISATION & ANONYMISATION
        df = normalize_deces_data(df)
        
        # 3. ÉCRITURE DANS MINIO
        success, final_count = write_to_minio_optimized(df, "deces")
        
        # RÉSUMÉ COMPLET
        global_time = time.time() - global_start_time
        
        print("\n" + "="*80)
        print("🎉 PIPELINE TERMINÉ AVEC SUCCÈS!")
        print("="*80)
        print(f"⏱️  TEMPS TOTAL: {global_time:.1f}s")
        print(f"🚀 DÉBIT MOYEN: {final_count/global_time:,.0f} lignes/sec")
        print(f"💾 DESTINATION: s3a://bronze/deces")
        print("="*80)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE DANS LE PIPELINE: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Nettoyage final
        gc.collect()

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         PIPELINE ULTRA-OPTIMISÉ - deces.csv (2GO)           ║
    ║  Traitement spécifique pour fichier volumineux              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    spark = None
    try:
        # Initialisation Spark optimisée
        spark = get_spark_session_optimized()
        
        # Test MinIO rapide
        if not test_minio_connection(spark):
            raise Exception("Échec connexion MinIO")
        
        # Lancer le pipeline spécifique
        success = process_deces_large_file(spark)
        
        if success:
            print("\n🎊 MISSION ACCOMPLIE! deces.csv traité avec succès!")
            sys.exit(0)
        else:
            print("\n💥 ÉCHEC du traitement deces.csv")
            sys.exit(1)
            
    except Exception as e:
        print(f"💥 ERREUR GLOBALE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            print("\n🔚 Arrêt propre de Spark...")
            spark.stop()
            print("✅ Spark arrêté")