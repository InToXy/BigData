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
        # Lister les JARs disponibles
        jars_dir = "/home/jovyan/jars"
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
            jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
            print(f"📦 JARs trouvés: {len(jar_files)} fichiers")
        else:
            raise Exception("Dossier jars non trouvé")

        # Configuration Spark ultra-light
        spark = SparkSession.builder \
            .appName("Bronze Pipeline - DECES 2GO") \
            .config("spark.jars", jars_path) \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.maxResultSize", "512m") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
            .master("local[*]") \
            .getOrCreate()

        # Configuration Hadoop pour MinIO
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        
        print("✅ Session Spark ADAPTÉE À WSL pour deces.csv")
        print(f"   - Driver Memory: 1G")
        print(f"   - Executor Memory: 512M")
        print(f"   - Partitions: 4")
        
        return spark

        spark = spark_builder.getOrCreate()
        
        # Configuration Hadoop pour MinIO
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.multipart.size", "67108864")
        
        print("✅ Session Spark ADAPTÉE À WSL pour deces.csv")
        print("   - Driver Memory: 2G")
        print("   - Executor Memory: 1G")
        print("   - Partition Size: 128MB")
        print("   - Partitions: 8")
        
        return spark
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark optimisée: {e}")
        raise

def read_deces_csv_optimized(spark):
    """Lecture des fichiers décès par lots."""
    print("📖 LECTURE DES FICHIERS DÉCÈS PAR LOTS...")
    
    start_time = time.time()
    total_count = 0
    final_df = None
    
    # Lire le répertoire des fichiers découpés
    input_dir = "/data/source/csv/deces_parts"
    
    try:
        # Lister tous les fichiers CSV dans le répertoire
        csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
        csv_files.sort()  # Assurer l'ordre de traitement
        
        print(f"📁 {len(csv_files)} fichiers à traiter...")
        
        # Traiter chaque fichier séparément
        for i, csv_file in enumerate(csv_files, 1):
            file_path = os.path.join(input_dir, csv_file)
            
            print(f"\n📄 Traitement de {csv_file} ({i}/{len(csv_files)})...")
            
            # Lire le fichier courant
            current_df = spark.read \
                .option("header", True) \
                .option("delimiter", ",") \
                .option("encoding", "UTF-8") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("nullValue", "") \
                .csv(f"file://{file_path}")
            
            # Compter les lignes
            current_count = current_df.count()
            total_count += current_count
            
            print(f"   ✓ {current_count:,} lignes lues")
            
            # Union avec le DataFrame final
            if final_df is None:
                final_df = current_df
            else:
                final_df = final_df.unionByName(current_df)
            
            # Forcer le garbage collection
            del current_df
            gc.collect()
            
    except Exception as e:
        print(f"❌ Erreur lecture fichiers: {e}")
        raise
        
    read_time = time.time() - start_time
    print(f"\n✅ Total: {total_count:,} lignes lues en {read_time:.1f}s")
    print(f"   - Débit: {total_count/read_time:,.0f} lignes/sec")
    
    initial_count = df.count()
    read_time = time.time() - start_time
    
    print(f"✅ {initial_count:,} lignes lues en {read_time:.1f}s")
    print(f"   - Débit: {initial_count/read_time:,.0f} lignes/sec")
    
    # Afficher les colonnes disponibles
    print(f"📋 Colonnes disponibles: {df.columns}")
    
    return df

def normalize_deces_data(df):
    """Normalisation selon les règles métier avec contrôle de la mémoire."""
    print("🔁 NORMALISATION DES DONNÉES PAR LOTS...")
    
    # Forcer le garbage collection
    gc.collect()
    
    # Vérifier si le DataFrame est trop volumineux
    estimated_size = df.count() * len(df.columns) * 8  # Estimation grossière en octets
    if estimated_size > 500_000_000:  # Si plus de 500MB
        print("⚠️ DataFrame volumineux détecté - Traitement par lots...")
        df = df.persist()  # Persister pour éviter les recalculs
        df = df.repartition(4)  # Répartir en 4 partitions maximum
    
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
    
    # 🔁 NORMALISATION - Codes postaux (vérification regex)
    print("   📮 Normalisation des codes postaux...")
    
    df = df.withColumn(
        "code_lieu_naissance_valide",
        when(
            col("code_lieu_naissance").rlike(r'^[0-9]{5}$'),
            col("code_lieu_naissance")
        ).otherwise(lit(None))
    )
    
    df = df.withColumn(
        "code_lieu_deces_valide",
        when(
            col("code_lieu_deces").rlike(r'^[0-9]{5}$'),
            col("code_lieu_deces")
        ).otherwise(lit(None))
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


def clean_deces_data(df):
    """Nettoyage des données avec validation."""
    print("🧼 NETTOYAGE ET VALIDATION...")
    
    start_time = time.time()
    before_count = df.count()
    
    # Supprimer les lignes où toutes les colonnes importantes sont nulles
    important_columns = ["nom", "prenom", "date_naissance", "date_deces"]
    available_important_cols = [c for c in important_columns if c in df.columns]
    
    if available_important_cols:
        condition = lit(False)
        for col_name in available_important_cols:
            condition = condition | col(col_name).isNotNull()
        df = df.filter(condition)
    
    after_count = df.count()
    clean_time = time.time() - start_time
    removed_count = before_count - after_count
    
    print(f"✓ {removed_count:,} lignes invalides supprimées en {clean_time:.1f}s")
    print(f"✓ {after_count:,} lignes après nettoyage")
    
    return df

def select_final_columns(df):
    """Sélectionne et organise les colonnes finales selon le mapping Bronze."""
    print("📋 ORGANISATION DES COLONNES FINALES...")
    
    # Définition des colonnes finales selon le mapping Bronze
    final_columns = [
        # 🔑 Clé technique
        "sk_deces",
        
        # 🔒 Champs anonymisés (RGPD)
        "nom_hash",
        "prenom_hash", 
        "numero_acte_deces_hash",
        
        # 🔁 Champs normalisés
        "sexe_normalise",
        "date_naissance_date",
        "code_lieu_naissance_valide",
        "lieu_naissance_normalise", 
        "pays_naissance_normalise",
        "date_deces_date",
        "code_lieu_deces_valide",
        
        # 📌 Métadonnées
        "_source",
        "_version", 
        "_ingestion_date"
    ]
    
    # Sélectionner uniquement les colonnes disponibles
    available_final_columns = [c for c in final_columns if c in df.columns]
    
    df = df.select(*available_final_columns)
    
    print(f"✓ {len(available_final_columns)} colonnes finales sélectionnées")
    return df

def remove_duplicates_optimized(df):
    """Dédoublonnage basé sur la clé technique."""
    print("🗑️ DÉDOUBLONNAGE...")
    
    before_count = df.count()
    
    # Dédoublonnage basé sur la clé technique
    window_spec = Window.partitionBy("sk_deces").orderBy(col("_ingestion_date").desc())
    df = df.withColumn("_row_num", row_number().over(window_spec))
    df = df.filter(col("_row_num") == 1).drop("_row_num")
    
    after_count = df.count()
    duplicates_removed = before_count - after_count
    
    print(f"✓ {duplicates_removed:,} doublons supprimés")
    print(f"✓ {after_count:,} lignes uniques")
    
    return df

def write_deces_to_minio_optimized(df):
    """Écriture dans MinIO avec contrôle de la mémoire."""
    bronze_path = "s3a://bronze/deces"
    
    print("💾 ÉCRITURE DANS MINIO PAR LOTS...")
    
    start_time = time.time()
    batch_size = 50000  # Taille de lot pour l'écriture
    
    try:
        # Forcer le garbage collection avant l'écriture
        gc.collect()
        
        # Nombre total de lignes
        total_rows = df.count()
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        print(f"📦 Préparation de l'écriture en {num_batches} lots...")
        
        # Écrire par lots
        for i in range(num_batches):
            start_idx = i * batch_size
            
            # Sélectionner un lot
            batch_df = df.limit(batch_size)
            
            # Chemin pour ce lot
            batch_path = f"{bronze_path}/part_{i:04d}"
            
            print(f"   💾 Écriture lot {i+1}/{num_batches}...")
            
            # Écrire le lot
            batch_df.write \
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
                .parquet(batch_path)
        
        write_time = time.time() - start_time
        written_count = df.count()
        
        print(f"✅ SUCCÈS ÉCRITURE MINIO!")
        print(f"   - {written_count:,} lignes écrites")
        print(f"   - Temps d'écriture: {write_time:.1f}s")
        print(f"   - Débit: {written_count/write_time:,.0f} lignes/sec")
        print(f"   - Destination: {bronze_path}")
        
        return True, written_count
        
    except Exception as e:
        print(f"❌ ÉCHEC ÉCRITURE MINIO: {e}")
        raise

def get_deces_quality_stats(df):
    """Statistiques de qualité optimisées."""
    print("📊 STATISTIQUES DE QUALITÉ...")
    
    total_rows = df.count()
    
    print(f"   - Lignes totales: {total_rows:,}")
    print(f"   - Colonnes: {len(df.columns)}")
    
    # Stats sur les champs importants
    key_columns = [
        "nom_hash", "prenom_hash", "date_naissance_date", 
        "date_deces_date", "sexe_normalise"
    ]
    
    for column in key_columns:
        if column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            fill_rate = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
            print(f"   - {column}: {fill_rate:.1f}% rempli")

def process_deces_large_file(spark):
    """PIPELINE COMPLET OPTIMISÉ POUR deces.csv (2GO)."""
    print("\n" + "="*100)
    print("🚀 DÉMARRAGE PIPELINE ULTRA-OPTIMISÉ - deces.csv (2GO)")
    print("="*100)
    
    global_start_time = time.time()
    
    try:
        # ========================================
        # 1. LECTURE ULTRA-OPTIMISÉE
        # ========================================
        stage_start = time.time()
        df = read_deces_csv_optimized(spark)
        read_time = time.time() - stage_start
        
        # ========================================
        # 2. NETTOYAGE BASIQUE
        # ========================================
        stage_start = time.time()
        df = clean_deces_data(df)
        clean_time = time.time() - stage_start
        
        # ========================================
        # 3. NORMALISATION COMPLÈTE
        # ========================================
        stage_start = time.time()
        df = normalize_deces_data(df)
        normalize_time = time.time() - stage_start
        
        # ========================================
        # 4. PERSISTENCE INTERMÉDIAIRE
        # ========================================
        print("💾 PERSISTENCE INTERMÉDIAIRE...")
        df.persist()
        intermediate_count = df.count()
        print(f"✓ DataFrame persisté: {intermediate_count:,} lignes")
        
        # ========================================
        # 5. SÉLECTION COLONNES FINALES
        # ========================================
        stage_start = time.time()
        df = select_final_columns(df)
        select_time = time.time() - stage_start
        
        # ========================================
        # 6. DÉDOUBLONNAGE
        # ========================================
        stage_start = time.time()
        df = remove_duplicates_optimized(df)
        dedup_time = time.time() - stage_start
        
        # ========================================
        # 7. ÉCRITURE MINIO
        # ========================================
        stage_start = time.time()
        success, final_count = write_deces_to_minio_optimized(df)
        write_time = time.time() - stage_start
        
        # ========================================
        # 8. STATISTIQUES FINALES
        # ========================================
        get_deces_quality_stats(df)
        
        # ========================================
        # RÉSUMÉ COMPLET
        # ========================================
        global_time = time.time() - global_start_time
        
        print("\n" + "="*100)
        print("🎉 PIPELINE deces.csv TERMINÉ AVEC SUCCÈS!")
        print("="*100)
        print(f"📊 TEMPS PAR ÉTAPE:")
        print(f"   - Lecture: {read_time:.1f}s")
        print(f"   - Nettoyage: {clean_time:.1f}s")
        print(f"   - Normalisation: {normalize_time:.1f}s")
        print(f"   - Sélection colonnes: {select_time:.1f}s")
        print(f"   - Dédoublonnage: {dedup_time:.1f}s")
        print(f"   - Écriture MinIO: {write_time:.1f}s")
        print(f"   ⏱️  TEMPS TOTAL: {global_time:.1f}s")
        print(f"   🚀 DÉBIT MOYEN: {final_count/global_time:,.0f} lignes/sec")
        print(f"   💾 DESTINATION: s3a://bronze/deces")
        print("="*100)
        
        # Nettoyage final
        df.unpersist()
        gc.collect()
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE DANS LE PIPELINE deces.csv")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

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
        print("🔍 Test connexion MinIO...")
        test_df = spark.createDataFrame([(1, "test_deces")], ["id", "data"])
        test_df.write \
            .mode("overwrite") \
            .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .parquet("s3a://bronze/test_deces")
        print("✅ Connexion MinIO OK")
        
        # Lancer le pipeline spécifique
        success = process_deces_large_file(spark)
        
        if success:
            print("\n🎊 MISSION ACCOMPLIE! deces.csv (2GO) traité avec succès!")
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