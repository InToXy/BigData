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
            for jar in jar_files:
                print(f"   - {jar}")
        else:
            raise Exception("Dossier jars non trouvé")
        
        # Configuration Spark avec FORCE de S3A - APPROCHE DIFFÉRENTE
        spark_builder = SparkSession.builder \
            .appName("Bronze Ingestion Pipeline - MinIO") \
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
        
        # Configuration Hadoop pour MinIO - APPROCHE AGGRESSIVE
        hadoop_conf = spark._jsc.hadoopConfiguration()
        
        # FORCER l'implémentation S3A de manière agressive
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
        
        # FORCER le schéma S3A pour éviter le fallback vers file://
        hadoop_conf.set("fs.s3a.impl.disable.cache", "false")
        
        print("✅ Session Spark initialisée avec succès avec configuration MinIO")
        
        return spark
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation de Spark avec MinIO: {e}")
        raise

def test_minio_connection(spark):
    """Teste la connexion à MinIO avec approche différente."""
    try:
        print("🔍 Test de connexion à MinIO (approche directe)...")
        
        # Utiliser directement l'API Hadoop pour tester
        conf = spark._jsc.hadoopConfiguration()
        
        # Créer un système de fichiers S3A explicitement
        s3a_uri = spark._jvm.java.net.URI.create("s3a://bronze/")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(s3a_uri, conf)
        
        # Tester en créant un dossier test
        test_dir = spark._jvm.org.apache.hadoop.fs.Path("s3a://bronze/test_spark")
        
        if not fs.exists(test_dir):
            fs.mkdirs(test_dir)
            print("✅ Dossier test créé dans MinIO")
        
        # Tester l'écriture avec Spark directement
        test_df = spark.createDataFrame([(1, "test"), (2, "minio")], ["id", "data"])
        test_path = "s3a://bronze/test_spark/data"
        
        # Écrire avec des options explicites
        test_df.write \
            .mode("overwrite") \
            .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .parquet(test_path)
        
        print("✅ Écriture test réussie dans MinIO")
        
        # Lire pour vérifier
        test_read_df = spark.read \
            .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .parquet(test_path)
        
        count = test_read_df.count()
        print(f"✅ Lecture test réussie - {count} lignes")
        
        # Nettoyer
        if fs.exists(test_dir):
            fs.delete(test_dir, True)
            print("✅ Données test nettoyées")
        
        return True
            
    except Exception as e:
        print(f"❌ Test MinIO échoué: {e}")
        return False

def write_to_minio_direct(df, output_table_name):
    """Écrit directement dans MinIO en forçant S3A."""
    bronze_path = f"s3a://bronze/{output_table_name}"
    
    try:
        print(f"   💾 Écriture directe dans MinIO...")
        
        # Écriture avec options S3A explicites
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
    """Nettoie et standardise les noms de colonnes d'un DataFrame."""
    def clean_name(name):
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        return name

    new_cols = [clean_name(c) for c in df.columns]
    return df.toDF(*new_cols)

def remove_duplicates(df, sk_columns):
    """Supprime les doublons en se basant sur les colonnes clés."""
    if not sk_columns or not all(c in df.columns for c in sk_columns):
        print(f"⚠️ Avertissement : Colonnes SK {sk_columns} non disponibles, dédoublonnage sur toutes les colonnes")
        return df.dropDuplicates()
    
    window_spec = Window.partitionBy(*sk_columns).orderBy(col("_ingestion_date").desc())
    df_with_row_num = df.withColumn("_row_num", row_number().over(window_spec))
    df_deduplicated = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
    
    return df_deduplicated

def clean_data(df):
    """Nettoie les données."""
    for column in df.columns:
        if column.startswith("_"):
            continue
            
        col_type = df.schema[column].dataType
        
        if isinstance(col_type, StringType):
            df = df.withColumn(
                column,
                when(
                    (trim(col(column)) == "") | 
                    (trim(col(column)).isNull()) |
                    (upper(trim(col(column))).isin("NULL", "NA", "N/A", "NONE", "UNKNOWN", "-")),
                    lit(None)
                ).otherwise(trim(col(column)))
            )
    
    return df

def remove_empty_rows(df, threshold=0.5):
    """Supprime les lignes avec trop de valeurs nulles."""
    total_cols = len([c for c in df.columns if not c.startswith("_")])
    min_non_null = int(total_cols * threshold)
    
    if total_cols == 0:
        return df
    
    non_null_expr = lit(0)
    for column in [c for c in df.columns if not c.startswith("_")]:
        non_null_expr = non_null_expr + when(col(column).isNotNull(), 1).otherwise(0)
    
    df = df.withColumn("_non_null_count", non_null_expr)
    df = df.filter(col("_non_null_count") >= min_non_null)
    df = df.drop("_non_null_count")
    
    return df

def normalize_dates(df):
    """
    Normalise les colonnes de dates en détectant automatiquement 
    les colonnes contenant 'date' et en appliquant plusieurs formats.
    """
    date_formats = [
        "yyyy-MM-dd",      # ISO 8601: 2024-01-15
        "dd/MM/yyyy",      # Format français: 15/01/2024
        "MM/dd/yyyy",      # Format US: 01/15/2024
        "yyyy/MM/dd",      # Format alternatif: 2024/01/15
        "dd-MM-yyyy",      # Format avec tirets: 15-01-2024
        "yyyyMMdd"         # Format compact: 20240115
    ]
    
    date_columns_normalized = []
    
    for column in df.columns:
        if column.startswith("_"):
            continue
        
        col_lower = column.lower()
        col_type = df.schema[column].dataType
        
        # Détecter les colonnes de dates
        if ("date" in col_lower or 
            col_lower.endswith("_dt") or 
            col_lower.startswith("dt_") or
            "naissance" in col_lower or
            "deces" in col_lower or
            "admission" in col_lower or
            "sortie" in col_lower):
            
            # Si c'est déjà un DateType, on le garde
            if isinstance(col_type, DateType):
                continue
            
            # Sinon, on tente la conversion avec plusieurs formats
            new_col_name = f"{column}_normalized"
            temp_col = col(column)
            
            # Essayer chaque format de date
            for date_format in date_formats:
                temp_col = coalesce(
                    temp_col,
                    to_date(col(column), date_format)
                )
            
            df = df.withColumn(new_col_name, temp_col)
            date_columns_normalized.append((column, new_col_name))
    
    if date_columns_normalized:
        print(f"  📅 {len(date_columns_normalized)} colonnes de dates normalisées:")
        for orig, norm in date_columns_normalized:
            print(f"     - {orig} → {norm}")
    
    return df

def normalize_data(df):
    """Normalise les données selon le type."""
    for column in df.columns:
        if column.startswith("_"):
            continue
        
        col_lower = column.lower()
        
        if "email" in col_lower or "mail" in col_lower:
            df = df.withColumn(column, lower(col(column)))
        elif "tel" in col_lower or "phone" in col_lower or "telephone" in col_lower:
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9+]", ""))
        elif "code_postal" in col_lower or "cp" in col_lower or "postal" in col_lower:
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9]", ""))
        elif "nom" in col_lower or "prenom" in col_lower or "ville" in col_lower:
            df = df.withColumn(
                column,
                when(col(column).isNotNull(), 
                     regexp_replace(upper(col(column)), r"\s+", " "))
                .otherwise(None)
            )
    
    # AJOUT: Normalisation des dates
    df = normalize_dates(df)
    
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

def add_surrogate_key(df, sk_columns, output_table_name):
    """Ajoute une clé de substitution (SK) basée sur les colonnes métier."""
    if not sk_columns:
        print(f"  ⚠️ Aucune colonne SK définie pour {output_table_name}")
        return df
    
    available_sk_cols = [c for c in sk_columns if c in df.columns]
    
    if not available_sk_cols:
        print(f"  ⚠️ Aucune colonne SK disponible pour {output_table_name}")
        return df
    
    df = df.withColumn(
        "sk_id",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in available_sk_cols]), 256)
    )
    
    print(f"  🔑 Clé SK créée à partir de : {', '.join(available_sk_cols)}")
    return df

def add_technical_columns(df, source_name, output_table_name):
    """Ajoute les colonnes techniques."""
    generate_uuid_udf = udf(lambda: str(uuid_lib.uuid4()), StringType())
    
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_source", lit(source_name))
    df = df.withColumn("_table_name", lit(output_table_name))
    df = df.withColumn("_record_uuid", generate_uuid_udf())
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    non_tech_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "_hash_record",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in non_tech_cols]), 256)
    )
    
    return df

def get_data_quality_stats(df, output_table_name):
    """Calcule et affiche des statistiques de qualité des données."""
    total_rows = df.count()
    total_cols = len([c for c in df.columns if not c.startswith("_")])
    
    print(f"\n  📊 Statistiques de qualité pour {output_table_name}:")
    print(f"     - Nombre total de lignes : {total_rows}")
    print(f"     - Nombre de colonnes métier : {total_cols}")
    
    for column in [c for c in df.columns if not c.startswith("_")]:
        null_count = df.filter(col(column).isNull()).count()
        fill_rate = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
        if fill_rate < 100:
            print(f"     - {column}: {fill_rate:.1f}% rempli ({null_count} nulls)")

def process_source(spark, config):
    """Fonction générique pour traiter une source et l'écrire dans MinIO."""
    source_type = config["type"]
    source_path = config["path"]
    output_table_name = config["output_table"]
    
    print(f"\n{'='*80}")
    print(f"🚀 Début du traitement : {output_table_name}")
    print(f"{'='*80}")
    print(f"Type: {source_type} | Source: {config['source_name']}")

    try:
        # ========================================
        # 1. LECTURE DE LA SOURCE
        # ========================================
        print(f"\n📥 Étape 1: Lecture de la source...")
        
        if source_type == "csv":
            if not source_path.startswith("file:///") or os.path.exists(source_path.replace("file://", "")):
                print(f"   ✓ Fichier trouvé: {source_path}")
            else:
                raise FileNotFoundError(f"Fichier non trouvé: {source_path}")

            reader = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("delimiter", config.get("delimiter", ",")) \
                .option("encoding", "UTF-8")
            
            if "decimal" in config:
                reader = reader.option("decimal", config["decimal"])
            
            df = reader.csv(source_path)

        elif source_type == "excel":
            print(f"   ⚠️ Lecture Excel désactivée")
            return
        elif source_type == "postgres":
            print(f"   ⏳ Connexion à PostgreSQL...")
            df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://bigdata_postgres:5432/healthcare_data") \
                .option("dbtable", source_path) \
                .option("user", "admin") \
                .option("password", "admin123") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            print(f"   ✓ Données chargées depuis PostgreSQL")
        else:
            raise ValueError(f"Type de source non supporté : {source_type}")

        initial_count = df.count()
        print(f"   ✓ {initial_count} lignes lues")

        # ========================================
        # 2. NETTOYAGE DES NOMS DE COLONNES
        # ========================================
        print(f"\n🧹 Étape 2: Nettoyage des noms de colonnes...")
        df = clean_col_names(df)
        print(f"   ✓ Colonnes standardisées: {', '.join(df.columns[:5])}...")

        # ========================================
        # 3. NETTOYAGE DES DONNÉES
        # ========================================
        print(f"\n🧼 Étape 3: Nettoyage des données...")
        df = clean_data(df)
        before_empty_clean = df.count()
        df = remove_empty_rows(df, threshold=0.3)
        after_empty_clean = df.count()
        removed_empty = before_empty_clean - after_empty_clean
        if removed_empty > 0:
            print(f"   ✓ {removed_empty} lignes vides supprimées")

        # ========================================
        # 4. NORMALISATION (AVEC DATES)
        # ========================================
        print(f"\n📐 Étape 4: Normalisation des données...")
        df = normalize_data(df)
        print(f"   ✓ Données normalisées (incluant les dates)")

        # ========================================
        # 5. AJOUT DES COLONNES TECHNIQUES
        # ========================================
        print(f"\n⚙️ Étape 5: Ajout des colonnes techniques...")
        df = add_technical_columns(df, config["source_name"], output_table_name)
        print(f"   ✓ Colonnes techniques ajoutées")

        # ========================================
        # 6. ANONYMISATION DES PII
        # ========================================
        print(f"\n🔐 Étape 6: Anonymisation des données sensibles...")
        pii_columns = config.get("pii_columns", [])
        if pii_columns:
            df = anonymize_pii(df, pii_columns)
        else:
            print(f"   ℹ️ Aucune colonne PII à anonymiser")

        # ========================================
        # 7. AJOUT DE LA CLÉ DE SUBSTITUTION
        # ========================================
        print(f"\n🔑 Étape 7: Création de la clé de substitution...")
        sk_columns = config.get("sk_columns", [])
        df = add_surrogate_key(df, sk_columns, output_table_name)

        # ========================================
        # 8. SUPPRESSION DES DOUBLONS
        # ========================================
        print(f"\n🗑️ Étape 8: Suppression des doublons...")
        before_dedup = df.count()
        df = remove_duplicates(df, sk_columns)
        after_dedup = df.count()
        duplicates_removed = before_dedup - after_dedup
        if duplicates_removed > 0:
            print(f"   ✓ {duplicates_removed} doublons supprimés")
        else:
            print(f"   ✓ Aucun doublon trouvé")

        # ========================================
        # 9. STATISTIQUES DE QUALITÉ
        # ========================================
        get_data_quality_stats(df, output_table_name)

        # ========================================
        # 10. ÉCRITURE DANS MINIO (OBLIGATOIRE)
        # ========================================
        print(f"\n💾 Étape 10: Écriture dans MinIO (obligatoire)...")
        minio_success, final_count = write_to_minio_direct(df, output_table_name)
        
        # ========================================
        # 11. AFFICHAGE DU SCHÉMA FINAL
        # ========================================
        print(f"\n📋 Schéma final:")
        df.printSchema()
        
        # ========================================
        # RÉSUMÉ
        # ========================================
        print(f"\n✅ Traitement terminé avec succès!")
        print(f"   - Lignes initiales : {initial_count}")
        print(f"   - Lignes après nettoyage : {after_empty_clean}")
        print(f"   - Lignes après dédoublonnage : {final_count}")
        print(f"   - Taux de rétention : {(final_count/initial_count*100):.1f}%")
        print(f"   - ✅ Données écrites dans MinIO avec succès")

    except Exception as e:
        print(f"\n❌ ERREUR lors du traitement")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        raise
    
    print(f"\n{'='*80}\n")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  BRONZE INGESTION PIPELINE - MinIO (ÉCRITURE OBLIGATOIRE)    ║
    ║  Pipeline de nettoyage, normalisation et anonymisation       ║
    ║  ✨ AVEC NORMALISATION DES DATES ✨                         ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        # Test de connexion
        print("🔍 Test de connexion à MinIO...")
        if not test_minio_connection(spark):
            print("💥 Impossible de se connecter à MinIO. Arrêt du pipeline.")
            sys.exit(1)
        
        print("🎯 Test MinIO réussi! Début du traitement des données...")
        
        # Configuration des sources
        source_configs = [
            {"type": "csv", "source_name": "activite_professionnel_sante.csv", "path": "file:///data/source/csv/activite_professionnel_sante.csv", "delimiter": ";", "output_table": "activite_professionnel_sante", "pii_columns": ["identifiant"], "sk_columns": ["identifiant", "identifiant_organisation"]},
        {"type": "csv", "source_name": "etablissement_sante.csv", "path": "file:///data/source/csv/etablissement_sante.csv", "delimiter": ";", "output_table": "etablissement_sante", "pii_columns": ["email", "telephone", "telephone_2", "siret_site"], "sk_columns": ["finess_site"]},
        {"type": "csv", "source_name": "professionnel_sante.csv", "path": "file:///data/source/csv/professionnel_sante.csv", "delimiter": ";", "output_table": "professionnel_sante", "pii_columns": ["nom", "prenom"], "sk_columns": ["identifiant"]},
        {"type": "csv", "source_name": "Hospitalisations.csv", "path": "file:///data/source/csv/Hospitalisations.csv", "delimiter": ";", "output_table": "hospitalisations", "pii_columns": ["id_patient"], "sk_columns": ["num_hospitalisation"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_es.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_es.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_participant.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_participant.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_participant", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_lexique.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_lexique.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_lexique", "pii_columns": [], "sk_columns": ["NAME"]},
        {"type": "csv", "source_name": "RCP_MCO_recueil2014_donnee2013_table_es.csv", "path": "file:///data/source/csv/RCP_MCO_recueil2014_donnee2013_table_es.csv", "delimiter": ";", "output_table": "satisfaction_2013_rcp_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "RCP_MCO_recueil2014_donnee2013_table_participant.csv", "path": "file:///data/source/csv/RCP_MCO_recueil2014_donnee2013_table_participant.csv", "delimiter": ";", "output_table": "satisfaction_2013_rcp_mco_participant", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "hpp_mco_recueil2015_donnee2014_tables_es.csv", "path": "file:///data/source/csv/hpp_mco_recueil2015_donnee2014_tables_es.csv", "delimiter": ";", "output_table": "satisfaction_2014_hpp_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "idm_mco_recueil2015_donnee2014_tables_es.csv", "path": "file:///data/source/csv/idm_mco_recueil2015_donnee2014_tables_es.csv", "delimiter": ";", "output_table": "satisfaction_2014_idm_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dan_mco_recueil2016_donnee2015_donnees.csv", "path": "file:///data/source/csv/dan_mco_recueil2016_donnee2015_donnees.csv", "delimiter": ",", "output_table": "satisfaction_2015_dan_mco", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dpa_had_recueil2016_donnee2015_donnees.csv", "path": "file:///data/source/csv/dpa_had_recueil2016_donnee2015_donnees.csv", "delimiter": ",", "output_table": "satisfaction_2015_dpa_had", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dpa-ssr-recueil2018-donnee2017-donnees.csv", "path": "file:///data/source/csv/dpa-ssr-recueil2018-donnee2017-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_dpa_ssr", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "ete-ortho-ipaqss-2017-2018-donnees.csv", "path": "file:///data/source/csv/ete-ortho-ipaqss-2017-2018-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_2018_ete_ortho", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "rcp-mco-recueil2018-donnee2017-donnees.csv", "path": "file:///data/source/csv/rcp-mco-recueil2018-donnee2017-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_rcp_mco", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "ESATIS48H_MCO_recueil2017_donnees.csv", "path": "file:///data/source/csv/ESATIS48H_MCO_recueil2017_donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-esatis48h-mco-open-data-2019.csv", "path": "file:///data/source/csv/resultats-esatis48h-mco-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-esatisca-mco-open-data-2019.csv", "path": "file:///data/source/csv/resultats-esatisca-mco-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_esatisca", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-iqss-open-data-2019.csv", "path": "file:///data/source/csv/resultats-iqss-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_iqss", "pii_columns": [], "sk_columns": ["finess"]},
        ]

        # Traitement des sources
        successful_tables = []
        
        for config in source_configs:
            try:
                process_source(spark, config)
                successful_tables.append(config["output_table"])
            except Exception as e:
                print(f"💥 Échec critique pour {config['output_table']}: {e}")
                print("💥 Arrêt du pipeline en raison de l'échec d'écriture MinIO")
                spark.stop()
                sys.exit(1)

        spark.stop()
        
        # Résumé final
        print("\n" + "="*80)
        print("🎉 PIPELINE BRONZE TERMINÉ AVEC SUCCÈS!")
        print("="*80)
        print(f"✅ Tables traitées et écrites dans MinIO: {len(successful_tables)}")
        for table in successful_tables:
            print(f"   - {table}")
        print(f"\n📊 Toutes les données sont maintenant dans MinIO: s3a://bronze/")
        print("="*80)
        
    except Exception as e:
        print(f"💥 Erreur critique du pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)