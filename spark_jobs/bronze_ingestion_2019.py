import os
import sys
import uuid as uuid_lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, lower, regexp_replace, when, 
    coalesce, udf, to_date, year, month,
    datediff, floor, substring, length, md5, date_format
)
from pyspark.sql.types import StringType, DateType, TimestampType
import re

# Configuration centralisée
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://172.18.0.2:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "bucket": "bronze"
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "chu_postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "healthcare_data"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin123")
}

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# CONFIGURATION POUR MACHINES LIMITÉES (WSL)
LOW_RESOURCE_MODE = True

def get_spark_session():
    """Session Spark optimisée avec configuration pour les dates."""
    try:
        jars_dir = "/home/jovyan/jars"
        jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
        jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
        
        builder = SparkSession.builder \
            .appName("Bronze Pipeline") \
            .config("spark.jars", jars_path) \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  # Pour formats dates M/d/yyyy
        
        if LOW_RESOURCE_MODE:
            builder = builder \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.executor.cores", "2") \
                .config("spark.sql.shuffle.partitions", "8")
        else:
            builder = builder \
                .config("spark.driver.memory", "6g") \
                .config("spark.executor.memory", "8g") \
                .config("spark.executor.cores", "4") \
                .config("spark.sql.shuffle.partitions", "32")
        
        # Configuration S3A
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.attempts.maximum", "1")
        hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
        hadoop_conf.set("fs.s3a.connection.timeout", "10000")
        
        print("✅ Spark initialisé")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def test_connections(spark):
    """Test rapide des connexions."""
    try:
        print("🔍 Test MinIO...")
        test_df = spark.createDataFrame([(1, "test")], ["id", "data"])
        test_path = f"s3a://{MINIO_CONFIG['bucket']}/test/data"
        test_df.write.mode("overwrite").parquet(test_path)
        print("✅ MinIO OK")
        
        print("🔍 Test PostgreSQL...")
        spark.read.format("jdbc") \
            .option("url", POSTGRES_JDBC_URL) \
            .option("dbtable", "(SELECT 1) as t") \
            .option("user", POSTGRES_CONFIG["user"]) \
            .option("password", POSTGRES_CONFIG["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load().count()
        print("✅ PostgreSQL OK")
        return True
    except Exception as e:
        print(f"❌ Test échoué: {e}")
        return False

def read_postgres_table_safe(spark, table_name, config):
    """Lit une table PostgreSQL de manière sécurisée."""
    table_clean = table_name.replace('"', '')
    
    # FILTRAGE SPÉCIAL POUR DECES: uniquement 2019
    if "deces" in table_name.lower():
        base_queries = [
            f"(SELECT * FROM {table_name} WHERE EXTRACT(YEAR FROM date_deces) = 2019) as filtered",
            f"(SELECT * FROM {table_clean} WHERE EXTRACT(YEAR FROM date_deces) = 2019) as filtered"
        ]
    else:
        base_queries = [
            f"(SELECT * FROM {table_name}) as data",
            f"(SELECT * FROM {table_clean}) as data"
        ]
    
    for query in base_queries:
        try:
            jdbc_options = {
                "url": POSTGRES_JDBC_URL,
                "dbtable": query,
                "user": POSTGRES_CONFIG["user"],
                "password": POSTGRES_CONFIG["password"],
                "driver": "org.postgresql.Driver",
                "fetchsize": "10000"
            }
            
            df = spark.read.format("jdbc").options(**jdbc_options).load()
            print(f"✅ Lecture {table_name}: {df.count()} lignes")
            return df
            
        except Exception as e:
            print(f"❌ Tentative échouée pour {query}: {str(e)}")
            continue
    
    raise Exception(f"Impossible de lire la table {table_name}")

def clean_col_names(df):
    """Nettoie les noms de colonnes."""
    return df.toDF(*[re.sub(r'[^a-zA-Z0-9]', '_', c).strip('_') for c in df.columns])

def normalize_dates(df):
    """Normalise les colonnes de dates avec support des formats variés et correction TIME."""
    date_columns = []
    time_columns = []
    
    for column in df.columns:
        col_lower = column.lower()
        # Identifier les colonnes TIME (heures uniquement)
        if any(keyword in col_lower for keyword in ["heure", "time"]) and not any(keyword in col_lower for keyword in ["date", "timestamp"]):
            time_columns.append(column)
        # Identifier les colonnes DATE
        elif any(keyword in col_lower for keyword in ["date", "naissance", "deces", "entree", "sortie", "consultation", "admission"]):
            date_columns.append(column)
    
    # Traiter les colonnes TIME : convertir timestamp en string time (HH:mm:ss)
    for column in time_columns:
        if isinstance(df.schema[column].dataType, TimestampType):
            # Extraire uniquement l'heure au format HH:mm:ss
            df = df.withColumn(column, date_format(col(column), "HH:mm:ss"))
    
    # Traiter les colonnes DATE
    for column in date_columns:
        if not isinstance(df.schema[column].dataType, DateType):
            df = df.withColumn(
                column,
                coalesce(
                    to_date(col(column), "yyyy-MM-dd"),
                    to_date(col(column), "dd/MM/yyyy"),
                    to_date(col(column), "MM/dd/yyyy"),
                    to_date(col(column), "M/d/yyyy"),
                    to_date(col(column), "yyyy/MM/dd"),
                    to_date(col(column), "dd-MM-yyyy"),
                    to_date(col(column), "MM-dd-yyyy"),
                    to_date(col(column), "M-d-yyyy")
                )
            )
    
    return df

def normalize_data(df, config):
    """Normalise les données avec préservation dimensions analytiques."""
    # Standardisation des noms de colonnes
    column_mappings = {
        "id_patient": ["Id_patient"],
        "nom": ["Nom"],
        "prenom": ["Prenom"], 
        "sexe": ["Sexe", "Civilite"],
        "date_naissance": ["Date"],
        "date_consultation": ["Date"],
        "date_deces": ["date_deces"],
        "code_diag": ["Code_diag", "Code_diagnostic", "Code_diag"],
        "code_postal": ["Code_postal"],
        "region": ["region", "libelle_region", "Libelle_region", "Lib_reg"],
        "departement": ["departement", "code_departement"],
        "finess": ["finess", "Finess"],
        "identifiant_organisation": ["identifiant_organisation", "finess_geo", "finess_pmsi"]
    }
    
    for standard_name, variants in column_mappings.items():
        for variant in variants:
            if variant in df.columns and standard_name not in df.columns:
                df = df.withColumnRenamed(variant, standard_name)
    
    # Normalisation par type
    for column in df.columns:
        col_lower = column.lower()
        col_type = df.schema[column].dataType

        if "email" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, lower(trim(col(column))))
        elif ("tel" in col_lower or "phone" in col_lower) and isinstance(col_type, StringType):
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9+]", ""))
        elif "code_postal" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9A-Z]", ""))
        elif ("sexe" in col_lower or "civilite" in col_lower) and isinstance(col_type, StringType):
            df = df.withColumn(
                column,
                when(upper(trim(col(column))).isin(["M", "MALE", "HOMME", "H", "1", "MONSIEUR"]), "M")
                .when(upper(trim(col(column))).isin(["F", "FEMALE", "FEMME", "W", "2", "MADAME"]), "F")
                .otherwise(upper(trim(col(column))))
            )
        elif "finess" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9]", ""))
    
    # Normalisation dates
    df = normalize_dates(df)
    
    return df

def generate_surrogate_keys(df, config):
    """Génère les clés de substitution (SK) pour les dimensions."""
    source_name = config["source_name"]
    output_table = config["output_table"]
    
    # Clé de substitution principale pour la table
    df = df.withColumn("_sk", sha2(concat_ws("_", lit(output_table), md5(concat_ws("|", *df.columns))), 256))
    
    # Clés de substitution pour les relations dimensionnelles basées sur les objectifs métier
    if "id_patient" in df.columns:
        df = df.withColumn("_sk_patient", sha2(col("id_patient").cast("string"), 256))
    
    if "identifiant" in df.columns and "professionnel" in output_table.lower():
        df = df.withColumn("_sk_professionnel", sha2(col("identifiant").cast("string"), 256))
    
    if "id_prof_sante" in df.columns:
        df = df.withColumn("_sk_prof_sante", sha2(col("id_prof_sante").cast("string"), 256))
    
    if "code_diag" in df.columns:
        df = df.withColumn("_sk_diagnostic", sha2(col("code_diag").cast("string"), 256))
    
    if "identifiant_organisation" in df.columns or "finess" in df.columns:
        # Clé pour les établissements (pour analyses par établissement)
        finess_col = "identifiant_organisation" if "identifiant_organisation" in df.columns else "finess"
        df = df.withColumn("_sk_etablissement", sha2(col(finess_col).cast("string"), 256))
    
    if "region" in df.columns:
        df = df.withColumn("_sk_region", sha2(upper(trim(col("region"))).cast("string"), 256))
    
    if "code_postal" in df.columns:
        df = df.withColumn("_sk_geographie", sha2(col("code_postal").cast("string"), 256))
    
    if "id_mut" in df.columns:
        df = df.withColumn("_sk_mutuelle", sha2(col("id_mut").cast("string"), 256))
    
    if "code_cis" in df.columns:
        df = df.withColumn("_sk_medicament", sha2(col("code_cis").cast("string"), 256))
    
    return df

def add_technical_columns(df, config):
    """Ajoute les colonnes techniques pour le tracking."""
    source_name = config["source_name"]
    output_table = config["output_table"]
    
    # Hash du record complet pour détection de changements
    df = df.withColumn("_hash_record", sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256))
    
    # Métadonnées d'ingestion
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_source_system", lit(source_name))
    df = df.withColumn("_source_table", lit(output_table))
    df = df.withColumn("_batch_id", lit(str(uuid_lib.uuid4())))
    
    # Version et flags pour historisation
    df = df.withColumn("_version", lit(1))
    df = df.withColumn("_is_current", lit(True))
    df = df.withColumn("_is_deleted", lit(False))
    
    return df

def anonymize_pii_analytical(df, config):
    """Anonymise les données sensibles tout en préservant les dimensions analytiques."""
    pii_columns = config.get("pii_columns", [])
    
    for pii_col in pii_columns:
        if pii_col in df.columns:
            col_lower = pii_col.lower()
            
            if any(keyword in col_lower for keyword in ["nom", "prenom"]):
                # Anonymiser mais garder l'initiale pour analyses démographiques
                df = df.withColumn(
                    pii_col + "_anonymized",
                    when(col(pii_col).isNotNull(), sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
                # Garder l'initiale du prénom pour analyses
                if "prenom" in col_lower:
                    df = df.withColumn(
                        "initiale_prenom",
                        when(col(pii_col).isNotNull(), upper(substring(trim(col(pii_col)), 1, 1)))
                        .otherwise(None)
                    )
                    
            elif any(keyword in col_lower for keyword in ["ville", "adresse", "code_postal"]):
                # Anonymiser mais extraire le département pour analyses géographiques
                df = df.withColumn(
                    pii_col + "_anonymized",
                    when(col(pii_col).isNotNull(), sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
                # Extraire le département si code postal
                if "code_postal" in col_lower and "departement" not in df.columns:
                    df = df.withColumn(
                        "departement",
                        when(length(col(pii_col)) >= 2, substring(col(pii_col), 1, 2))
                        .otherwise(None)
                    )
                    
            elif any(keyword in col_lower for keyword in ["email", "tel", "telephone", "num_secu"]):
                # Anonymisation complète
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
            else:
                # Anonymisation standard
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
    
    return df

def process_dataframe(df, config, source_type):
    """Traite un DataFrame avec normalisation et anonymisation."""
    # 1. Nettoyage colonnes
    df = clean_col_names(df)
    
    # 2. Nettoyage données basique
    for column in df.columns:
        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column, 
                when(trim(col(column)).isin(["NULL", "NA", "", "-", "nan"]), lit(None))
                .otherwise(trim(col(column))))
    
    # 3. Normalisation des données
    df = normalize_data(df, config)
    
    # 4. Anonymisation ciblée
    df = anonymize_pii_analytical(df, config)
    
    # 5. Clés de substitution
    df = generate_surrogate_keys(df, config)
    
    # 6. Colonnes techniques
    df = add_technical_columns(df, config)
    
    return df

def write_to_minio(df, output_table):
    """Écrit les données dans MinIO."""
    bronze_path = f"s3a://{MINIO_CONFIG['bucket']}/{output_table}"
    
    # Optimisation partitions
    df = df.coalesce(2)
    
    # Nettoyage caractères de contrôle
    for column in df.columns:
        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column,
                when(col(column).isNotNull(), 
                     regexp_replace(col(column), r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "")))
    
    # Réorganisation des colonnes: techniques en premier
    technical_cols = [c for c in df.columns if c.startswith('_')]
    business_cols = [c for c in df.columns if not c.startswith('_')]
    df = df.select(*technical_cols, *business_cols)
    
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .option("maxRecordsPerFile", "500000") \
        .parquet(bronze_path)
    
    return df.count()

def process_csv_source(spark, config):
    """Traite une source CSV."""
    source_path = config["path"]
    output_table = config["output_table"]
    
    try:
        # Lecture CSV avec options adaptées
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", config.get("delimiter", ";")) \
            .option("encoding", config.get("encoding", "utf-8")) \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        initial_count = df.count()
        print(f"📥 CSV {output_table}: {initial_count} lignes")
        
        # Traitement
        df_processed = process_dataframe(df, config, "csv")
        
        # Écriture
        written_count = write_to_minio(df_processed, output_table)
        
        print(f"✅ CSV {output_table} traité: {written_count} lignes")
        return True
        
    except Exception as e:
        print(f"❌ Erreur CSV {output_table}: {e}")
        return False

def process_postgres_source(spark, config):
    """Traite une source PostgreSQL."""
    source_path = config["path"]
    output_table = config["output_table"]

    try:
        # Lecture sécurisée de la table
        df = read_postgres_table_safe(spark, source_path, config)
        initial_count = df.count()
        
        # Traitement
        df_processed = process_dataframe(df, config, "postgres")
        
        # Écriture
        written_count = write_to_minio(df_processed, output_table)
        
        print(f"✅ PostgreSQL {output_table} traité: {written_count} lignes")
        return True
        
    except Exception as e:
        print(f"❌ Erreur PostgreSQL {output_table}: {e}")
        return False

def process_source(spark, config):
    """Route vers le bon traitement selon le type de source."""
    source_type = config["type"]
    
    print(f"\n🎯 TRAITEMENT: {config['output_table']}")
    
    if source_type == "csv":
        return process_csv_source(spark, config)
    elif source_type == "postgres":
        return process_postgres_source(spark, config)
    else:
        print(f"❌ Type non supporté: {source_type}")
        return False

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════╗
    ║        PIPELINE BRONZE - MINIO       ║
    ║          DÉCÈS 2019 UNIQUEMENT       ║
    ╚══════════════════════════════════════╝
    """)
    
    print(f"⚙️  Configuration:")
    print(f"   - Mode ressources limitées: {LOW_RESOURCE_MODE}")
    print(f"   - Filtre Décès 2019: ✅ ACTIVÉ")
    print(f"   - Clés de substitution: ACTIVÉES")
    print(f"   - Colonnes techniques: ACTIVÉES")
    print(f"   - Parser dates legacy: ACTIVÉ (pour formats M/d/yyyy)")
    
    try:
        spark = get_spark_session()
        
        if not test_connections(spark):
            print("💥 Erreur connexion")
            sys.exit(1)
        
        # CONFIGURATION COMPLÈTE POUR TOUTES LES DONNÉES
        source_configs = [
            # === SOURCES POSTGRESQL - DONNÉES MÉTIER PRINCIPALES ===
            {
                "type": "postgres",
                "source_name": "Deces_2019",
                "path": "\"deces\"",
                "output_table": "deces",
                "pii_columns": ["nom", "prenom", "adresse", "ville"],
                "preserve_columns": ["sexe", "date_naissance", "date_deces", "code_postal", "region", "departement"]
            },
            {
                "type": "postgres", 
                "source_name": "Patients",
                "path": "\"Patient\"",
                "output_table": "patients",
                "pii_columns": ["Nom", "Prenom", "EMail", "Tel", "Num_Secu", "Adresse", "Ville"],
                "preserve_columns": ["Sexe", "Date", "Code_postal", "Age"]
            },
            {
                "type": "postgres",
                "source_name": "Consultations",
                "path": "\"Consultation\"",
                "output_table": "consultations",
                "pii_columns": [],
                "preserve_columns": ["Date", "Code_diag", "Id_prof_sante", "Id_patient", "Id_mut"]
            },
            {
                "type": "postgres",
                "source_name": "Diagnostics",
                "path": "\"Diagnostic\"",
                "output_table": "diagnostics",
                "pii_columns": [],
                "preserve_columns": ["Code_diag", "Diagnostic"]
            },
            {
                "type": "postgres",
                "source_name": "Professionnels_Sante",
                "path": "\"Professionnel_de_sante\"",
                "output_table": "professionnels_sante",
                "pii_columns": ["Nom", "Prenom"],
                "preserve_columns": ["Civilite", "Profession", "Code_specialite", "Identifiant"]
            },
            {
                "type": "csv",
                "source_name": "Etablissements",
                "path": "file:///data/source/csv/etablissement_sante.csv",
                "output_table": "etablissements",
                "encoding": "utf-8",
                "delimiter": ";",
                "pii_columns": ["email", "telephone", "telephone_2", "adresse"],
                "preserve_columns": ["region", "departement", "code_postal", "ville", "finess_site", "identifiant_organisation"]
            },
            {
                "type": "csv",
                "source_name": "Hospitalisations_CSV",
                "path": "file:///data/source/csv/Hospitalisations.csv",
                "output_table": "hospitalisations",
                "encoding": "ascii",
                "delimiter": ";",
                "pii_columns": ["nom_patient", "prenom_patient"],
                "preserve_columns": ["sexe", "date_naissance", "region", "diagnostic_principal", "date_admission", "date_sortie", "identifiant_organisation", "Id_patient"]
            },
            {
                "type": "csv",
                "source_name": "Satisfaction_MCO_2019",
                "path": "file:///data/source/csv/resultats-esatisca-mco-open-data-2019.csv",
                "output_table": "satisfaction_mco_2019",
                "encoding": "Windows-1252",
                "delimiter": ";",
                "pii_columns": [],
                "preserve_columns": ["finess", "region", "score_all_ajust", "classement", "evolution"]
            }
        ]

        successful_tables = []
        failed_tables = []
        
        for config in source_configs:
            try:
                success = process_source(spark, config)
                if success:
                    successful_tables.append(config["output_table"])
                else:
                    failed_tables.append(config["output_table"])
            except Exception as e:
                print(f"💥 Échec {config['output_table']}: {e}")
                failed_tables.append(config["output_table"])
        
        spark.stop()
        
        # RAPPORT FINAL
        print(f"\n🎉 PIPELINE BRONZE TERMINÉ")
        print(f"✅ Succès: {len(successful_tables)} tables")
        print(f"❌ Échecs: {len(failed_tables)} tables")
        print(f"💾 Données dans MinIO: s3a://{MINIO_CONFIG['bucket']}/")
        print(f"🔑 Clés de substitution générées pour toutes les tables")
        print(f"📊 Colonnes techniques ajoutées (_ingestion_date, _hash_record, etc.)")
        
        # RAPPORT DES TABLES TRAITÉES
        print(f"\n📋 TABLES TRAITÉES:")
        for table in successful_tables:
            print(f"   ✅ {table}")
        if failed_tables:
            print(f"\n⚠️  TABLES EN ÉCHEC:")
            for table in failed_tables:
                print(f"   ❌ {table}")
                
    except Exception as e:
        print(f"💥 Erreur: {e}")
        sys.exit(1)
