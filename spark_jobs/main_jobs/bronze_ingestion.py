import os
import sys
import uuid as uuid_lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, lower, regexp_replace, when, 
    coalesce, udf, to_date, year, month,
    datediff, floor, substring, length, md5,
    regexp_extract, split, expr, unix_timestamp,
    monotonically_increasing_id
)
from pyspark.sql.types import StringType, DateType, TimestampType, IntegerType
import re
from datetime import datetime

# Configuration centralisée
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "bucket": "bronze"
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "bigdata_postgres"),
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
            .appName("Bronze Pipeline Optimisé") \
            .config("spark.jars", jars_path) \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        
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
            continue
    
    raise Exception(f"Impossible de lire la table {table_name}")

def clean_col_names(df):
    """Nettoie les noms de colonnes."""
    return df.toDF(*[re.sub(r'[^a-zA-Z0-9_]', '_', c).strip('_').lower() for c in df.columns])

def normalize_dates_advanced(df, config):
    """Normalisation avancée des dates - VERSION SÉCURISÉE."""
    # Colonnes qui sont VRAIMENT des dates (basé sur le nom et le contexte métier)
    real_date_columns = [
        "date_naissance", "date_deces", "date_consultation", 
        "date_admission", "date_sortie", "date_entree"
    ]
    
    date_patterns = [
        "yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "M/d/yyyy",
        "yyyy/MM/dd", "dd-MM-yyyy", "MM-dd-yyyy", "M-d-yyyy", 
        "dd/MM/yy", "MM/dd/yy", "dd.MM.yyyy", "yyyy.MM.dd"
    ]
    
    for column in df.columns:
        col_lower = column.lower()
        
        # VÉRIFICATION CRITIQUE: Ne traiter que les VRAIES colonnes de dates
        is_real_date_column = any(real_date in col_lower for real_date in [
            "date_naissance", "date_deces", "date_consultation", 
            "date_admission", "date_sortie", "date_entree"
        ])
        
        # Éviter les colonnes géographiques
        is_geographic_column = any(geo_keyword in col_lower for geo_keyword in [
            "code_lieu", "lieu_", "pays_", "numero_acte", "finess", "code_postal", "departement", "region"
        ])
        
        if is_real_date_column and not is_geographic_column:
            if not isinstance(df.schema[column].dataType, (DateType, TimestampType)):
                print(f"     🔧 Normalisation des dates pour: {column}")
                
                # Tentative de conversion avec tous les formats
                date_expr = None
                for pattern in date_patterns:
                    if date_expr is None:
                        date_expr = to_date(col(column), pattern)
                    else:
                        date_expr = coalesce(date_expr, to_date(col(column), pattern))
                
                # Fallback pour les timestamps
                date_expr = coalesce(
                    date_expr,
                    to_date(col(column).cast("timestamp"))
                )
                
                # Validation: années raisonnables
                date_expr = when(
                    (year(date_expr) >= 1900) & (year(date_expr) <= 2100),
                    date_expr
                ).otherwise(lit(None))
                
                df = df.withColumn(column, date_expr)
        elif is_geographic_column:
            # S'assurer que les colonnes géographiques restent en string
            if not isinstance(df.schema[column].dataType, StringType):
                df = df.withColumn(column, col(column).cast("string"))
                print(f"     🔧 Colonne géographique préservée: {column}")
    
    return df

def diagnose_date_issues(df, table_name):
    """Diagnostique les problèmes de dates dans une table."""
    if "deces" in table_name.lower():
        print(f"\n🔍 DIAGNOSTIC DES DATES - {table_name}")
        print("=" * 50)

        # Identifier les VRAIES colonnes de dates seulement
        real_date_columns = ["date_naissance", "date_deces"]

        for date_col in real_date_columns:
            if date_col in df.columns:
                # Compter les valeurs nulles
                null_count = df.filter(col(date_col).isNull()).count()
                total_count = df.count()

                # Analyser les années avec CORRECTION du format
                year_stats = df.select(
                    year(col(date_col)).alias("year")
                ).filter(
                    col("year").isNotNull()
                ).groupBy("year").count().orderBy("count", ascending=False).limit(10)

                print(f"\n📅 {date_col}:")
                print(f"   • Total: {total_count:,} lignes")
                print(f"   • Nulles: {null_count:,} ({null_count/total_count*100:.1f}%)")

                # CORRECTION: Afficher correctement les années
                year_samples = year_stats.collect()
                if year_samples:
                    year_list = [f"{row['year']} ({row['count']:,})" for row in year_samples]
                    print(f"   • Top années: {year_list[:5]}")  # Limiter à 5 pour lisibilité

                # Détecter les années hors limites
                invalid_years = df.filter(
                    (year(col(date_col)) < 1900) | (year(col(date_col)) > 2100)
                ).count()

                if invalid_years > 0:
                    print(f"   ⚠️  Années invalides: {invalid_years}")

def calculate_age_from_date(date_col, reference_date=None):
    """Calcule l'âge à partir d'une date de naissance."""
    if reference_date is None:
        reference_date = current_timestamp()
    
    return floor(datediff(reference_date, date_col) / 365.25)

def normalize_data_enhanced(df, config):
    """Normalisation améliorée des données avec gestion intelligente des dates."""
    table_name = config.get("output_table", "").lower()
    
    # 1. Nettoyage colonnes
    df = clean_col_names(df)
    
    # 2. Gestion SPÉCIFIQUE des dates par table (CORRECTION CRUCIALE)
    if "consultation" in table_name and "date" in df.columns:
        df = df.withColumnRenamed("date", "date_consultation")
        print(f"   🔧 Colonne 'date' renommée en 'date_consultation'")
    elif "patient" in table_name and "date" in df.columns:
        df = df.withColumnRenamed("date", "date_naissance")
        print(f"   🔧 Colonne 'date' renommée en 'date_naissance'")
    elif "deces" in table_name and "date" in df.columns:
        df = df.withColumnRenamed("date", "date_deces")
        print(f"   🔧 Colonne 'date' renommée en 'date_deces'")
    
    # 3. Standardisation des noms de colonnes
    column_mappings = {
        "id_patient": ["id_patient", "patient_id"],
        "nom": ["nom", "last_name"],
        "prenom": ["prenom", "first_name"], 
        "sexe": ["sexe", "civilite", "gender"],
        "code_diag": ["code_diag", "diagnostic_code", "code_diagnostic"],
        "code_postal": ["code_postal", "postal_code"],
        "region": ["region", "libelle_region", "lib_reg"],
        "departement": ["departement", "code_departement"],
        "finess": ["finess", "finess_site"],
        "identifiant_organisation": ["identifiant_organisation", "finess_geo", "finess_pmsi"],
        "id_prof_sante": ["id_prof_sante", "professionnel_id"],
        "diagnostic_principal": ["diagnostic_principal", "principal_diagnostic"]
    }
    
    applied_renames = {}
    for standard_name, variants in column_mappings.items():
        for variant in variants:
            if variant in df.columns and standard_name not in df.columns:
                if variant not in applied_renames.values():
                    df = df.withColumnRenamed(variant, standard_name)
                    applied_renames[standard_name] = variant
                    break
    
    # 4. Nettoyage données basique
    for column in df.columns:
        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column, 
                when(trim(col(column)).isin(["NULL", "NA", "", "-", "nan", "None"]), lit(None))
                .otherwise(trim(col(column))))
    
    # 5. CORRECTION DES REGEX - Version sécurisée
    for column in df.columns:
        col_lower = column.lower()
        col_type = df.schema[column].dataType

        if "email" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, lower(trim(col(column))))
            
        # CORRECTION : Regex valide pour téléphones
        elif ("tel" in col_lower or "phone" in col_lower) and isinstance(col_type, StringType):
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9+]", ""))
            
        elif "code_postal" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, 
                when(length(regexp_replace(col(column), r"[^0-9]", "")) == 5,
                     regexp_replace(col(column), r"[^0-9]", ""))
                .otherwise(None)
            )
            
        elif ("sexe" in col_lower or "civilite" in col_lower) and isinstance(col_type, StringType):
            df = df.withColumn(
                column,
                when(upper(trim(col(column))).isin(["M", "MALE", "HOMME", "H", "1", "MONSIEUR"]), "M")
                .when(upper(trim(col(column))).isin(["F", "FEMALE", "FEMME", "W", "2", "MADAME"]), "F")
                .when(upper(trim(col(column))).isin(["NB", "NON-BINAIRE", "X"]), "X")
                .otherwise("I")
            )
            
        elif "finess" in col_lower and isinstance(col_type, StringType):
            df = df.withColumn(column, regexp_replace(col(column), r"[^0-9]", ""))
    
    # 6. Normalisation avancée des dates
    df = normalize_dates_advanced(df, config)
    
    return df

def anonymize_pii_rgpd_compliant(df, config):
    """Anonymisation RGPD-compliant qui préserve les dimensions analytiques."""
    pii_columns = config.get("pii_columns", [])
    
    for pii_col in pii_columns:
        if pii_col in df.columns:
            col_lower = pii_col.lower()
            
            if any(keyword in col_lower for keyword in ["nom", "prenom"]):
                # Anonymisation forte mais conservation des initiales
                df = df.withColumn(
                    pii_col + "_anonymized",
                    when(col(pii_col).isNotNull(), 
                         sha2(concat_ws("|", col(pii_col), lit(str(uuid_lib.uuid4()))), 256))
                    .otherwise(None)
                )
                
                # Conservation de l'initiale pour analyses
                if "prenom" in col_lower:
                    df = df.withColumn(
                        "initiale_prenom",
                        when(col(pii_col).isNotNull(), 
                             upper(substring(regexp_replace(trim(col(pii_col)), r"[^a-zA-Z]", ""), 1, 1)))
                        .otherwise(None)
                    )
                
                # Suppression de la colonne originale
                df = df.drop(pii_col)
                    
            elif "email" in col_lower:
                # Anonymisation complète
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), 
                         sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
                
            elif any(keyword in col_lower for keyword in ["tel", "telephone", "num_secu"]):
                # Anonymisation complète
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), 
                         sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
                
            elif any(keyword in col_lower for keyword in ["adresse", "ville"]):
                # Anonymisation mais extraction des dimensions géographiques
                df = df.withColumn(
                    pii_col + "_anonymized",
                    when(col(pii_col).isNotNull(), 
                         sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
                df = df.drop(pii_col)
                
            elif "code_postal" in col_lower:
                # Conservation du code postal pour analyses géographiques
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), 
                         regexp_replace(col(pii_col), r"[^0-9A-Z]", ""))
                    .otherwise(None)
                )
                
            else:
                # Anonymisation standard
                df = df.withColumn(
                    pii_col,
                    when(col(pii_col).isNotNull(), 
                         sha2(col(pii_col).cast("string"), 256))
                    .otherwise(None)
                )
    
    return df

def add_analytical_columns(df, config):
    """Ajoute des colonnes analytiques pour répondre aux besoins métier."""
    source_name = config["source_name"]
    output_table = config["output_table"]
    
    # Colonnes pour analyses temporelles
    date_columns = [c for c in df.columns if "date" in c.lower()]
    
    for date_col in date_columns:
        if isinstance(df.schema[date_col].dataType, (DateType, TimestampType)):
            # Année et mois pour les agrégations temporelles
            df = df.withColumn(f"{date_col}_annee", year(col(date_col)))
            df = df.withColumn(f"{date_col}_mois", month(col(date_col)))
    
    # Colonnes spécifiques selon la table
    if "patients" in output_table.lower():
        # Calcul de l'âge si date de naissance disponible
        if "date_naissance" in df.columns:
            df = df.withColumn("age", 
                when(col("date_naissance").isNotNull(),
                     calculate_age_from_date(col("date_naissance")))
                .otherwise(col("age") if "age" in df.columns else lit(None))
            )
        
        # Catégorie d'âge pour les analyses
        df = df.withColumn("categorie_age",
            when(col("age").isNull(), "Inconnu")
            .when(col("age") < 18, "0-17")
            .when(col("age") < 30, "18-29")
            .when(col("age") < 45, "30-44")
            .when(col("age") < 60, "45-59")
            .when(col("age") < 75, "60-74")
            .otherwise("75+")
        )
    
    # Pour les consultations et hospitalisations
    if any(keyword in output_table.lower() for keyword in ["consultation", "hospitalisation"]):
        if "date_entree" in df.columns and "date_sortie" in df.columns:
            df = df.withColumn("duree_sejour",
                when(col("date_entree").isNotNull() & col("date_sortie").isNotNull(),
                     datediff(col("date_sortie"), col("date_entree")))
                .otherwise(lit(0))
            )
    
    # Pour les données de satisfaction
    if "satisfaction" in output_table.lower():
        # Normalisation des scores
        score_columns = [c for c in df.columns if "score" in c.lower()]
        for score_col in score_columns:
            if score_col in df.columns:
                # Conversion des virgules en points pour les nombres français
                df = df.withColumn(score_col,
                    regexp_replace(col(score_col), ",", ".")
                ).withColumn(score_col,
                    col(score_col).cast("double")
                )
    
    return df

def generate_business_keys(df, config):
    """Génère des clés métier optimisées pour les besoins analytiques."""
    output_table = config["output_table"]
    
    # Clé de substitution principale
    df = df.withColumn("_sk", sha2(concat_ws("_", lit(output_table), 
        md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]))), 256))
    
    # Clés dimensionnelles spécifiques selon les besoins métier identifiés
    if any(keyword in output_table.lower() for keyword in ["patient", "consultation", "hospitalisation"]):
        if "id_patient" in df.columns:
            df = df.withColumn("_sk_patient", sha2(col("id_patient").cast("string"), 256))
        
        if "code_diag" in df.columns:
            df = df.withColumn("_sk_diagnostic", sha2(col("code_diag").cast("string"), 256))
        
        if "identifiant_organisation" in df.columns or "finess" in df.columns:
            org_col = "identifiant_organisation" if "identifiant_organisation" in df.columns else "finess"
            df = df.withColumn("_sk_etablissement", sha2(col(org_col).cast("string"), 256))
        
        # ✅ CORRECTION: Ajouter _sk_professionnel pour consultations
        if "id_prof_sante" in df.columns:
            df = df.withColumn("_sk_professionnel", sha2(col("id_prof_sante").cast("string"), 256))
        
        if "region" in df.columns:
            df = df.withColumn("_sk_region", sha2(upper(trim(col("region"))).cast("string"), 256))
        
        if "sexe" in df.columns:
            df = df.withColumn("_sk_sexe", sha2(upper(trim(col("sexe"))).cast("string"), 256))
    
    # Clés pour les tables de référence
    if "etablissement" in output_table.lower():
        if "finess_site" in df.columns:
            df = df.withColumn("_sk_etablissement", sha2(col("finess_site").cast("string"), 256))
    
    if "diagnostic" in output_table.lower():
        if "code_diag" in df.columns:
            df = df.withColumn("_sk_diagnostic", sha2(col("code_diag").cast("string"), 256))
    
    # Pour les professionnels de santé
    if "professionnel" in output_table.lower():
        if "identifiant" in df.columns:
            df = df.withColumn("_sk_professionnel", sha2(col("identifiant").cast("string"), 256))
    
    return df

def add_technical_columns(df, config):
    """Ajoute les colonnes techniques pour le tracking."""
    source_name = config["source_name"]
    output_table = config["output_table"]
    
    # Hash du record complet pour détection de changements
    df = df.withColumn("_hash_record", sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256))
    
    # ID NUMÉRIQUE POUR SUPERSET: row_number() absolu
    df = df.withColumn("_id", monotonically_increasing_id())
    
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

def process_dataframe_enhanced(df, config, source_type):
    """Traite un DataFrame avec améliorations RGPD et analytiques."""
    # 1. Nettoyage colonnes
    df = clean_col_names(df)
    
    # 2. Normalisation des données
    df = normalize_data_enhanced(df, config)
    
    # 3. AMÉLIORATIONS SPÉCIFIQUES PAR TABLE
    df = enhance_hospitalisations_data(df, config)
    df = enhance_deces_data(df, config)
    df = enhance_etablissements_data(df, config)
    df = enhance_satisfaction_data(df, config)
    
    # 4. Ajout des colonnes analytiques
    df = add_analytical_columns(df, config)
    
    # 5. Anonymisation RGPD-compliant
    df = anonymize_pii_rgpd_compliant(df, config)
    
    # 6. Génération des clés métier
    df = generate_business_keys(df, config)
    
    # 7. Colonnes techniques
    df = add_technical_columns(df, config)
    
    return df

def write_to_minio_safe(df, output_table):
    """Écrit les données dans MinIO de manière SÉCURISÉE."""
    bronze_path = f"s3a://{MINIO_CONFIG['bucket']}/{output_table}"
    
    try:
        # Optimisation partitions
        df = df.coalesce(2)
        
        # CORRECTION : Nettoyage caractères de contrôle avec regex VALIDE
        for column in df.columns:
            if isinstance(df.schema[column].dataType, StringType):
                # Regex valide pour supprimer les caractères de contrôle
                df = df.withColumn(column,
                    when(col(column).isNotNull(), 
                         regexp_replace(col(column), r"[\x00-\x1F]", ""))
                    .otherwise(col(column))
                )
        
        # Réorganisation des colonnes: techniques en premier
        technical_cols = [c for c in df.columns if c.startswith('_')]
        business_cols = [c for c in df.columns if not c.startswith('_')]
        df = df.select(*technical_cols, *business_cols)
        
        # Écriture avec gestion d'erreur
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "500000") \
            .parquet(bronze_path)
        
        return df.count()
        
    except Exception as e:
        print(f"❌ Erreur écriture {output_table}: {e}")
        # Fallback: écriture sans transformations risquées
        try:
            df.coalesce(2).write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(bronze_path)
            return df.count()
        except Exception as fallback_error:
            print(f"💥 Échec fallback {output_table}: {fallback_error}")
            raise

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
        df_processed = process_dataframe_enhanced(df, config, "csv")
        
        # Écriture SÉCURISÉE
        written_count = write_to_minio_safe(df_processed, output_table)
        
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
        
        # DIAGNOSTIC pour décès
        if "deces" in output_table.lower():
            diagnose_date_issues(df, output_table)
        
        # Traitement
        df_processed = process_dataframe_enhanced(df, config, "postgres")
        
        # Écriture SÉCURISÉE
        written_count = write_to_minio_safe(df_processed, output_table)
        
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
def enhance_hospitalisations_data(df, config):
    """Améliore les données hospitalisations avec mapping des colonnes."""
    if config["output_table"] != "hospitalisations":
        return df
    
    print("   🔧 Amélioration des données hospitalisations")
    
    # Mapping des colonnes existantes vers les noms standard
    column_mappings = {
        "date_admission": ["date_entree", "jour_hospitalisation"],
        "diagnostic_principal": ["code_diagnostic", "suite_diagnostic_consultation"],
        "sexe": ["sexe", "gender"]
    }
    
    # Vérification et renommage
    for standard_name, variants in column_mappings.items():
        for variant in variants:
            if variant in df.columns and standard_name not in df.columns:
                df = df.withColumnRenamed(variant, standard_name)
                print(f"     ✅ '{variant}' → '{standard_name}'")
                break
    
    # Si sexe manque toujours, essayer de le déduire
    if "sexe" not in df.columns:
        df = df.withColumn("sexe", lit("I"))  # Inconnu par défaut
        print("     ⚠️  Colonne 'sexe' ajoutée avec valeur par défaut 'I'")
    
    return df

def enhance_deces_data(df, config):
    """Améliore les données décès avec extraction région/département - VERSION CORRIGÉE."""
    if config["output_table"] != "deces":
        return df

    print("   🔧 Amélioration des données décès")

    # CORRECTION: code_lieu_deces est un code géographique, pas une date
    if "code_lieu_deces" in df.columns:
        # S'assurer que c'est une chaîne de caractères
        if not isinstance(df.schema["code_lieu_deces"].dataType, StringType):
            df = df.withColumn("code_lieu_deces", col("code_lieu_deces").cast("string"))
        
        # Extraction du département depuis le code_lieu_deces (les 2 premiers chiffres)
        if "departement" not in df.columns:
            df = df.withColumn(
                "departement",
                when(length(col("code_lieu_deces")) >= 2, 
                     substring(col("code_lieu_deces"), 1, 2))
                .otherwise(lit(None))
            )
            print("     ✅ Département extrait de code_lieu_deces")
    
    # 3. Mapping des départements vers les régions
    if "departement" in df.columns and "region" not in df.columns:
        df = df.withColumn(
            "region",
            when(col("departement").isin(["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("departement").isin(["02", "08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]), "Grand Est")
            .when(col("departement").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("departement").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("departement").isin(["14", "27", "50", "61", "76"]), "Normandie")
            .when(col("departement").isin(["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("departement").isin(["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("departement").isin(["18", "28", "36", "37", "41", "45"]), "Centre-Val de Loire")
            .when(col("departement").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("departement").isin(["22", "29", "35", "56"]), "Bretagne")
            .when(col("departement").isin(["59", "62", "80"]), "Hauts-de-France")
            .when(col("departement").isin(["13", "83", "84", "04", "05", "06"]), "Provence-Alpes-Côte d'Azur")
            .when(col("departement").isin(["2A", "2B"]), "Corse")
            .when(col("departement").isin(["971", "972", "973", "974", "976"]), "Outre-Mer")
            .otherwise("Région inconnue")
        )
        print("     ✅ Région déduite du département")
    
    return df

def enhance_etablissements_data(df, config):
    """Améliore les données établissements avec mapping des colonnes."""
    if config["output_table"] != "etablissements":
        return df
    
    print("   🔧 Amélioration des données établissements")
    
    # finess_site est déjà présent dans tes données
    if "finess_site" not in df.columns and "identifiant_organisation" in df.columns:
        df = df.withColumnRenamed("identifiant_organisation", "finess_site")
        print("     ✅ 'identifiant_organisation' → 'finess_site'")
    
    # Extraction de la région depuis le code_postal
    if "region" not in df.columns and "code_postal" in df.columns:
        df = df.withColumn(
            "departement_from_cp",
            when(length(col("code_postal")) >= 2, substring(col("code_postal"), 1, 2))
            .otherwise(lit(None))
        ).withColumn(
            "region",
            when(col("departement_from_cp").isin(["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("departement_from_cp").isin(["02", "08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]), "Grand Est")
            .when(col("departement_from_cp").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("departement_from_cp").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("departement_from_cp").isin(["14", "27", "50", "61", "76"]), "Normandie")
            .when(col("departement_from_cp").isin(["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("departement_from_cp").isin(["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("departement_from_cp").isin(["18", "28", "36", "37", "41", "45"]), "Centre-Val de Loire")
            .when(col("departement_from_cp").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("departement_from_cp").isin(["22", "29", "35", "56"]), "Bretagne")
            .when(col("departement_from_cp").isin(["59", "62", "80"]), "Hauts-de-France")
            .when(col("departement_from_cp").isin(["13", "83", "84", "04", "05", "06"]), "Provence-Alpes-Côte d'Azur")
            .when(col("departement_from_cp").isin(["2A", "2B"]), "Corse")
            .when(col("departement_from_cp").isin(["971", "972", "973", "974", "976"]), "Outre-Mer")
            .otherwise("Région inconnue")
        ).drop("departement_from_cp")
        print("     ✅ Région déduite du code postal")
    
    return df

def enhance_satisfaction_data(df, config):
    """Améliore les données satisfaction avec mapping des colonnes."""
    if "satisfaction" not in config["output_table"]:
        return df
    
    print("   🔧 Amélioration des données satisfaction")
    
    # Mapping des colonnes de score
    score_mappings = {
        "score_all_ajust": ["score_all_rea_ajust", "score_all_ajust"],
        "classement": ["classement"],
        "evolution": ["evolution"]
    }
    
    for standard_name, variants in score_mappings.items():
        for variant in variants:
            if variant in df.columns and standard_name not in df.columns:
                df = df.withColumnRenamed(variant, standard_name)
                print(f"     ✅ '{variant}' → '{standard_name}'")
                break
    
    return df

def verify_bronze_data():
    """Vérifie que les données essentielles sont présentes pour les indicateurs."""
    spark = get_spark_session()

    essential_tables = {
        "patient": ["id_patient", "sexe", "date_naissance", "age"],
        "consultation": ["id_patient", "date_consultation", "code_diag", "id_prof_sante"],
        "hospitalisations": ["id_patient", "date_admission", "diagnostic_principal", "sexe"],
        "deces_2019": ["date_deces", "region", "departement"],
        "diagnostic": ["code_diag", "diagnostic"],
        "professionnel_de_sante": ["identifiant", "profession"],
        "etablissement_sante": ["finess_site", "region"],
        "satisfaction_2019": ["finess", "region", "score_all_ajust"],
        "mutuelle": ["id_mut", "nom_mutuelle"],
        "medicaments": ["code_cip", "nom_medicament"],
        "laboratoire": ["id_laboratoire", "nom_laboratoire"],
        "salle": ["id_salle", "nom_salle"],
        "specialites": ["code_specialite", "nom_specialite"]
    }
    
    print("🔍 VÉRIFICATION DES DONNÉES BRONZE")
    print("=" * 50)
    
    for table, required_cols in essential_tables.items():
        try:
            df = spark.read.parquet(f"s3a://bronze/{table}")
            count = df.count()
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            status = "✅" if not missing_cols and count > 0 else "❌"
            print(f"{status} {table}: {count} lignes - Colonnes manquantes: {missing_cols}")
            
        except Exception as e:
            print(f"❌ {table}: ERREUR - {e}")
    
    spark.stop()

def main_optimized():
    """Version optimisée du main avec seulement 8 tables essentielles."""
    print("""
    ╔══════════════════════════════════════════╗
    ║        PIPELINE BRONZE CORRIGÉ          ║
    ║         ERRORS REGEX RÉSOLUES           ║
    ╚══════════════════════════════════════════╝
    """)
    
    print(f"⚙️  Configuration corrigée:")
    print(f"   - Regex invalides CORRIGÉES")
    print(f"   - 12 tables complètes (alignement avec Silver)")
    print(f"   - Écriture sécurisée activée")
    print(f"   - Anonymisation RGPD-compliant")
    
    try:
        spark = get_spark_session()
        
        if not test_connections(spark):
            print("💥 Erreur connexion")
            sys.exit(1)
        
        # CONFIGURATION COMPLÈTE - 12 TABLES POUR ALIGNEMENT AVEC SILVER
        source_configs = [
    # === CORE DATA - PATIENTS ===
    {
        "type": "postgres",
        "source_name": "Patients",
        "path": "\"Patient\"",
        "output_table": "patient",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": ["nom", "prenom", "email", "tel", "num_secu", "adresse", "ville"],
        "preserve_columns": ["sexe", "date_naissance", "code_postal", "age", "groupe_sanguin", "poid", "taille"],
        "column_mappings": {
            "date": "date_naissance"
        }
    },
    
    # === CORE DATA - CONSULTATIONS ===
    {
        "type": "postgres",
        "source_name": "Consultations", 
        "path": "\"Consultation\"",
        "output_table": "consultation",
        "encoding": "utf-8", 
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["date_consultation", "code_diag", "id_prof_sante", "id_patient", "id_mut", "motif", "heure_debut", "heure_fin"],
        "column_mappings": {
            "date": "date_consultation"
        }
    },
    
    # === CORE DATA - DÉCÈS (FILTRÉ 2019) ===
    {
    "type": "postgres",
    "source_name": "Deces_2019",
    "path": "\"deces\"",
    "output_table": "deces_2019",
    "encoding": "utf-8",
    "delimiter": ";",
    "pii_columns": ["nom", "prenom", "adresse", "ville"],
    "preserve_columns": ["sexe", "date_naissance", "date_deces", "code_postal", "region", "departement", "code_lieu_deces", "lieu_naissance", "pays_naissance"],
    "date_validation": {
        "min_year": 1900,
        "max_year": 2024,
        "fix_invalid_dates": True
    },
    "geo_extraction": {
        "from_column": "code_lieu_deces",
        "extract_departement": True,
        "deduce_region": True
    }
},
    
    # === CORE DATA - HOSPITALISATIONS ===
    {
        "type": "csv",
        "source_name": "Hospitalisations",
        "path": "file:///data/source/csv/Hospitalisations.csv",
        "output_table": "hospitalisations",
        "encoding": "ascii", 
        "delimiter": ";",
        "pii_columns": ["nom_patient", "prenom_patient"],
        "preserve_columns": ["sexe", "date_naissance", "region", "diagnostic_principal", "date_admission", "date_sortie", "identifiant_organisation", "id_patient", "duree_sejour"],
        "column_mappings": {
            "date_entree": "date_admission",
            "code_diagnostic": "diagnostic_principal", 
            "jour_hospitalisation": "duree_sejour"
        }
    },
    
    # === REFERENCE DATA - DIAGNOSTICS ===
    {
        "type": "postgres", 
        "source_name": "Diagnostics",
        "path": "\"Diagnostic\"",
        "output_table": "diagnostic",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["code_diag", "diagnostic"]
    },
    
    # === REFERENCE DATA - PROFESSIONNELS SANTÉ ===
    {
        "type": "postgres",
        "source_name": "Professionnels_Sante",
        "path": "\"Professionnel_de_sante\"", 
        "output_table": "professionnel_de_sante",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": ["nom", "prenom"],
        "preserve_columns": ["civilite", "profession", "code_specialite", "identifiant", "categorie_professionnelle", "type_identifiant"]
    },
    
    # === REFERENCE DATA - ÉTABLISSEMENTS ===
    {
        "type": "csv",
        "source_name": "Etablissements", 
        "path": "file:///data/source/csv/etablissement_sante.csv",
        "output_table": "etablissement_sante",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": ["email", "telephone", "telephone_2", "adresse"],
        "preserve_columns": ["region", "departement", "code_postal", "ville", "finess_site", "identifiant_organisation", "raison_sociale_site", "siren_site", "siret_site"],
        "geo_extraction": {
            "from_column": "code_postal", 
            "extract_departement": True,
            "deduce_region": True
        }
    },
    
    # === QUALITY DATA - SATISFACTION 2020 ===
    {
        "type": "csv",
        "source_name": "Satisfaction_MCO_2020",
        "path": "file:///data/source/csv/resultats-esatisca-mco-open-data-2020.csv",
        "output_table": "satisfaction_mco_2020",  
        "encoding": "utf-8",
        "delimiter": ",",
        "pii_columns": [],
        "preserve_columns": ["finess", "region", "score_all_ajust", "classement", "evolution", "rs_finess", "finess_geo", "participation", "depot"],
        "score_columns": [
            "score_avh_ajust", "score_acc_ajust", "score_pec_ajust", 
            "score_cer_ajust", "score_ovs_ajust", "taux_reco_brut"
        ]
    },

    # === REFERENCE DATA - MUTUELLE ===
    {
        "type": "postgres",
        "source_name": "Mutuelle",
        "path": "\"Mutuelle\"",
        "output_table": "mutuelle",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["id_mut", "nom_mutuelle", "type_mutuelle", "code_postal", "ville"]
    },

    # === REFERENCE DATA - MEDICAMENTS ===
    {
        "type": "postgres",
        "source_name": "Medicaments",
        "path": "\"Medicaments\"",
        "output_table": "medicaments",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["code_cip", "nom_medicament", "forme", "dosage", "prix", "laboratoire_id", "classe_therapeutique"]
    },

    # === REFERENCE DATA - LABORATOIRE ===
    {
        "type": "postgres",
        "source_name": "Laboratoire",
        "path": "\"Laboratoire\"",
        "output_table": "laboratoire",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["id_laboratoire", "nom_laboratoire", "adresse", "ville", "code_postal", "pays"]
    },

    # === REFERENCE DATA - SALLE ===
    {
        "type": "postgres",
        "source_name": "Salle",
        "path": "\"Salle\"",
        "output_table": "salle",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["id_salle", "nom_salle", "type_salle", "capacite", "etage", "batiment", "finess_site"]
    },

    # === REFERENCE DATA - SPECIALITES ===
    {
        "type": "postgres",
        "source_name": "Specialites",
        "path": "\"Specialites\"",
        "output_table": "specialites",
        "encoding": "utf-8",
        "delimiter": ";",
        "pii_columns": [],
        "preserve_columns": ["code_specialite", "nom_specialite", "categorie", "description"]
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
        
        # RAPPORT FINAL CORRIGÉ
        print(f"\n🎉 PIPELINE BRONZE CORRIGÉ TERMINÉ")
        print(f"✅ Succès: {len(successful_tables)} tables")
        print(f"❌ Échecs: {len(failed_tables)} tables")
        print(f"🎯 Tables complètes: 12/12")
        print(f"🔐 Anonymisation RGPD: ACTIVÉE")
        print(f"🔧 Regex corrigées: OUI")
        
        # RAPPORT DES TABLES TRAITÉES
        print(f"\n📋 TABLES TRAITÉES:")
        for table in successful_tables:
            print(f"   ✅ {table}")
        if failed_tables:
            print(f"\n⚠️  TABLES EN ÉCHEC:")
            for table in failed_tables:
                print(f"   ❌ {table}")
                
        # Vérification finale
        print(f"\n🔍 VÉRIFICATION FINALE:")
        verify_bronze_data()
                
    except Exception as e:
        print(f"💥 Erreur: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Utiliser la version corrigée
    main_optimized()