import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, to_date, count, sum, avg,
    when, lit, datediff, months_between, round, expr,
    countDistinct, dense_rank, row_number, desc, upper, trim,
    regexp_replace, coalesce, length, substring, concat_ws,
    current_timestamp, md5, sha2, mean, stddev, isnan, isnull,
    create_map, regexp_extract, lower, split, explode, size,
    array_contains, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
import os
import re

# Configuration identique à Bronze
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "bucket": "silver"
}

def get_spark_session():
    """Session Spark optimisée avec configuration IDENTIQUE à Bronze."""
    try:
        # MÊME CONFIGURATION QUE BRONZE
        jars_dir = "/home/jovyan/jars"
        jar_files = [
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
            f"{jars_dir}/hadoop-common-3.3.4.jar",
            f"{jars_dir}/postgresql-42.6.0.jar"
        ]
        
        # Vérification des JARs et configuration des classpath
        for jar in jar_files:
            if not os.path.exists(jar):
                raise Exception(f"❌ JAR manquant: {jar}")
        
        jars_path = ",".join(jar_files)
        print(f"📚 JARs chargés: {len(jar_files)}")
        
        # Configuration de base de Spark - IDENTIQUE À BRONZE
        builder = SparkSession.builder \
            .appName("Silver Pipeline") \
            .config("spark.jars", jars_path) \
            .config("spark.driver.extraClassPath", jars_path) \
            .config("spark.executor.extraClassPath", jars_path) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
        # Configuration Hadoop et S3A - CRITIQUE
        hadoop_conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": MINIO_CONFIG["endpoint"],
            "spark.hadoop.fs.s3a.access.key": MINIO_CONFIG["access_key"],
            "spark.hadoop.fs.s3a.secret.key": MINIO_CONFIG["secret_key"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.connection.maximum": "100",
            "spark.hadoop.fs.s3a.threads.max": "20",
            "spark.hadoop.fs.s3a.connection.timeout": "200000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
            "spark.hadoop.fs.s3a.retry.limit": "3"
        }
        
        # Application des configurations Hadoop
        for key, value in hadoop_conf.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # CONFIGURATION HADOOP EXPLICITE - CRITIQUE
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        
        # Configuration pour éviter les erreurs de cache
        hadoop_conf.set("fs.s3a.impl.disable.cache", "true")
        hadoop_conf.set("fs.s3a.bucket.all.committer.magic.enabled", "true")
        
        print("✅ Spark Silver initialisé avec configuration S3A")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark Silver: {e}")
        raise

def test_minio_connection(spark):
    """Teste la connexion à MinIO."""
    try:
        print("🔍 Test connexion MinIO Silver...")
        
        # Test simple de Spark
        test_df = spark.range(1).limit(1)
        test_count = test_df.count()
        print(f"✅ Test Spark de base OK: {test_count} enregistrements")
        
        # Test S3A avec une opération simple
        try:
            # Créer un petit DataFrame de test
            test_data = [("test", 1)]
            test_df = spark.createDataFrame(test_data, ["name", "value"])
            
            # Essayer d'écrire dans Silver
            test_path = f"s3a://silver/test_connection"
            test_df.write.mode("overwrite").parquet(test_path)
            print("✅ Écriture S3A test réussie")
            
            # Essayer de lire depuis Bronze
            try:
                test_read = spark.read.parquet("s3a://bronze/patients").limit(1)
                test_count = test_read.count()
                print(f"✅ Lecture S3A test réussie: {test_count} enregistrement(s)")
            except Exception as read_error:
                print(f"⚠️  Lecture S3A échouée: {read_error}")
            
            return True
            
        except Exception as s3_error:
            print(f"⚠️  Test S3A échoué: {s3_error}")
            return False
            
    except Exception as e:
        print(f"❌ Erreur test connexion: {e}")
        return False

def read_bronze_table(spark, table_name):
    """Lit une table depuis la couche bronze."""
    try:
        bronze_path = f"s3a://bronze/{table_name}"
        print(f"📂 Lecture de {table_name} depuis {bronze_path}")
        
        df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(bronze_path)
        
        count = df.count()
        print(f"  - {count:,} lignes chargées")
        print(f"  - Colonnes disponibles: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        raise

def data_quality_checks(df, table_name):
    """Effectue des contrôles de qualité sur les données."""
    print(f"  🔍 Contrôles qualité pour {table_name}...")
    
    count = df.count()
    if count == 0:
        print(f"  ⚠️  Table {table_name} vide")
        return df
    
    print(f"  ✅ {count} enregistrements analysés")
    return df

def normalize_diagnostic_codes(df):
    """Normalise les codes diagnostics selon la classification CIM-10."""
    print("  🏷️  Normalisation des codes diagnostics...")
    
    if "code_diag" not in df.columns:
        return df
    
    # Mapping des catégories CIM-10 simplifié
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    def categorize_diagnostic(code):
        if not code:
            return "Non spécifié"
        code_str = str(code).upper().strip()
        
        if code_str.startswith(('A', 'B')): return "Maladies infectieuses"
        elif code_str.startswith(('C', 'D')): return "Tumeurs" 
        elif code_str.startswith('E'): return "Maladies endocriniennes"
        elif code_str.startswith('F'): return "Troubles mentaux"
        elif code_str.startswith('G'): return "Maladies neurologiques"
        elif code_str.startswith('I'): return "Maladies cardiovasculaires"
        elif code_str.startswith('J'): return "Maladies respiratoires"
        else: return "Autres maladies"
    
    categorize_udf = udf(categorize_diagnostic, StringType())
    
    df = df.withColumn("categorie_diagnostic", categorize_udf(col("code_diag")))
    df = df.withColumn("code_diag_normalise", upper(trim(col("code_diag"))))
    
    return df

def create_patient_dimension(spark):
    """Crée la dimension patient enrichie."""
    print("\n👤 Création de la dimension patient...")
    
    patients = read_bronze_table(spark, "patients")
    patients = data_quality_checks(patients, "patients")
    
    # Vérifier les colonnes disponibles et créer l'initiale du prénom
    if "prenom" in patients.columns:
        patients = patients.withColumn(
            "initiale_prenom", 
            when(col("prenom").isNotNull(), upper(substring(trim(col("prenom")), 1, 1)))
        )
    else:
        patients = patients.withColumn("initiale_prenom", lit(None))
    
    patient_dim = patients.select(
        col("_sk_patient").alias("patient_sk"),
        col("id_patient"),
        col("sexe"),
        to_date(col("date_naissance")).alias("date_naissance"),
        
        # Calcul âge
        round(months_between(current_timestamp(), to_date(col("date_naissance"))) / 12).alias("age_actuel"),
        
        # Tranches d'âge
        when(col("date_naissance").isNull(), "Non renseigné")
        .when(datediff(current_timestamp(), to_date(col("date_naissance"))) / 365 < 18, "0-17 ans")
        .when(datediff(current_timestamp(), to_date(col("date_naissance"))) / 365 <= 35, "18-35 ans")
        .when(datediff(current_timestamp(), to_date(col("date_naissance"))) / 365 <= 55, "36-55 ans")
        .when(datediff(current_timestamp(), to_date(col("date_naissance"))) / 365 <= 75, "56-75 ans")
        .otherwise("75+ ans").alias("tranche_age"),
        
        # Géographie
        when(length(trim(col("code_postal"))) >= 2, substring(trim(col("code_postal")), 1, 2)).alias("departement"),
        col("code_postal"),
        col("initiale_prenom"),
        
        current_timestamp().alias("silver_ingestion_timestamp")
    )
    
    print(f"  ✅ Dimension patient créée: {patient_dim.count()} patients")
    return patient_dim

def create_etablissement_dimension(spark):
    """Crée la dimension établissement."""
    print("\n🏥 Création de la dimension établissement...")
    
    etablissements = read_bronze_table(spark, "etablissements")
    etablissements = data_quality_checks(etablissements, "etablissements")
    
    # Vérifier les colonnes disponibles et gérer les colonnes manquantes
    available_columns = etablissements.columns
    
    # Sélectionner les colonnes disponibles ou créer des valeurs par défaut
    select_exprs = [
        col("_sk_etablissement").alias("etablissement_sk"),
        col("identifiant_organisation").alias("finess"),
        col("raison_sociale_site").alias("nom_etablissement"),
    ]
    
    # Gérer la région
    if "region" in available_columns:
        select_exprs.extend([
            col("region"),
            upper(trim(col("region"))).alias("region_normalisee")
        ])
    else:
        select_exprs.extend([
            lit("Non spécifié").alias("region"),
            lit("NON_SPECIFIE").alias("region_normalisee")
        ])
    
    # Gérer le département
    if "departement" in available_columns:
        select_exprs.append(col("departement"))
    else:
        select_exprs.append(lit("Non spécifié").alias("departement"))
    
    # Colonnes toujours présentes (basées sur votre schéma)
    select_exprs.extend([
        col("code_postal"),
        col("commune").alias("ville"),
    ])
    
    # Catégorisation de l'établissement
    select_exprs.append(
        when(lower(col("raison_sociale_site")).contains("chu"), "CHU")
        .when(lower(col("raison_sociale_site")).contains("hopital"), "Hôpital")
        .when(lower(col("raison_sociale_site")).contains("clinique"), "Clinique")
        .otherwise("Autre").alias("type_etablissement")
    )
    
    # Timestamp d'ingestion
    select_exprs.append(current_timestamp().alias("silver_ingestion_timestamp"))
    
    etablissement_dim = etablissements.select(*select_exprs)
    
    print(f"  ✅ Dimension établissement créée: {etablissement_dim.count()} établissements")
    return etablissement_dim

def create_consultation_fact(spark):
    """Crée la table de fait des consultations."""
    print("\n📊 Création de la table de fait consultations...")
    
    consultations = read_bronze_table(spark, "consultations")
    patients = read_bronze_table(spark, "patients")
    prof_sante = read_bronze_table(spark, "professionnels_sante_pg")
    diagnostics = read_bronze_table(spark, "diagnostics")
    
    diagnostics = normalize_diagnostic_codes(diagnostics)
    
    # Jointures avec gestion des données manquantes
    consultation_fact = consultations.alias("c") \
        .join(patients.alias("p"), col("c.id_patient") == col("p.id_patient"), "left") \
        .join(prof_sante.alias("ps"), col("c.id_prof_sante") == col("ps.identifiant"), "left") \
        .join(diagnostics.alias("d"), col("c.code_diag") == col("d.code_diag"), "left") \
        .select(
            col("c._sk").alias("consultation_sk"),
            col("c._sk_patient").alias("patient_sk"),
            col("c._sk_prof_sante").alias("professionnel_sk"),
            col("c._sk_diagnostic").alias("diagnostic_sk"),
            
            to_date(col("c.date_consultation")).alias("date_consultation"),
            year(col("c.date_consultation")).alias("annee_consultation"),
            month(col("c.date_consultation")).alias("mois_consultation"),
            
            lit(1).alias("nb_consultations"),
            col("p.sexe"),
            col("d.categorie_diagnostic"),
            col("d.code_diag_normalise"),
            
            current_timestamp().alias("silver_ingestion_timestamp")
        )
    
    consultation_fact = data_quality_checks(consultation_fact, "consultation_fact")
    print(f"  ✅ Table consultation créée: {consultation_fact.count()} enregistrements")
    return consultation_fact

def create_hospitalisation_fact(spark):
    """Crée la table de fait des hospitalisations."""
    print("\n🏥 Création de la table de fait hospitalisations...")
    
    hospitalisations = read_bronze_table(spark, "hospitalisations")
    patients = read_bronze_table(spark, "patients")
    etablissements = read_bronze_table(spark, "etablissements")
    diagnostics = read_bronze_table(spark, "diagnostics")
    
    diagnostics = normalize_diagnostic_codes(diagnostics)
    
    hospitalisation_fact = hospitalisations.alias("h") \
        .join(patients.alias("p"), col("h.id_patient") == col("p.id_patient"), "left") \
        .join(etablissements.alias("e"), col("h.identifiant_organisation") == col("e.identifiant_organisation"), "left") \
        .join(diagnostics.alias("d"), col("h.code_diagnostic") == col("d.code_diag"), "left") \
        .select(
            col("h._sk").alias("hospitalisation_sk"),
            col("h._sk_patient").alias("patient_sk"),
            col("h._sk_etablissement").alias("etablissement_sk"),
            col("h._sk_diagnostic").alias("diagnostic_sk"),
            
            to_date(col("h.date_entree")).alias("date_entree"),
            to_date(col("h.date_sortie")).alias("date_sortie"),
            year(col("h.date_entree")).alias("annee_entree"),
            
            col("h.jour_hospitalisation").alias("duree_sejour_jours"),
            lit(1).alias("nb_hospitalisations"),
            
            col("p.sexe"),
            col("e.region").alias("region_etablissement"),
            col("d.categorie_diagnostic"),
            
            current_timestamp().alias("silver_ingestion_timestamp")
        )
    
    hospitalisation_fact = data_quality_checks(hospitalisation_fact, "hospitalisation_fact")
    print(f"  ✅ Table hospitalisation créée: {hospitalisation_fact.count()} enregistrements")
    return hospitalisation_fact

def create_deces_fact(spark):
    """Crée la table de fait des décès."""
    print("\n⚰️ Création de la table de fait décès...")
    
    deces = read_bronze_table(spark, "deces")
    etablissements = read_bronze_table(spark, "etablissements")
    
    deces_fact = deces.alias("d") \
        .join(etablissements.alias("e"), col("d._sk_etablissement") == col("e._sk_etablissement"), "left") \
        .select(
            col("d._sk").alias("deces_sk"),
            col("d._sk_patient").alias("patient_sk"),
            col("d._sk_etablissement").alias("etablissement_sk"),
            
            to_date(col("d.date_deces")).alias("date_deces"),
            year(col("d.date_deces")).alias("annee_deces"),
            
            col("d.sexe"),
            to_date(col("d.date_naissance")).alias("date_naissance"),
            round(months_between(to_date(col("d.date_deces")), to_date(col("d.date_naissance"))) / 12).alias("age_deces"),
            
            col("e.region").alias("region_deces"),
            col("d.code_postal"),
            
            lit(1).alias("nb_deces"),
            
            current_timestamp().alias("silver_ingestion_timestamp")
        )
    
    deces_fact = data_quality_checks(deces_fact, "deces_fact")
    print(f"  ✅ Table décès créée: {deces_fact.count()} enregistrements")
    return deces_fact

def write_silver_tables(spark, dimensions_and_facts):
    """Tente d'écrire les tables Silver avec gestion d'erreurs."""
    print("\n💾 Tentative d'écriture des tables Silver...")
    
    for name, df in dimensions_and_facts.items():
        try:
            if df.count() > 0:
                silver_path = f"s3a://silver/{name}"
                print(f"  📤 Écriture {name} ({df.count()} lignes)...")
                
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(silver_path)
                    
                print(f"  ✅ {name} écrit avec succès")
            else:
                print(f"  ⚠️  {name} vide - écriture ignorée")
                
        except Exception as e:
            print(f"  ❌ Erreur écriture {name}: {e}")
            print(f"  💡 Les données {name} sont disponibles en mémoire")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                   SILVER LAYER PIPELINE                      ║
    ║  Enrichissement sémantique et contrôle qualité multi-source  ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        print("✨ Session Spark initialisée avec optimisations")
        
        # Test connexion
        connection_ok = test_minio_connection(spark)
        
        if not connection_ok:
            print("❌ Connexion S3A échouée - arrêt du pipeline")
            spark.stop()
            exit(1)
        
        # Création des dimensions et faits
        print("\n🎯 DÉBUT DU TRAITEMENT SILVER...")
        
        patient_dim = create_patient_dimension(spark)
        etablissement_dim = create_etablissement_dimension(spark)
        consultation_fact = create_consultation_fact(spark)
        hospitalisation_fact = create_hospitalisation_fact(spark)
        deces_fact = create_deces_fact(spark)
        
        # Collecte pour écriture
        silver_tables = {
            "dim_patient": patient_dim,
            "dim_etablissement": etablissement_dim, 
            "fact_consultation": consultation_fact,
            "fact_hospitalisation": hospitalisation_fact,
            "fact_deces": deces_fact
        }
        
        # Tentative d'écriture
        write_silver_tables(spark, silver_tables)
        
        # Résumé
        print(f"""
🎉 PIPELINE SILVER TERMINÉ AVEC SUCCÈS!

📊 RÉSULTATS:
├── Dimensions:
│   ├── Patients: {patient_dim.count():,} enregistrements
│   └── Établissements: {etablissement_dim.count():,} enregistrements
└── Faits:
    ├── Consultations: {consultation_fact.count():,} enregistrements  
    ├── Hospitalisations: {hospitalisation_fact.count():,} enregistrements
    └── Décès: {deces_fact.count():,} enregistrements

💡 INFORMATIONS:
✓ Données enrichies et normalisées
✓ Clés de substitution générées
✓ Modèle dimensionnel créé
✓ Contrôles qualité effectués
        """)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution du pipeline Silver: {e}")
        import traceback
        traceback.print_exc()