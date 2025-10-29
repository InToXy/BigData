import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession

# Import de l'utilitaire metastore
sys.path.insert(0, '/home/jovyan/jobs/utils')
from metastore_init import initialize_for_layer
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min, max,
    when, lit, datediff, floor, year, month, quarter,
    row_number, rank, percent_rank, lag, lead,
    concat_ws, split, regexp_extract, regexp_replace, upper, trim,
    date_add, current_date, current_timestamp, hour,
    broadcast, expr, array_contains, collect_list, first
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "bronze_bucket": "bronze",
    "silver_bucket": "silver"
}

# Décorateur pour le monitoring
def log_transformation(func):
    """Décorateur pour logger les transformations"""
    def wrapper(*args, **kwargs):
        table_name = kwargs.get('table_name', func.__name__)
        start_time = time.time()
        print(f"🔄 [{datetime.now()}] Début: {table_name}")
        
        result = func(*args, **kwargs)
        
        duration = time.time() - start_time
        count = result.count() if hasattr(result, 'count') else 0
        print(f"✅ [{datetime.now()}] Fin: {table_name} - {count:,} lignes - {duration:.2f}s")
        
        return result
    return wrapper

# Métriques de qualité
def compute_quality_metrics(df, table_name):
    """Calcule des métriques de qualité"""
    total_rows = df.count()
    
    metrics = {
        "table": table_name,
        "total_rows": total_rows,
        "null_rates": {},
        "distinct_counts": {}
    }
    
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        metrics["null_rates"][col_name] = null_count / total_rows * 100 if total_rows > 0 else 0
        
        if total_rows < 1000000:  # Limiter pour les grandes tables
            metrics["distinct_counts"][col_name] = df.select(col_name).distinct().count()
    
    print(f"📊 Métriques {table_name}:")
    print(f"   - Lignes: {total_rows:,}")
    print(f"   - Colonnes avec >10% nulls: {[k for k,v in metrics['null_rates'].items() if v > 10]}")
    
    return metrics

def get_spark_session():
    """Session Spark optimisée pour le traitement Silver."""
    try:
        # Charger les JARs nécessaires
        jars_dir = "/home/jovyan/jars"
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
            jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
        else:
            jars_path = ""

        builder = SparkSession.builder \
            .appName("Silver Layer Processor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skew.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport()
        
        if jars_path:
            builder = builder.config("spark.jars", jars_path)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Silver initialisé")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark Silver: {e}")
        raise

def read_bronze_table(spark, table_name):
    """Lit une table depuis le layer Bronze."""
    try:
        bronze_path = f"s3a://{MINIO_CONFIG['bronze_bucket']}/{table_name}"
        df = spark.read.parquet(bronze_path)
        print(f"✅ Bronze '{table_name}' lu: {df.count()} lignes")
        return df
    except Exception as e:
        print(f"❌ Erreur lecture Bronze {table_name}: {e}")
        raise

def write_silver_table(df, table_name, partition_cols=None):
    """Écrit une table dans le layer Silver ET enregistre dans le metastore."""
    try:
        # ✅ NOUVEAU: Écriture avec enregistrement dans le metastore
        table_full_name = f"silver.{table_name}"

        writer = df.write.mode("overwrite").format("parquet")

        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        writer.option("compression", "snappy") \
              .option("maxRecordsPerFile", "100000") \
              .option("path", f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}") \
              .saveAsTable(table_full_name)

        print(f"✅ Silver '{table_full_name}' écrit: {df.count()} lignes + enregistré dans metastore")
        return True

    except Exception as e:
        print(f"❌ Erreur écriture Silver {table_name}: {e}")
        raise

@log_transformation
def create_silver_patients(bronze_patients, bronze_consultations, bronze_hospitalisations):
    """Crée la table Silver des patients avec indicateurs d'activité."""
    
    # Conversion des types de données problématiques
    bronze_hospitalisations_clean = bronze_hospitalisations.withColumn(
        "jour_hospitalisation", 
        col("jour_hospitalisation").cast("integer")
    )
    
    # Activité des patients (consultations) - Utilisation des clés _sk avec alias
    consultations_activity = bronze_consultations \
        .filter(col("date_consultation").isNotNull()) \
        .groupBy("_sk_patient") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("code_diag").alias("nb_diagnostics_distincts"),
            min("date_consultation").alias("date_premiere_consultation"),
            max("date_consultation").alias("date_derniere_consultation"),
            avg(year("date_consultation")).alias("annee_moyenne_consultations")
        ).alias("consult_act")
    
    # Hospitalisations des patients - Utilisation des clés _sk avec alias
    hospitalisations_activity = bronze_hospitalisations_clean \
        .filter(col("date_admission").isNotNull()) \
        .groupBy("_sk_patient") \
        .agg(
            count("*").alias("nb_hospitalisations_total"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            min("date_admission").alias("date_premiere_hospitalisation"),
            max("date_admission").alias("date_derniere_hospitalisation")
        ).alias("hosp_act")
    
    # Table Silver patients enrichie avec jointures sur _sk et sélection explicite
    silver_patients = bronze_patients.alias("p") \
        .join(consultations_activity, col("p._sk_patient") == col("consult_act._sk_patient"), "left") \
        .join(hospitalisations_activity, col("p._sk_patient") == col("hosp_act._sk_patient"), "left") \
        .select(
            col("p.*"),  # Toutes les colonnes de base des patients
            col("consult_act.nb_consultations_total"),
            col("consult_act.nb_diagnostics_distincts"),
            col("consult_act.date_premiere_consultation"),
            col("consult_act.date_derniere_consultation"),
            col("consult_act.annee_moyenne_consultations"),
            col("hosp_act.nb_hospitalisations_total"),
            col("hosp_act.total_jours_hospitalisation"),
            col("hosp_act.duree_moyenne_sejour"),
            col("hosp_act.date_premiere_hospitalisation"),
            col("hosp_act.date_derniere_hospitalisation")
        ) \
        .withColumn("statut_activite",
            when(col("nb_consultations_total").isNull(), "Inactif")
            .when(col("nb_consultations_total") <= 2, "Occasionnel")
            .when(col("nb_consultations_total") <= 10, "Regulier")
            .otherwise("Frequents")
        ) \
        .withColumn("patient_hospitalise", 
            when(col("nb_hospitalisations_total").isNotNull(), True).otherwise(False)
        ) \
        .withColumn("categorie_age",
            when(col("age") < 18, "0-17")
            .when(col("age") < 30, "18-29")
            .when(col("age") < 45, "30-44")
            .when(col("age") < 60, "45-59")
            .when(col("age") < 75, "60-74")
            .otherwise("75+")
        ) \
        .withColumn("segment_patient",
            when((col("age") < 18) & (col("nb_consultations_total") > 5), "Jeune_Frequent")
            .when((col("age") >= 65) & (col("nb_hospitalisations_total") > 0), "Senior_Hospitalise")
            .when(col("statut_activite") == "Frequents", "Patient_Chronique")
            .otherwise("Standard")
        ) \
        .withColumn("region_code", when(col("code_postal").isNotNull(), regexp_extract(col("code_postal"), r"^(\d{2})", 1)).otherwise("00")) \
        .withColumn("patient_region",
            when(col("region_code").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("region_code").isin(["14", "27", "50", "61", "76"]), "Normandie")
            .when(col("region_code").isin(["02", "59", "60", "62", "80"]), "Hauts-de-France")
            .when(col("region_code").isin(["35", "22", "56", "29"]), "Bretagne")
            .when(col("region_code").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("region_code").isin(["17", "16", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["33", "24", "40", "47", "64"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["31", "09", "11", "12", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("region_code").isin(["69", "42", "43", "63", "03", "15", "07", "26", "38", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("region_code").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("region_code").isin(["54", "55", "57", "88"]), "Grand Est")
            .when(col("region_code").isin(["51", "08", "10", "52"]), "Grand Est")
            .when(col("region_code").isin(["18", "28", "36", "37", "41", "45"]), "Centre-Val de Loire")
            .when(col("region_code").isin(["97"]), "Outre-Mer")
            .otherwise("Autre")
        ) \
        .fillna({
            "nb_consultations_total": 0,
            "nb_diagnostics_distincts": 0,
            "nb_hospitalisations_total": 0,
            "total_jours_hospitalisation": 0,
            "duree_moyenne_sejour": 0
        })
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_patients, "silver_patients")
    
    return silver_patients

@log_transformation
def create_silver_consultations(bronze_consultations, silver_patients, bronze_diagnostics, bronze_professionnels, bronze_etablissements):
    """Crée la table Silver des consultations enrichie avec lien établissement."""
    
    # Conversion des types de données pour les heures
    bronze_consultations_clean = bronze_consultations \
        .withColumn("heure_debut_timestamp", col("heure_debut").cast("timestamp")) \
        .withColumn("heure_fin_timestamp", col("heure_fin").cast("timestamp")) \
        .alias("c")
    
    patients_clean = silver_patients.select(
        "_sk_patient", 
        col("sexe").alias("patient_sexe"),
        "age", 
        "categorie_age", 
        "patient_region"
    ).alias("p")
    
    # Préparer les diagnostics
    diagnostics_clean = bronze_diagnostics.select(
        "_sk", 
        "code_diag", 
        "diagnostic"
    ).withColumn(
        "type_pathologie",
        when(col("diagnostic").contains("chronique"), "Chronique")
        .when(col("diagnostic").contains("aigu"), "Aigu")
        .when(col("diagnostic").contains("prevention"), "Prevention")
        .otherwise("Standard")
    ).alias("d")
    
    # Préparer les professionnels
    professionnels_clean = bronze_professionnels.select(
        "_sk", 
        "identifiant", 
        "profession", 
        "categorie_professionnelle"
    ).alias("prof")
    
    # ✅ AJOUT : Lien établissement via région (extraite du code postal)
    etablissement_par_region = bronze_etablissements \
        .withColumn("region_code", 
            when(col("code_postal").isNotNull(), 
                 regexp_extract(col("code_postal"), r"^(\d{2})", 1))
            .otherwise("00")
        ).withColumn("region",
            when(col("region_code").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("region_code").isin(["14", "27", "50", "61", "76"]), "Normandie") 
            .when(col("region_code").isin(["02", "59", "60", "62", "80"]), "Hauts-de-France")
            .when(col("region_code").isin(["35", "22", "56", "29"]), "Bretagne")
            .when(col("region_code").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("region_code").isin(["17", "16", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["33", "24", "40", "47", "64"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["31", "09", "11", "12", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("region_code").isin(["69", "42", "43", "63", "03", "15", "07", "26", "38", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("region_code").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("region_code").isin(["54", "55", "57", "88"]), "Grand Est")
            .otherwise("Autre")
        ) \
        .groupBy("region") \
        .agg(
            first("finess").alias("finess_principal"),
            first("raison_sociale_site").alias("raison_sociale_principal")
        ).alias("etab")
    
    # Construction de la table silver avec des sélections explicites et jointures sur _sk
    silver_consultations = bronze_consultations_clean \
        .join(patients_clean, col("c._sk_patient") == col("p._sk_patient"), "left") \
        .join(diagnostics_clean, col("c._sk_diagnostic") == col("d._sk"), "left") \
        .join(professionnels_clean, 
              col("c._sk_professionnel") == col("prof._sk"), "left") \
        .join(etablissement_par_region,
              col("p.patient_region") == col("etab.region"), "left") \
        .select(
            col("c.*"),
            col("p.patient_sexe"),
            col("p.age"),
            col("p.categorie_age"),
            col("p.patient_region"),
            col("d.type_pathologie"),
            col("prof.profession"),
            col("prof.categorie_professionnelle"),
            # ✅ AJOUT DES COLONNES ÉTABLISSEMENT
            col("etab.finess_principal").alias("consultation_etablissement_finess"),
            col("etab.raison_sociale_principal").alias("consultation_etablissement_nom")
        ) \
        .withColumn("duree_consultation_heures",
            (col("heure_fin_timestamp").cast("long") - col("heure_debut_timestamp").cast("long")) / 3600.0
        ) \
        .withColumn("consultation_longue",
            when(col("duree_consultation_heures") > 1, True).otherwise(False)
        ) \
        .withColumn("saison_consultation",
            when(month(col("date_consultation")).isin([12, 1, 2]), "Hiver")
            .when(month(col("date_consultation")).isin([3, 4, 5]), "Printemps")
            .when(month(col("date_consultation")).isin([6, 7, 8]), "Ete")
            .otherwise("Automne")
        ) \
        .withColumn("periode_journee",
            when(hour(col("heure_debut_timestamp")) < 12, "Matin")
            .when(hour(col("heure_debut_timestamp")) < 18, "Apres-midi")
            .otherwise("Soir")
        ) \
        .withColumn("region", col("patient_region")) \
        .withColumn("sexe", col("patient_sexe")) \
        .withColumn("date_consultation_annee", year(col("date_consultation"))) \
        .withColumn("date_consultation_mois", month(col("date_consultation"))) \
        .filter(col("date_consultation").isNotNull())
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_consultations, "silver_consultations")
    
    return silver_consultations

@log_transformation
def create_silver_hospitalisations(bronze_hospitalisations, silver_patients, bronze_diagnostics, bronze_etablissements):
    """Crée la table Silver des hospitalisations enrichie."""
    
    # Conversion des types de données
    bronze_hospitalisations_clean = bronze_hospitalisations \
        .withColumn("jour_hospitalisation", col("jour_hospitalisation").cast("integer")) \
        .alias("h")
    
    patients_clean = silver_patients.select(
        "_sk_patient", 
        col("sexe").alias("patient_sexe"),
        "age", 
        "categorie_age", 
        "patient_region"
    ).alias("p")
    
    # Préparer les données des établissements avec région extraite du code postal
    etablissements_with_region = bronze_etablissements \
        .withColumn("region_code", 
            when(col("code_postal").isNotNull(), 
                 regexp_extract(col("code_postal"), r"^(\d{2})", 1))
            .otherwise("00")
        ).withColumn("region",
            when(col("region_code").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("region_code").isin(["14", "27", "50", "61", "76"]), "Normandie") 
            .when(col("region_code").isin(["02", "59", "60", "62", "80"]), "Hauts-de-France")
            .when(col("region_code").isin(["35", "22", "56", "29"]), "Bretagne")
            .when(col("region_code").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("region_code").isin(["17", "16", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["33", "24", "40", "47", "64"]), "Nouvelle-Aquitaine")
            .when(col("region_code").isin(["31", "09", "11", "12", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("region_code").isin(["69", "42", "43", "63", "03", "15", "07", "26", "38", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("region_code").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("region_code").isin(["54", "55", "57", "88"]), "Grand Est")
            .otherwise("Autre")
        )
    
    etablissements_clean = etablissements_with_region.select(
        "_sk",
        "identifiant_organisation",
        "finess",
        "raison_sociale_site",
        col("region").alias("etablissement_region"),
        "code_postal"
    ).alias("e")

    # Préparer les diagnostics
    diagnostics_clean = bronze_diagnostics.select(
        "_sk",
        "code_diag",
        "diagnostic"
    ).withColumn(
        "type_hospitalisation",
        when(col("diagnostic").rlike("(?i)urgence|aigu|trauma"), "Urgence")
        .when(col("diagnostic").rlike("(?i)chronique|long"), "Chronique")
        .when(col("diagnostic").rlike("(?i)prevention|depistage"), "Prevention")
        .when(col("diagnostic").rlike("(?i)chirurgie|operation"), "Chirurgical")
        .otherwise("Medical")
    ).alias("d")

    # Construction de la table silver avec des sélections explicites et jointures
    # ✅ CORRECTION: Jointure sur identifiant_organisation au lieu de _sk_etablissement
    silver_hospitalisations = bronze_hospitalisations_clean \
        .join(patients_clean, col("h._sk_patient") == col("p._sk_patient"), "left") \
        .join(diagnostics_clean, col("h._sk_diagnostic") == col("d._sk"), "left") \
        .join(etablissements_clean,
              col("h.identifiant_organisation") == col("e.identifiant_organisation"), "left") \
        .select(
            col("h.*"),
            # ✅ _sk_etablissement existe déjà dans Bronze h.*, pas besoin de le rajouter
            col("p.patient_sexe"),
            col("p.age"),
            col("p.categorie_age"),
            col("p.patient_region"),
            col("d.type_hospitalisation"),
            col("e.raison_sociale_site"),
            col("e.etablissement_region"),
            col("e.code_postal")
        ) \
        .withColumn("gravite_sejour",
            when(col("jour_hospitalisation") > 30, "Long")
            .when(col("jour_hospitalisation") > 7, "Moyen")
            .otherwise("Court")
        ) \
        .withColumn("saison_hospitalisation",
            when(month(col("date_admission")).isin([12, 1, 2]), "Hiver")
            .when(month(col("date_admission")).isin([3, 4, 5]), "Printemps")
            .when(month(col("date_admission")).isin([6, 7, 8]), "Ete")
            .otherwise("Automne")
        ) \
        .withColumn("readmission_risque",
            when((col("age") > 65) & (col("jour_hospitalisation") > 10), "Eleve")
            .when(col("type_hospitalisation") == "Chronique", "Modere")
            .otherwise("Faible")
        ) \
        .withColumn("region", col("patient_region")) \
        .withColumn("sexe", col("patient_sexe")) \
        .withColumn("date_admission_annee", year(col("date_admission"))) \
        .withColumn("date_admission_mois", month(col("date_admission"))) \
        .filter(col("date_admission").isNotNull())
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_hospitalisations, "silver_hospitalisations")
    
    return silver_hospitalisations

@log_transformation
def create_silver_deces(bronze_deces):
    """Crée la table Silver des décès enrichie."""
    
    # Calcul de l'âge à partir de la date de naissance et de la date de décès
    bronze_deces_with_age = bronze_deces \
        .withColumn("age", 
                   floor(datediff(col("date_deces"), col("date_naissance")) / 365.25)
        ) \
        .withColumn("categorie_age",
            when(col("age") < 18, "0-17")
            .when(col("age") < 30, "18-29")
            .when(col("age") < 45, "30-44")
            .when(col("age") < 60, "45-59")
            .when(col("age") < 75, "60-74")
            .otherwise("75+")
        )
    
    silver_deces = bronze_deces_with_age \
        .withColumn("departement_deces", when(col("code_lieu_deces").isNotNull(), regexp_extract(col("code_lieu_deces"), r"^(\d{2})", 1)).otherwise("00")) \
        .withColumn("region_deces",
            when(col("departement_deces").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
            .when(col("departement_deces").isin(["14", "27", "50", "61", "76"]), "Normandie")
            .when(col("departement_deces").isin(["02", "59", "60", "62", "80"]), "Hauts-de-France")
            .when(col("departement_deces").isin(["35", "22", "56", "29"]), "Bretagne")
            .when(col("departement_deces").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
            .when(col("departement_deces").isin(["17", "16", "79", "86", "87"]), "Nouvelle-Aquitaine")
            .when(col("departement_deces").isin(["33", "24", "40", "47", "64"]), "Nouvelle-Aquitaine")
            .when(col("departement_deces").isin(["31", "09", "11", "12", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
            .when(col("departement_deces").isin(["69", "42", "43", "63", "03", "15", "07", "26", "38", "73", "74"]), "Auvergne-Rhône-Alpes")
            .when(col("departement_deces").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
            .when(col("departement_deces").isin(["54", "55", "57", "88"]), "Grand Est")
            .when(col("departement_deces").isin(["51", "08", "10", "52"]), "Grand Est")
            .when(col("departement_deces").isin(["18", "28", "36", "37", "41", "45"]), "Centre-Val de Loire")
            .when(col("departement_deces").isin(["97"]), "Outre-Mer")
            .otherwise("Autre")
        ) \
        .withColumn("esperance_vie_atteinte",
            when(col("age") >= 80, "Longevite")
            .when(col("age") >= 65, "Retraite")
            .when(col("age") >= 18, "Adulte")
            .otherwise("Premature")
        ) \
        .withColumn("saison_deces",
            when(month(col("date_deces")).isin([12, 1, 2]), "Hiver")
            .when(month(col("date_deces")).isin([3, 4, 5]), "Printemps")
            .when(month(col("date_deces")).isin([6, 7, 8]), "Ete")
            .otherwise("Automne")
        ) \
        .withColumn("tranche_age_deces",
            when(col("age") < 1, "0-1 an")
            .when(col("age") < 18, "1-17 ans")
            .when(col("age") < 30, "18-29 ans")
            .when(col("age") < 45, "30-44 ans")
            .when(col("age") < 60, "45-59 ans")
            .when(col("age") < 75, "60-74 ans")
            .otherwise("75+ ans")
        ) \
        .withColumn("date_deces_annee", year(col("date_deces"))) \
        .withColumn("date_deces_mois", month(col("date_deces")))
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_deces, "silver_deces")
    
    return silver_deces

@log_transformation
def create_silver_etablissements(bronze_etablissements, bronze_hospitalisations, bronze_satisfaction):
    """Crée la table Silver des établissements avec indicateurs de performance."""
    
    # Conversion des types de données
    bronze_hospitalisations_clean = bronze_hospitalisations \
        .withColumn("jour_hospitalisation", col("jour_hospitalisation").cast("integer"))
    
    # ✅ CORRECTION : Agréger par établissement via hospitalisations
    hospitalisations_par_etablissement = bronze_hospitalisations_clean \
        .groupBy("identifiant_organisation") \
        .agg(
            count("*").alias("nb_hospitalisations_total"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        ) \
        .withColumnRenamed("identifiant_organisation", "finess_hospit").alias("hosp_stats")
    
    # Satisfaction par établissement
    satisfaction_par_etablissement = bronze_satisfaction \
        .groupBy("finess") \
        .agg(
            avg("score_all_ajust").alias("score_satisfaction_moyen"),
            avg("taux_reco_brut").alias("taux_recommandation_moyen"),
            count("*").alias("nb_campagnes_satisfaction")
        ) \
        .withColumnRenamed("finess", "finess_satisf").alias("sat_stats")
    
    # Construction finale avec vraies statistiques
    silver_etablissements = bronze_etablissements.alias("etab") \
        .join(hospitalisations_par_etablissement,
              col("etab.finess") == col("hosp_stats.finess_hospit"),
              "left") \
        .join(satisfaction_par_etablissement,
              col("etab.finess") == col("sat_stats.finess_satisf"),
              "left") \
        .select(
            col("etab.*"),
            col("hosp_stats.nb_hospitalisations_total"),
            col("hosp_stats.nb_patients_hospitalises"),
            col("hosp_stats.duree_moyenne_sejour"),
            col("hosp_stats.total_jours_hospitalisation"),
            col("sat_stats.score_satisfaction_moyen"),
            col("sat_stats.taux_recommandation_moyen"),
            col("sat_stats.nb_campagnes_satisfaction")
        ) \
        .withColumn("niveau_activite",
            when(col("nb_hospitalisations_total") > 100, "Tres_actif")
            .when(col("nb_hospitalisations_total") > 50, "Actif")
            .when(col("nb_hospitalisations_total") > 10, "Modere")
            .otherwise("Faible")
        ) \
        .withColumn("specialisation_etablissement",
            when(col("nb_hospitalisations_total").isNull(), "Consultation_seule")
            .otherwise("Hospitalisation")
        ) \
        .withColumn("performance_globale",
            when((col("score_satisfaction_moyen") > 80) & (col("duree_moyenne_sejour") < 10), "Excellente")
            .when((col("score_satisfaction_moyen") > 70) & (col("duree_moyenne_sejour") < 15), "Bonne")
            .when(col("score_satisfaction_moyen") > 60, "Satisfaisante")
            .otherwise("A_ameliorer")
        ) \
        .fillna({
            "nb_hospitalisations_total": 0,
            "nb_patients_hospitalises": 0,
            "duree_moyenne_sejour": 0,
            "total_jours_hospitalisation": 0,
            "score_satisfaction_moyen": 0,
            "taux_recommandation_moyen": 0,
            "nb_campagnes_satisfaction": 0
        })
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_etablissements, "silver_etablissements")
    
    return silver_etablissements

@log_transformation
def create_silver_professionnels(bronze_professionnels, bronze_consultations):
    """Crée la table Silver des professionnels de santé avec indicateurs d'activité."""
    
    activite_professionnels = bronze_consultations \
        .groupBy("id_prof_sante") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("id_patient").alias("nb_patients_uniques"),
            countDistinct("code_diag").alias("nb_diagnostics_distincts"),
            avg(year("date_consultation")).alias("annee_moyenne_activite"),
            min("date_consultation").alias("date_premiere_consultation"),
            max("date_consultation").alias("date_derniere_consultation")
        ).alias("act")
    
    silver_professionnels = bronze_professionnels.alias("prof") \
        .join(activite_professionnels, 
              col("prof.identifiant") == col("act.id_prof_sante"), "left") \
        .select(
            col("prof.*"),
            col("act.nb_consultations_total"),
            col("act.nb_patients_uniques"),
            col("act.nb_diagnostics_distincts"),
            col("act.annee_moyenne_activite"),
            col("act.date_premiere_consultation"),
            col("act.date_derniere_consultation")
        ) \
        .withColumn("niveau_activite",
            when(col("nb_consultations_total") > 1000, "Tres_actif")
            .when(col("nb_consultations_total") > 100, "Actif")
            .when(col("nb_consultations_total") > 10, "Occasionnel")
            .otherwise("Inactif")
        ) \
        .withColumn("experience_approximative",
            when(col("date_premiere_consultation").isNotNull(), 
                 year(current_date()) - year(col("date_premiere_consultation")))
            .otherwise(0)
        ) \
        .withColumn("productivite_moyenne",
            when(col("nb_patients_uniques") > 0, 
                 col("nb_consultations_total") / col("nb_patients_uniques"))
            .otherwise(0)
        ) \
        .withColumn("specialisation_cas",
            when(col("nb_consultations_total") > 0,
                 when(col("nb_diagnostics_distincts") / col("nb_consultations_total") < 0.1, "Specialiste")
                 .when(col("nb_diagnostics_distincts") / col("nb_consultations_total") > 0.5, "Generaliste")
                 .otherwise("Polyvalent")
            ).otherwise("Inconnu")
        ) \
        .fillna({
            "nb_consultations_total": 0,
            "nb_patients_uniques": 0,
            "nb_diagnostics_distincts": 0,
            "annee_moyenne_activite": 0,
            "productivite_moyenne": 0
        })
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_professionnels, "silver_professionnels")
    
    return silver_professionnels

@log_transformation
def create_silver_diagnostics(bronze_diagnostics, bronze_consultations, bronze_hospitalisations):
    """Crée la table Silver des diagnostics avec indicateurs de prévalence."""
    
    # Conversion des types de données
    bronze_hospitalisations_clean = bronze_hospitalisations \
        .withColumn("jour_hospitalisation", col("jour_hospitalisation").cast("integer"))
    
    # Prévalence dans les consultations
    prevalence_consultations = bronze_consultations \
        .groupBy("code_diag") \
        .agg(
            count("*").alias("nb_occurrences_consultations"),
            countDistinct("id_patient").alias("nb_patients_consultations"),
            countDistinct("id_prof_sante").alias("nb_professionnels_prescripteurs")
        ).alias("prev_cons")
    
    # Prévalence dans les hospitalisations
    prevalence_hospitalisations = bronze_hospitalisations_clean \
        .groupBy("code_diag") \
        .agg(
            count("*").alias("nb_occurrences_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalisations"),
            avg("jour_hospitalisation").alias("duree_moyenne_hospitalisation")
        ).alias("prev_hosp")
    
    silver_diagnostics = bronze_diagnostics.alias("diag") \
        .join(prevalence_consultations, "code_diag", "left") \
        .join(prevalence_hospitalisations, "code_diag", "left") \
        .select(
            col("diag.*"),
            col("prev_cons.nb_occurrences_consultations"),
            col("prev_cons.nb_patients_consultations"),
            col("prev_cons.nb_professionnels_prescripteurs"),
            col("prev_hosp.nb_occurrences_hospitalisations"),
            col("prev_hosp.nb_patients_hospitalisations"),
            col("prev_hosp.duree_moyenne_hospitalisation")
        ) \
        .withColumn("gravite_pathologie",
            when(col("nb_occurrences_hospitalisations").isNull(), "Benin")
            .when(col("duree_moyenne_hospitalisation") > 20, "Severe")
            .when(col("duree_moyenne_hospitalisation") > 7, "Modere")
            .otherwise("Leger")
        ) \
        .withColumn("prevalence_globale",
            when(col("nb_occurrences_consultations") > 1000, "Tres_frequente")
            .when(col("nb_occurrences_consultations") > 100, "Frequente")
            .when(col("nb_occurrences_consultations") > 10, "Occasionnelle")
            .otherwise("Rare")
        ) \
        .withColumn("taux_hospitalisation",
            when(col("nb_occurrences_consultations") > 0,
                 col("nb_occurrences_hospitalisations") / col("nb_occurrences_consultations"))
            .otherwise(0)
        ) \
        .withColumn("type_pathologie",
            when(col("diagnostic").rlike("(?i)chronique|long|permanent"), "Chronique")
            .when(col("diagnostic").rlike("(?i)aigu|urgence|sudden"), "Aigu")
            .when(col("diagnostic").rlike("(?i)prevention|depistage|vaccin"), "Prevention")
            .when(col("diagnostic").rlike("(?i)suivi|controle"), "Suivi")
            .otherwise("Autre")
        ) \
        .fillna({
            "nb_occurrences_consultations": 0,
            "nb_patients_consultations": 0,
            "nb_professionnels_prescripteurs": 0,
            "nb_occurrences_hospitalisations": 0,
            "nb_patients_hospitalisations": 0,
            "duree_moyenne_hospitalisation": 0,
            "taux_hospitalisation": 0
        })
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_diagnostics, "silver_diagnostics")
    
    return silver_diagnostics

@log_transformation
def create_silver_satisfaction(bronze_satisfaction):
    """Crée la table Silver de satisfaction avec indicateurs enrichis."""
    
    silver_satisfaction = bronze_satisfaction \
        .withColumn("niveau_satisfaction",
            when(col("score_all_ajust") >= 85, "Excellente")
            .when(col("score_all_ajust") >= 75, "Bonne")
            .when(col("score_all_ajust") >= 60, "Satisfaisante")
            .otherwise("Insatisfaisante")
        ) \
        .withColumn("performance_relative",
            when(col("score_all_ajust") > 80, "Au_dessus_attentes")
            .when(col("score_all_ajust") > 70, "Conforme_attentes")
            .otherwise("En_dessous_attentes")
        ) \
        .withColumn("taux_participation_categorise",
            when(col("participation") == "1- Obligatoire", "Obligatoire")
            .otherwise("Volontaire")
        ) \
        .withColumn("score_global_normalise",
            (col("score_all_ajust") - 50) / 50  # Normalisation 0-1
        )
    
    # Calcul des métriques de qualité
    compute_quality_metrics(silver_satisfaction, "silver_satisfaction")
    
    return silver_satisfaction

@log_transformation
def create_silver_mutuelle(bronze_mutuelle):
    """Crée la table Silver des mutuelles."""
    print("\n🏥 Création SILVER_MUTUELLE...")

    # ✅ CORRECTION: Utiliser uniquement les colonnes disponibles dans Bronze
    # Les colonnes bronze_mutuelle sont: _sk, _hash_record, id_mut, nom, adresse
    silver_mutuelle = bronze_mutuelle \
        .withColumn("type_mutuelle",
            when(col("nom").rlike("(?i)agirc|arrco|retraite"), "Retraite")
            .when(col("nom").rlike("(?i)etudiant|jeune"), "Étudiante")
            .when(col("nom").rlike("(?i)sante|medical"), "Santé")
            .when(col("nom").rlike("(?i)entreprise|salarie"), "Entreprise")
            .otherwise("Générale")
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_mutuelle, "silver_mutuelle")

    return silver_mutuelle

@log_transformation
def create_silver_medicaments(bronze_medicaments):
    """Crée la table Silver des médicaments."""
    print("\n💊 Création SILVER_MEDICAMENTS...")

    # ✅ CORRECTION: Utiliser uniquement les colonnes disponibles dans Bronze
    # Colonnes disponibles: code_cis, denomination, forme_pharmaceutique,
    # voies_d_administration, statut_administratif, titulaire, etc.
    silver_medicaments = bronze_medicaments \
        .withColumn("classe_therapeutique",
            when(col("denomination").rlike("(?i)antibio|amoxicilline|penicilline"), "Antibiotique")
            .when(col("denomination").rlike("(?i)antalgique|doliprane|paracetamol|ibuprofene"), "Antalgique")
            .when(col("denomination").rlike("(?i)antiinflammatoire|cortisone"), "Anti-inflammatoire")
            .when(col("denomination").rlike("(?i)cardio|beta|atenolol"), "Cardiovasculaire")
            .when(col("denomination").rlike("(?i)psycho|antidepresseur"), "Psychotrope")
            .otherwise("Autre")
        ) \
        .withColumn("disponibilite",
            when(col("etat_de_commercialisation") == "Commercialisee", "Disponible")
            .when(col("etat_de_commercialisation").rlike("(?i)arret"), "Arrêté")
            .otherwise("Non disponible")
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_medicaments, "silver_medicaments")

    return silver_medicaments

@log_transformation
def create_silver_laboratoire(bronze_laboratoire):
    """Crée la table Silver des laboratoires."""
    print("\n🔬 Création SILVER_LABORATOIRE...")

    # ✅ CORRECTION: Utiliser uniquement les colonnes disponibles dans Bronze
    # Colonnes disponibles: id_labo, laboratoire, pays
    silver_laboratoire = bronze_laboratoire \
        .withColumn("zone_geographique",
            when(col("pays") == "France", "France")
            .when(col("pays").isin(["ALLEMAGNE", "Allemagne", "ITALIE", "ESPAGNE"]), "Europe")
            .when(col("pays").isin(["USA", "ETATS-UNIS", "CANADA"]), "Amérique du Nord")
            .when(col("pays").isNotNull(), "International")
            .otherwise("Non défini")
        ) \
        .withColumn("type_laboratoire",
            when(col("laboratoire").rlike("(?i)sanofi|pfizer|novartis|roche"), "Grand groupe")
            .when(col("laboratoire").rlike("(?i)generique|arrow|biogaran|teva"), "Génériques")
            .otherwise("Spécialisé")
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_laboratoire, "silver_laboratoire")

    return silver_laboratoire

@log_transformation
def create_silver_salle(bronze_salle):
    """Crée la table Silver des salles."""
    print("\n🏥 Création SILVER_SALLE...")

    # ✅ CORRECTION: Utiliser uniquement les colonnes disponibles dans Bronze
    # Colonnes disponibles: id_salle, num_consultation, code_bloc, num_etage, num_salle
    silver_salle = bronze_salle \
        .withColumn("type_salle",
            when(col("code_bloc").rlike("(?i)bloc-[a-c]"), "Chirurgical")
            .when(col("code_bloc").rlike("(?i)bloc-[d-f]"), "Consultation")
            .otherwise("Standard")
        ) \
        .withColumn("categorie_etage",
            when(col("num_etage") == "Etage-0", "Rez-de-chaussée")
            .when(col("num_etage") == "Etage-1", "Premier étage")
            .when(col("num_etage").rlike("Etage-[23]"), "Étages intermédiaires")
            .otherwise("Étages supérieurs")
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_salle, "silver_salle")

    return silver_salle

@log_transformation
def create_silver_specialites(bronze_specialites):
    """Crée la table Silver des spécialités."""
    print("\n🎯 Création SILVER_SPECIALITES...")

    # ✅ CORRECTION: Utiliser uniquement les colonnes disponibles dans Bronze
    # Colonnes disponibles: code_specialite, fonction, specialite
    silver_specialites = bronze_specialites \
        .withColumn("domaine_medical",
            when(col("specialite").rlike("(?i)chirurg"), "Chirurgie")
            .when(col("specialite").rlike("(?i)cardio"), "Cardiologie")
            .when(col("specialite").rlike("(?i)pedia|pediatr"), "Pédiatrie")
            .when(col("specialite").rlike("(?i)onco|cancer"), "Oncologie")
            .when(col("specialite").rlike("(?i)anesthes"), "Anesthésiologie")
            .when(col("specialite").rlike("(?i)gynec|obstet"), "Gynécologie")
            .otherwise("Autre")
        ) \
        .withColumn("categorie_fonction",
            when(col("fonction") == "Medecin", "Médical")
            .when(col("fonction").rlike("(?i)infirmier|aide"), "Paramédical")
            .when(col("fonction").rlike("(?i)pharmacien"), "Pharmacie")
            .otherwise("Autre")
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_specialites, "silver_specialites")

    return silver_specialites

@log_transformation
def create_silver_satisfaction(bronze_satisfaction):
    """Crée la table Silver des données de satisfaction."""
    print("\n😊 Création SILVER_SATISFACTION...")

    # La table satisfaction est déjà bien structurée dans Bronze
    # On ajoute juste quelques enrichissements
    silver_satisfaction = bronze_satisfaction \
        .withColumn("niveau_satisfaction",
            when(col("score_all_ajust") >= 85, "Excellente")
            .when(col("score_all_ajust") >= 75, "Très Bonne")
            .when(col("score_all_ajust") >= 65, "Bonne")
            .when(col("score_all_ajust") >= 50, "Satisfaisante")
            .otherwise("À améliorer")
        ) \
        .withColumn("performance_relative",
            when(col("classement") == "A", "Au dessus des attentes")
            .when(col("classement") == "B", "Conforme aux attentes")
            .when(col("classement") == "C", "En dessous des attentes")
            .otherwise("Non classé")
        ) \
        .withColumn("taux_participation_categorise",
            when(col("participation") == "1- Obligatoire", "Obligatoire")
            .when(col("participation") == "2- Volontaire", "Volontaire")
            .otherwise("Non défini")
        ) \
        .withColumn("score_global_normalise",
            when(col("score_all_ajust").isNotNull(),
                 col("score_all_ajust") / 100.0)
            .otherwise(0.0)
        )

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_satisfaction, "silver_satisfaction")

    return silver_satisfaction

@log_transformation
def create_silver_indicators_metier(silver_consultations, silver_hospitalisations, silver_deces, silver_satisfaction):
    """Crée la table Silver des indicateurs métier agrégés."""
    
    # Indicateurs consultations
    indicateurs_consultations = silver_consultations \
        .groupBy("region", "categorie_age", "sexe", year("date_consultation").alias("annee")) \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("id_patient").alias("nb_patients_uniques"),
            countDistinct("id_prof_sante").alias("nb_professionnels"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation")
        ) \
        .withColumn("type_indicateur", lit("consultations"))

    # Indicateurs hospitalisations
    indicateurs_hospitalisations = silver_hospitalisations \
        .groupBy("region", "categorie_age", "sexe", year("date_admission").alias("annee")) \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            spark_sum("jour_hospitalisation").alias("total_jours_hospitalisation")
        ) \
        .withColumn("type_indicateur", lit("hospitalisations"))

    # Indicateurs décès
    indicateurs_deces = silver_deces \
        .groupBy("region_deces", "tranche_age_deces", "sexe", year("date_deces").alias("annee")) \
        .agg(
            count("*").alias("nb_deces"),
            avg("age").alias("age_moyen_deces"),
            countDistinct("departement_deces").alias("nb_departements_touches")
        ) \
        .withColumn("type_indicateur", lit("deces")) \
        .withColumnRenamed("region_deces", "region") \
        .withColumnRenamed("tranche_age_deces", "categorie_age")

    # Indicateurs satisfaction
    indicateurs_satisfaction = silver_satisfaction \
        .groupBy("region", "niveau_satisfaction") \
        .agg(
            count("*").alias("nb_etablissements"),
            avg("score_all_ajust").alias("score_satisfaction_moyen"),
            avg("taux_reco_brut").alias("taux_recommandation_moyen")
        ) \
        .withColumn("type_indicateur", lit("satisfaction")) \
        .withColumn("categorie_age", lit("Tous")) \
        .withColumn("sexe", lit("Tous")) \
        .withColumn("annee", lit(2020))

    # Union de tous les indicateurs
    silver_indicators_metier = indicateurs_consultations \
        .unionByName(indicateurs_hospitalisations, allowMissingColumns=True) \
        .unionByName(indicateurs_deces, allowMissingColumns=True) \
        .unionByName(indicateurs_satisfaction, allowMissingColumns=True)

    # Calcul des métriques de qualité
    compute_quality_metrics(silver_indicators_metier, "silver_indicators_metier")
    
    return silver_indicators_metier

def main():
    """Exécute le pipeline complet du layer Silver."""
    print("""
    ╔══════════════════════════════════════════╗
    ║           SILVER LAYER PIPELINE          ║
    ║     Enrichissement et Indicateurs        ║
    ╚══════════════════════════════════════════╝
    """)
    
    try:
        # Initialisation Spark
        spark = get_spark_session()

        # Initialiser le schéma silver dans le metastore
        initialize_for_layer(spark, "silver")

        print("\n📥 Lecture des données Bronze...")
        # Tables principales (noms au singulier dans Bronze)
        bronze_patients = read_bronze_table(spark, "patient")
        bronze_consultations = read_bronze_table(spark, "consultation")
        bronze_hospitalisations = read_bronze_table(spark, "hospitalisations")
        bronze_deces = read_bronze_table(spark, "deces_2019")
        bronze_etablissements = read_bronze_table(spark, "etablissement_sante")
        bronze_professionnels = read_bronze_table(spark, "professionnel_de_sante")
        bronze_diagnostics = read_bronze_table(spark, "diagnostic")
        bronze_satisfaction = read_bronze_table(spark, "satisfaction_mco_2020")
        
        # Tables de référence
        bronze_mutuelle = read_bronze_table(spark, "mutuelle")
        bronze_medicaments = read_bronze_table(spark, "medicaments")
        bronze_laboratoire = read_bronze_table(spark, "laboratoire")
        bronze_salle = read_bronze_table(spark, "salle")
        bronze_specialites = read_bronze_table(spark, "specialites")

        print("\n🏗️  Construction des tables Silver...")
        # Tables principales
        silver_patients = create_silver_patients(bronze_patients, bronze_consultations, bronze_hospitalisations)
        silver_consultations = create_silver_consultations(bronze_consultations, silver_patients, bronze_diagnostics, bronze_professionnels, bronze_etablissements)
        silver_hospitalisations = create_silver_hospitalisations(bronze_hospitalisations, silver_patients, bronze_diagnostics, bronze_etablissements)
        silver_deces = create_silver_deces(bronze_deces)
        silver_etablissements = create_silver_etablissements(bronze_etablissements, bronze_hospitalisations, bronze_satisfaction)
        silver_professionnels = create_silver_professionnels(bronze_professionnels, bronze_consultations)
        silver_diagnostics = create_silver_diagnostics(bronze_diagnostics, bronze_consultations, bronze_hospitalisations)
        silver_satisfaction = create_silver_satisfaction(bronze_satisfaction)

        # Tables de référence
        silver_mutuelle = create_silver_mutuelle(bronze_mutuelle)
        silver_medicaments = create_silver_medicaments(bronze_medicaments)
        silver_laboratoire = create_silver_laboratoire(bronze_laboratoire)
        silver_salle = create_silver_salle(bronze_salle)
        silver_specialites = create_silver_specialites(bronze_specialites)

        # Indicateurs métier
        silver_indicators = create_silver_indicators_metier(silver_consultations, silver_hospitalisations, silver_deces, silver_satisfaction)

        print("\n💾 Écriture des tables Silver...")
        # Tables principales (noms cohérents avec Gold)
        write_silver_table(silver_patients, "patient")
        write_silver_table(silver_consultations, "consultation")
        write_silver_table(silver_hospitalisations, "hospitalisations")
        write_silver_table(silver_deces, "deces_2019")
        write_silver_table(silver_etablissements, "etablissement_sante")
        write_silver_table(silver_professionnels, "professionnel_de_sante")
        write_silver_table(silver_diagnostics, "diagnostic")
        write_silver_table(silver_satisfaction, "satisfaction")

        # Tables de référence
        write_silver_table(silver_mutuelle, "mutuelle")
        write_silver_table(silver_medicaments, "medicaments")
        write_silver_table(silver_laboratoire, "laboratoire")
        write_silver_table(silver_salle, "salle")
        write_silver_table(silver_specialites, "specialites")

        # Indicateurs
        write_silver_table(silver_indicators, "indicators_metier")

        # Rapport final
        print("""
    🎉 PIPELINE SILVER TERMINÉ AVEC SUCCÈS!

    📊 TABLES SILVER CRÉÉES:
    ✅ Toutes les tables principales et de référence

    🎯 INDICATEURS PRÉPARÉS:
    • Taux de consultation par établissement
    • Taux d'hospitalisation par diagnostic
    • Décès par région et âge
    • Satisfaction par établissement
    • Performance des professionnels
    • Prévalence des pathologies

    📈 MÉTRIQUES DE QUALITÉ:
    • Monitoring des temps d'exécution
    • Taux de nulls contrôlés
    • Jointures optimisées avec alias

    🚀 PRÊT POUR LE LAYER GOLD!
        """)

        spark.stop()

    except Exception as e:
        print(f"💥 Erreur pipeline Silver: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()