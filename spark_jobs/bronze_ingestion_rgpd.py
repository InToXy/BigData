#!/usr/bin/env python3
"""
Script d'ingestion Bronze avec mappings RGPD conformes
Implémente les 8 mappings définis pour le projet médical CHU
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, regexp_replace, when, coalesce,
    to_date, year, month, quarter, substring, length, md5, sha2,
    current_timestamp, concat_ws, date_format, datediff, floor,
    expr, monotonically_increasing_id
)
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bucket": "bronze"
}

POSTGRES_CONFIG = {
    "host": "chu_postgres_data",
    "port": "5432",
    "database": "healthcare_data",
    "user": "admin",
    "password": "admin123"
}

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# ============================================================
# SPARK SESSION
# ============================================================

def get_spark_session():
    """Crée une session Spark avec configuration S3A."""
    jars_dir = "/home/jovyan/jars"
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
    jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
    
    spark = SparkSession.builder \
        .appName("Bronze_Ingestion_RGPD") \
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
    print("✅ Spark session créée")
    return spark

# ============================================================
# FONCTIONS UTILITAIRES RGPD
# ============================================================

def hash_pii(column):
    """Hash MD5 pour anonymisation RGPD."""
    return when(col(column).isNotNull(), md5(col(column).cast("string"))).otherwise(lit(None))

def add_metadata(df, source_system, source_table):
    """Ajoute les métadonnées techniques."""
    return df \
        .withColumn("_sk", monotonically_increasing_id()) \
        .withColumn("_hash_record", md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]))) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_source_table", lit(source_table)) \
        .withColumn("_ingestion_date", current_timestamp()) \
        .withColumn("_version", lit(1)) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_is_deleted", lit(False))

def validate_code_postal(cp_col):
    """Valide un code postal français (5 chiffres)."""
    return when(
        col(cp_col).rlike("^[0-9]{5}$"),
        col(cp_col)
    ).otherwise(lit(None))

def validate_age(age_col):
    """Valide un âge entre 0 et 150."""
    return when(
        (col(age_col) >= 0) & (col(age_col) <= 150),
        col(age_col)
    ).otherwise(lit(None))

def normalize_pays(pays_col):
    """Normalise le pays avec défaut FRANCE."""
    return when(
        (col(pays_col).isNotNull()) & (trim(col(pays_col)) != ""),
        upper(trim(col(pays_col)))
    ).otherwise(lit("FRANCE"))

# ============================================================
# MAPPING 1: DECES (CSV)
# ============================================================

def process_deces(spark):
    """Traite le fichier deces.csv avec anonymisation RGPD - ANNÉE 2019 UNIQUEMENT"""
    print("\n" + "="*80)
    print("� TRAITEMENT: deces.csv (ANNÉE 2019 UNIQUEMENT)")
    print("="*80)
    
    # Lecture CSV
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("encoding", "UTF-8") \
        .csv("/data/source/DECES EN FRANCE/deces.csv")
    
    total_count = df.count()
    print(f"   📊 Total lignes fichier: {total_count:,}")
    
    # FILTRE: Uniquement l'année 2019
    df = df.filter(year(col("date_deces")) == 2019)
    print(f"   🔍 Lignes 2019: {df.count():,}")
    
    # Application du mapping RGPD
    df_bronze = df.select(
        # PII anonymisées
        hash_pii("nom").alias("nom_anonymized"),
        hash_pii("prenom").alias("prenom_anonymized"),
        when(col("prenom").isNotNull(), upper(substring(trim(col("prenom")), 1, 1))).alias("initiale_prenom"),
        
        # Données normalisées
        upper(trim(col("sexe"))).alias("sexe"),
        to_date(col("date_naissance"), "yyyy-MM-dd").alias("date_naissance"),
        year(to_date(col("date_naissance"), "yyyy-MM-dd")).alias("date_naissance_annee"),
        month(to_date(col("date_naissance"), "yyyy-MM-dd")).alias("date_naissance_mois"),
        
        trim(col("code_lieu_naissance")).alias("code_lieu_naissance"),
        upper(trim(col("lieu_naissance"))).alias("lieu_naissance"),
        normalize_pays("pays_naissance").alias("pays_naissance"),
        
        to_date(col("date_deces"), "yyyy-MM-dd").alias("date_deces"),
        year(to_date(col("date_deces"), "yyyy-MM-dd")).alias("date_deces_annee"),
        month(to_date(col("date_deces"), "yyyy-MM-dd")).alias("date_deces_mois"),
        
        trim(col("code_lieu_deces")).alias("code_lieu_deces"),
        trim(col("numero_acte_deces")).alias("numero_acte_deces")
    )
    
    # Enrichissement géographique (département depuis code_lieu_deces)
    df_bronze = df_bronze.withColumn("departement", substring(col("code_lieu_deces"), 1, 2))
    
    # Identifier la région depuis le département (mapping simplifié)
    df_bronze = df_bronze.withColumn(
        "region",
        when(col("departement").isin(["75", "77", "78", "91", "92", "93", "94", "95"]), "Ile-de-France")
        .when(col("departement").isin(["44", "49", "53", "72", "85"]), "Pays de la Loire")
        .when(col("departement").isin(["24", "33", "40", "47", "64", "79", "86", "87"]), "Nouvelle-Aquitaine")
        .when(col("departement").isin(["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]), "Occitanie")
        .when(col("departement").isin(["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"]), "Auvergne-Rhône-Alpes")
        .when(col("departement").isin(["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]), "Grand Est")
        .when(col("departement").isin(["14", "27", "50", "61", "76"]), "Normandie")
        .when(col("departement").isin(["22", "29", "35", "56"]), "Bretagne")
        .when(col("departement").isin(["18", "28", "36", "37", "41", "45"]), "Centre-Val de Loire")
        .when(col("departement").isin(["21", "25", "39", "58", "70", "71", "89", "90"]), "Bourgogne-Franche-Comté")
        .when(col("departement").isin(["02", "59", "60", "62", "80"]), "Hauts-de-France")
        .when(col("departement").isin(["04", "05", "06", "13", "83", "84"]), "Provence-Alpes-Côte d'Azur")
        .when(col("departement").isin(["2A", "2B"]), "Corse")
        .otherwise("Autre")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "deces")
    
    # Filtrer uniquement 2019 pour réduire le volume
    df_bronze = df_bronze.filter(col("date_deces_annee") == 2019)
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze (2019 uniquement): {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/deces/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 2: ETABLISSEMENTS (CSV)
# ============================================================

def process_etablissements(spark):
    """Traite etablissement_sante.csv avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: etablissement_sante.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Etablissement de SANTE/etablissement_sante.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiants (non hashés pour jointures) - finess_site souvent vide, utiliser identifiant_organisation
        coalesce(trim(col("finess_site")), trim(col("identifiant_organisation"))).alias("finess"),
        trim(col("finess_etablissement_juridique")).alias("finess_juridique"),
        trim(col("identifiant_organisation")).alias("identifiant_organisation"),
        
        # Données publiques normalisées
        upper(trim(col("raison_sociale_site"))).alias("raison_sociale"),
        upper(trim(col("enseigne_commerciale_site"))).alias("enseigne_commerciale"),
        trim(col("siren_site")).alias("siren"),
        trim(col("siret_site")).alias("siret"),
        
        # PII géographiques hashées
        hash_pii("adresse").alias("adresse_hash"),
        trim(col("numero_voie")).alias("numero_voie"),
        upper(trim(col("type_voie"))).alias("type_voie"),
        hash_pii("voie").alias("voie_hash"),
        
        validate_code_postal("code_postal").alias("code_postal"),
        trim(col("code_commune")).alias("code_commune"),
        upper(trim(col("commune"))).alias("commune"),
        trim(col("cedex")).alias("cedex"),
        normalize_pays("pays").alias("pays"),
        
        # Contacts hashés
        hash_pii("telephone").alias("telephone_hash"),
        hash_pii("telephone_2").alias("telephone2_hash"),
        hash_pii("telecopie").alias("telecopie_hash"),
        hash_pii("email").alias("email_hash")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "etablissement_sante")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/etablissements/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 3: PROFESSIONNELS SANTE (CSV)
# ============================================================

def process_professionnels(spark):
    """Traite professionnel_sante.csv avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: professionnel_sante.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Etablissement de SANTE/professionnel_sante.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiant non hashé
        trim(col("identifiant")).alias("identifiant_original"),
        upper(trim(col("type_identifiant"))).alias("type_identifiant"),
        
        # PII anonymisées
        upper(trim(col("civilite"))).alias("civilite"),
        hash_pii("nom").alias("nom_anonymized"),
        hash_pii("prenom").alias("prenom_anonymized"),
        when(col("prenom").isNotNull(), upper(substring(trim(col("prenom")), 1, 1))).alias("initiale_prenom"),
        
        # Données métier normalisées
        upper(trim(col("categorie_professionnelle"))).alias("categorie_professionnelle"),
        upper(trim(col("profession"))).alias("profession"),
        upper(trim(col("specialite"))).alias("specialite"),
        upper(trim(col("commune"))).alias("commune")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "professionnel_sante")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/professionnels_sante/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 4: HOSPITALISATIONS (CSV)
# ============================================================

def process_hospitalisations(spark):
    """Traite Hospitalisations.csv avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: Hospitalisations.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Hospitalisation/Hospitalisations.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiants non hashés (pour jointures)
        trim(col("Num_Hospitalisation")).alias("num_hospitalisation_original"),
        col("Id_patient").cast(IntegerType()).alias("id_patient_original"),
        trim(col("identifiant_organisation")).alias("identifiant_organisation"),
        
        # Données médicales
        upper(trim(col("Code_diagnostic"))).alias("code_diagnostic"),
        trim(col("Suite_diagnostic_consultation")).alias("suite_diagnostic_consultation"),
        
        # Dates - Format dd/MM/yyyy
        to_date(col("Date_Entree"), "dd/MM/yyyy").alias("date_entree"),
        year(to_date(col("Date_Entree"), "dd/MM/yyyy")).alias("date_entree_annee"),
        month(to_date(col("Date_Entree"), "dd/MM/yyyy")).alias("date_entree_mois"),
        
        # Durée validée
        when(
            (col("Jour_Hospitalisation") >= 0) & (col("Jour_Hospitalisation") <= 365),
            col("Jour_Hospitalisation").cast(IntegerType())
        ).otherwise(lit(None)).alias("jour_hospitalisation")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "hospitalisations")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/hospitalisations/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 5: SATISFACTION (CSV)
# ============================================================

def process_satisfaction(spark):
    """Traite ESATIS48H_MCO_recueil2017_donnees.csv avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: ESATIS48H_MCO_recueil2017_donnees.csv")
    print("="*60)
    
    csv_path = "file:///data/source/Satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(csv_path)
    
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiants
        trim(col("finess")).alias("finess"),
        upper(trim(col("rs_finess"))).alias("rs_finess"),
        trim(col("finess_geo")).alias("identifiant_organisation"),
        upper(trim(col("rs_finess_geo"))).alias("rs_finess_geo"),
        upper(trim(col("region"))).alias("region"),
        
        # Participation
        trim(col("participation")).alias("participation"),
        trim(col("Depot")).alias("depot"),
        
        # Scores (conversion float)
        col("nb_rep_score_all_rea_ajust").cast(IntegerType()).alias("nb_rep_score_all_rea_ajust"),
        regexp_replace(col("score_all_rea_ajust"), ",", ".").cast(FloatType()).alias("score_all_ajust"),
        trim(col("classement")).alias("classement"),
        trim(col("evolution")).alias("evolution"),
        
        col("nb_rep_score_accueil_rea_ajust").cast(IntegerType()).alias("nb_rep_score_accueil_rea_ajust"),
        regexp_replace(col("score_accueil_rea_ajust"), ",", ".").cast(FloatType()).alias("score_accueil_rea_ajust"),
        
        col("nb_rep_score_PECinf_rea_ajust").cast(IntegerType()).alias("nb_rep_score_pecinf_rea_ajust"),
        regexp_replace(col("score_PECinf_rea_ajust"), ",", ".").cast(FloatType()).alias("score_pecinf_rea_ajust"),
        
        col("nb_rep_score_PECmed_rea_ajust").cast(IntegerType()).alias("nb_rep_score_pecmed_rea_ajust"),
        regexp_replace(col("score_PECmed_rea_ajust"), ",", ".").cast(FloatType()).alias("score_pecmed_rea_ajust"),
        
        col("nb_rep_score_chambre_rea_ajust").cast(IntegerType()).alias("nb_rep_score_chambre_rea_ajust"),
        regexp_replace(col("score_chambre_rea_ajust"), ",", ".").cast(FloatType()).alias("score_chambre_rea_ajust"),
        
        col("nb_rep_score_repas_rea_ajust").cast(IntegerType()).alias("nb_rep_score_repas_rea_ajust"),
        regexp_replace(col("score_repas_rea_ajust"), ",", ".").cast(FloatType()).alias("score_repas_rea_ajust"),
        
        col("nb_rep_score_sortie_rea_ajust").cast(IntegerType()).alias("nb_rep_score_sortie_rea_ajust"),
        regexp_replace(col("score_sortie_rea_ajust"), ",", ".").cast(FloatType()).alias("score_sortie_rea_ajust")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "CSV", "satisfaction_mco_2017")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/satisfaction_mco_2017/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 6: PATIENTS (PostgreSQL)
# ============================================================

def process_patients(spark):
    """Traite la table patients PostgreSQL avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: patients (PostgreSQL)")
    print("="*60)
    
    jdbc_options = {
        "url": POSTGRES_JDBC_URL,
        "dbtable": "patients",
        "user": POSTGRES_CONFIG["user"],
        "password": POSTGRES_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read.format("jdbc").options(**jdbc_options).load()
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiant non hashé
        col("Id_patient").cast(IntegerType()).alias("id_patient_original"),
        
        # PII anonymisées
        hash_pii("Nom").alias("nom_anonymized"),
        hash_pii("Prenom").alias("prenom_anonymized"),
        when(col("Prenom").isNotNull(), upper(substring(trim(col("Prenom")), 1, 1))).alias("initiale_prenom"),
        
        # Données démographiques
        to_date(col("Date"), "M/d/yyyy").alias("date_naissance"),
        year(to_date(col("Date"), "M/d/yyyy")).alias("date_naissance_annee"),
        month(to_date(col("Date"), "M/d/yyyy")).alias("date_naissance_mois"),
        validate_age("Age").alias("age_valide"),
        upper(trim(col("Sexe"))).alias("sexe"),
        
        # Géographie anonymisée + extraction département
        hash_pii("Adresse").alias("adresse_hash"),
        upper(trim(col("Ville"))).alias("ville"),
        validate_code_postal("Code_Postal").alias("code_postal"),
        substring(col("Code_Postal"), 1, 2).alias("departement"),
        
        # Contacts anonymisés
        hash_pii("Telephone").alias("telephone_hash"),
        hash_pii("Mail").alias("email_hash"),
        hash_pii("Numero_Securite_Sociale").alias("numero_secu_hash"),
        
        # Médecin traitant (identifiant, non PII)
        trim(col("Medecin_Traitant")).alias("medecin_traitant_id")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "POSTGRES", "patients")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/patients/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 7: CONSULTATIONS (PostgreSQL)
# ============================================================

def process_consultations(spark):
    """Traite la table consultations PostgreSQL avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: consultations (PostgreSQL)")
    print("="*60)
    
    jdbc_options = {
        "url": POSTGRES_JDBC_URL,
        "dbtable": "consultations",
        "user": POSTGRES_CONFIG["user"],
        "password": POSTGRES_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read.format("jdbc").options(**jdbc_options).load()
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Identifiants non hashés
        trim(col("Num_Consultation")).alias("num_consultation_original"),
        col("Id_patient").cast(IntegerType()).alias("id_patient_original"),
        trim(col("identifiant_organisation")).alias("identifiant_organisation"),
        
        # Données médicales
        upper(trim(col("Code_diagnostic"))).alias("code_diagnostic"),
        
        # Dates
        to_date(col("Date_Consultation"), "M/d/yyyy").alias("date_consultation"),
        year(to_date(col("Date_Consultation"), "M/d/yyyy")).alias("date_consultation_annee"),
        month(to_date(col("Date_Consultation"), "M/d/yyyy")).alias("date_consultation_mois"),
        quarter(to_date(col("Date_Consultation"), "M/d/yyyy")).alias("date_consultation_trimestre"),
        
        # Coût
        regexp_replace(col("Cout"), ",", ".").cast(FloatType()).alias("cout_consultation")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "POSTGRES", "consultations")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/consultations/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAPPING 8: DIAGNOSTICS (PostgreSQL)
# ============================================================

def process_diagnostics(spark):
    """Traite la table diagnostics PostgreSQL avec mapping RGPD."""
    print("\n" + "="*60)
    print("📊 Traitement: diagnostics (PostgreSQL)")
    print("="*60)
    
    jdbc_options = {
        "url": POSTGRES_JDBC_URL,
        "dbtable": "diagnostics",
        "user": POSTGRES_CONFIG["user"],
        "password": POSTGRES_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read.format("jdbc").options(**jdbc_options).load()
    print(f"   Lignes source: {df.count()}")
    
    # Mapping RGPD
    df_bronze = df.select(
        # Données de référence (non PII)
        upper(trim(col("Code_diagnostic"))).alias("code_diagnostic"),
        upper(trim(col("Description"))).alias("description_diagnostic"),
        upper(trim(col("Categorie"))).alias("categorie_diagnostic"),
        upper(trim(col("Gravite"))).alias("gravite"),
        
        # Niveau de gravité numérique
        when(upper(col("Gravite")) == "FAIBLE", 1)
        .when(upper(col("Gravite")).isin("MODÉRÉE", "MODEREE"), 2)
        .when(upper(col("Gravite")).isin("ÉLEVÉE", "ELEVEE"), 3)
        .otherwise(lit(None)).cast(IntegerType()).alias("gravite_niveau")
    )
    
    # Ajout métadonnées
    df_bronze = add_metadata(df_bronze, "POSTGRES", "diagnostics")
    
    count = df_bronze.count()
    print(f"   ✅ Lignes bronze: {count}")
    
    # Écriture MinIO
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/diagnostics/"
    df_bronze.write.mode("overwrite").parquet(output_path)
    print(f"   💾 Écrit dans: {output_path}")
    
    return count

# ============================================================
# MAIN - ORCHESTRATION
# ============================================================

def main():
    """Orchestre l'ingestion complète Bronze avec mappings RGPD."""
    print("\n" + "="*60)
    print("🏥 INGESTION BRONZE - CONFORMITÉ RGPD")
    print("   📅 DÉCÈS: ANNÉE 2019 UNIQUEMENT")
    print("="*60)
    print(f"Début: {datetime.now()}")
    
    spark = get_spark_session()
    
    results = {}
    
    try:
        # Traitement de toutes les sources
        results["deces"] = process_deces(spark)
        results["etablissements"] = process_etablissements(spark)
        results["professionnels"] = process_professionnels(spark)
        results["hospitalisations"] = process_hospitalisations(spark)
        results["satisfaction"] = process_satisfaction(spark)
        results["patients"] = process_patients(spark)
        results["consultations"] = process_consultations(spark)
        results["diagnostics"] = process_diagnostics(spark)
        
        # Résumé
        print("\n" + "="*60)
        print("✅ RÉSUMÉ DE L'INGESTION BRONZE")
        print("="*60)
        total = 0
        for table, count in results.items():
            print(f"   {table:25s}: {count:>10,} lignes")
            total += count
        print("   " + "-"*40)
        print(f"   {'TOTAL':25s}: {total:>10,} lignes")
        print("="*60)
        print(f"Fin: {datetime.now()}")
        print("✅ Pipeline Bronze terminé avec succès!")
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
