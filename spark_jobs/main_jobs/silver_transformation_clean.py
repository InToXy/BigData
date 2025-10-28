#!/usr/bin/env python3
"""
silver_transformation.py
========================
Transformation Bronze → Silver pour le CHU Data Warehouse

Transformations:
- Bronze (données brutes normalisées) → Silver (schéma en étoile)
- Création dimensions: dim_patient, dim_etablissement, dim_temps
- Création faits: fact_consultation, fact_hospitalisation, fact_deces
- Métriques: metrique_satisfaction, metrique_activite

Architecture:
- Input: s3a://bronze/* (tables Parquet)
- Output: s3a://silver/* (star schema)

Usage:
    docker exec chu_jupyter spark-submit \
      --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
      /home/jovyan/jobs/main_jobs/silver_transformation.py
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min, max as spark_max,
    when, lit, datediff, floor, year, month, quarter, dayofmonth, dayofweek, date_format,
    row_number, rank, concat_ws, upper, trim, substring, length, to_date, current_timestamp,
    regexp_extract, lower
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bronze_bucket": "bronze",
    "silver_bucket": "silver"
}

# ============================================================
# SESSION SPARK
# ============================================================
def get_spark_session():
    """Crée la session Spark avec configuration S3A."""
    try:
        builder = SparkSession.builder \
            .appName("Silver Layer Transformation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.parquet.compression.codec", "snappy")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Silver session créée")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur création Spark: {e}")
        raise

# ============================================================
# LECTURE BRONZE
# ============================================================
def read_bronze_table(spark, table_name):
    """Lit une table du layer Bronze."""
    try:
        bronze_path = f"s3a://{MINIO_CONFIG['bronze_bucket']}/{table_name}"
        df = spark.read.parquet(bronze_path)
        count_rows = df.count()
        print(f"✅ Bronze '{table_name}' lu: {count_rows:,} lignes")
        return df
    except Exception as e:
        print(f"⚠️  Table Bronze '{table_name}' non trouvée: {e}")
        return None

# ============================================================
# ÉCRITURE SILVER
# ============================================================
def write_silver_table(df, table_name):
    """Écrit une table dans le layer Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df.write.mode("overwrite").parquet(silver_path)
        count_rows = df.count()
        print(f"✅ Silver '{table_name}' écrit: {count_rows:,} lignes")
    except Exception as e:
        print(f"❌ Erreur écriture Silver '{table_name}': {e}")
        raise

# ============================================================
# DIMENSIONS
# ============================================================
def create_dim_patient(spark):
    """Crée la dimension Patient enrichie."""
    print("\n🔵 Création dim_patient...")
    
    patients_bronze = read_bronze_table(spark, "patients")
    if patients_bronze is None:
        print("⚠️  Pas de table patients dans Bronze, création dimension vide")
        return spark.createDataFrame([], schema="sk_patient INT, patient_nk STRING")
    
    # Construction robuste avec vérification de colonnes
    select_exprs = []
    
    # Clés
    if "_sk_patient" in patients_bronze.columns:
        select_exprs.append(col("_sk_patient").alias("sk_patient"))
    else:
        select_exprs.append(row_number().over(Window.orderBy(lit(1))).alias("sk_patient"))
    
    if "id_patient" in patients_bronze.columns:
        select_exprs.append(col("id_patient").alias("patient_nk"))
    else:
        select_exprs.append(lit("UNKNOWN").alias("patient_nk"))
    
    # Attributs démographiques
    if "nom" in patients_bronze.columns:
        select_exprs.append(col("nom"))
    else:
        select_exprs.append(lit("ANONYME").alias("nom"))
    
    if "prenom" in patients_bronze.columns:
        select_exprs.append(col("prenom"))
    else:
        select_exprs.append(lit("ANONYME").alias("prenom"))
    
    if "sexe" in patients_bronze.columns:
        select_exprs.append(col("sexe"))
    else:
        select_exprs.append(lit("I").alias("sexe"))
    
    # Date de naissance et calcul âge
    if "date_naissance" in patients_bronze.columns:
        select_exprs.append(to_date(col("date_naissance")).alias("date_naissance"))
        age_col = (year(current_timestamp()) - year(to_date(col("date_naissance"))))
    elif "Date" in patients_bronze.columns:
        select_exprs.append(to_date(col("Date")).alias("date_naissance"))
        age_col = (year(current_timestamp()) - year(to_date(col("Date"))))
    else:
        select_exprs.append(lit(None).cast(DateType()).alias("date_naissance"))
        age_col = lit(None).cast(IntegerType())
    
    select_exprs.append(age_col.alias("age"))
    
    # Tranche d'âge
    select_exprs.append(
        when(age_col.isNull(), "Inconnu")
        .when(age_col < 18, "0-17")
        .when(age_col <= 35, "18-35")
        .when(age_col <= 55, "36-55")
        .when(age_col <= 75, "56-75")
        .otherwise("75+").alias("tranche_age")
    )
    
    # Géographie
    if "ville" in patients_bronze.columns:
        select_exprs.append(upper(trim(col("ville"))).alias("ville"))
    else:
        select_exprs.append(lit("INCONNU").alias("ville"))
    
    if "code_postal" in patients_bronze.columns:
        select_exprs.append(
            when(length(trim(col("code_postal"))) == 5, substring(trim(col("code_postal")), 1, 2))
            .otherwise("99").alias("departement")
        )
    else:
        select_exprs.append(lit("99").alias("departement"))
    
    # Métadonnées
    select_exprs.extend([
        current_timestamp().alias("silver_created_at"),
        lit(1).alias("is_active")
    ])
    
    dim_patient = patients_bronze.select(*select_exprs).distinct()
    
    write_silver_table(dim_patient, "dim_patient")
    return dim_patient


def create_dim_etablissement(spark):
    """Crée la dimension Établissement enrichie."""
    print("\n🔵 Création dim_etablissement...")
    
    etab_bronze = read_bronze_table(spark, "etablissements")
    if etab_bronze is None:
        print("⚠️  Pas de table etablissements dans Bronze")
        return spark.createDataFrame([], schema="sk_etablissement INT, etablissement_nk STRING")
    
    select_exprs = []
    
    # Clés
    if "_sk_etablissement" in etab_bronze.columns:
        select_exprs.append(col("_sk_etablissement").alias("sk_etablissement"))
    else:
        select_exprs.append(row_number().over(Window.orderBy(lit(1))).alias("sk_etablissement"))
    
    if "identifiant_organisation" in etab_bronze.columns:
        select_exprs.append(col("identifiant_organisation").alias("etablissement_nk"))
    else:
        select_exprs.append(lit("UNKNOWN").alias("etablissement_nk"))
    
    # Nom
    if "raison_sociale_site" in etab_bronze.columns:
        nom_col = col("raison_sociale_site")
        select_exprs.append(nom_col.alias("nom_etablissement"))
        
        # Type déduit du nom
        select_exprs.append(
            when(lower(nom_col).contains("chu"), "CHU")
            .when(lower(nom_col).contains("hopital"), "Hôpital")
            .when(lower(nom_col).contains("clinique"), "Clinique")
            .when(lower(nom_col).contains("centre hospitalier"), "Centre Hospitalier")
            .otherwise("Autre").alias("type_etablissement")
        )
    else:
        select_exprs.append(lit("Établissement Inconnu").alias("nom_etablissement"))
        select_exprs.append(lit("Autre").alias("type_etablissement"))
    
    # Géographie
    if "commune" in etab_bronze.columns:
        select_exprs.append(upper(trim(col("commune"))).alias("commune"))
    else:
        select_exprs.append(lit("INCONNU").alias("commune"))
    
    if "code_postal" in etab_bronze.columns:
        cp_col = col("code_postal")
        dept_code = substring(trim(cp_col), 1, 2)
        
        select_exprs.append(
            when(length(trim(cp_col)) == 5, dept_code)
            .otherwise("99").alias("departement")
        )
        
        # Région (mapping régions françaises)
        select_exprs.append(
            when(dept_code.isin("75", "77", "78", "91", "92", "93", "94", "95"), "Île-de-France")
            .when(dept_code.isin("44", "49", "53", "72", "85"), "Pays de la Loire")
            .when(dept_code.isin("35", "56", "22", "29"), "Bretagne")
            .when(dept_code.isin("14", "27", "50", "61", "76"), "Normandie")
            .when(dept_code.isin("02", "59", "60", "62", "80"), "Hauts-de-France")
            .when(dept_code.isin("67", "68", "88"), "Grand Est")
            .when(dept_code.isin("21", "25", "39", "58", "70", "71", "89", "90"), "Bourgogne-Franche-Comté")
            .when(dept_code.isin("03", "15", "43", "63", "69", "73", "74"), "Auvergne-Rhône-Alpes")
            .when(dept_code.isin("16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"), "Nouvelle-Aquitaine")
            .when(dept_code.isin("09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"), "Occitanie")
            .when(dept_code.isin("04", "05", "06", "13", "83", "84"), "Provence-Alpes-Côte d'Azur")
            .when(dept_code.isin("20"), "Corse")
            .when(dept_code.isin("97"), "Outre-Mer")
            .otherwise("Autre").alias("region")
        )
    else:
        select_exprs.append(lit("99").alias("departement"))
        select_exprs.append(lit("Autre").alias("region"))
    
    # Métadonnées
    select_exprs.extend([
        current_timestamp().alias("silver_created_at"),
        lit(1).alias("is_active")
    ])
    
    dim_etablissement = etab_bronze.select(*select_exprs).distinct()
    
    write_silver_table(dim_etablissement, "dim_etablissement")
    return dim_etablissement


def create_dim_temps(spark):
    """Crée la dimension Temps (calendrier 2018-2025)."""
    print("\n🔵 Création dim_temps...")
    
    dates_df = spark.sql("""
        SELECT explode(sequence(to_date('2018-01-01'), to_date('2025-12-31'), interval 1 day)) as date_complete
    """)
    
    dim_temps = dates_df.select(
        row_number().over(Window.orderBy(col("date_complete"))).alias("sk_temps"),
        col("date_complete"),
        year(col("date_complete")).alias("annee"),
        month(col("date_complete")).alias("mois"),
        quarter(col("date_complete")).alias("trimestre"),
        dayofmonth(col("date_complete")).alias("jour"),
        date_format(col("date_complete"), "EEEE").alias("jour_semaine"),
        when(dayofweek(col("date_complete")).isin(1, 7), "Weekend")
          .otherwise("Semaine").alias("type_jour")
    ).distinct()
    
    write_silver_table(dim_temps, "dim_temps")
    return dim_temps


# ============================================================
# FAITS
# ============================================================
def create_fact_consultation(spark):
    """Crée la table de faits consultations."""
    print("\n🟢 Création fact_consultation...")
    
    consult_bronze = read_bronze_table(spark, "consultations")
    if consult_bronze is None:
        print("⚠️  Pas de table consultations dans Bronze")
        return None
    
    select_exprs = []
    
    # Clé de fait
    if "_sk_consultation" in consult_bronze.columns:
        select_exprs.append(col("_sk_consultation").alias("sk_consultation"))
    else:
        select_exprs.append(row_number().over(Window.orderBy(lit(1))).alias("sk_consultation"))
    
    # Clés étrangères
    if "_sk_patient" in consult_bronze.columns:
        select_exprs.append(col("_sk_patient").alias("sk_patient"))
    else:
        select_exprs.append(lit(0).alias("sk_patient"))
    
    # Date de consultation
    date_candidates = ["date_consultation", "date", "consultation_date"]
    date_found = False
    for date_col in date_candidates:
        if date_col in consult_bronze.columns:
            select_exprs.append(to_date(col(date_col)).alias("date_consultation"))
            date_found = True
            break
    
    if not date_found:
        select_exprs.append(lit(None).cast(DateType()).alias("date_consultation"))
    
    # Métriques
    if "montant" in consult_bronze.columns:
        select_exprs.append(col("montant").cast("double").alias("montant"))
    else:
        select_exprs.append(lit(0.0).alias("montant"))
    
    if "duree_minutes" in consult_bronze.columns:
        select_exprs.append(col("duree_minutes").cast("int").alias("duree_minutes"))
    else:
        select_exprs.append(lit(0).alias("duree_minutes"))
    
    # Métadonnées
    select_exprs.append(current_timestamp().alias("silver_created_at"))
    
    fact_consultation = consult_bronze.select(*select_exprs)
    
    write_silver_table(fact_consultation, "fact_consultation")
    return fact_consultation


def create_fact_hospitalisation(spark):
    """Crée la table de faits hospitalisations."""
    print("\n🟢 Création fact_hospitalisation...")
    
    hosp_bronze = read_bronze_table(spark, "hospitalisations")
    if hosp_bronze is None:
        print("⚠️  Pas de table hospitalisations dans Bronze")
        return None
    
    select_exprs = []
    
    # Clé de fait
    if "_sk_hospitalisation" in hosp_bronze.columns:
        select_exprs.append(col("_sk_hospitalisation").alias("sk_hospitalisation"))
    else:
        select_exprs.append(row_number().over(Window.orderBy(lit(1))).alias("sk_hospitalisation"))
    
    # Clés étrangères
    if "_sk_patient" in hosp_bronze.columns:
        select_exprs.append(col("_sk_patient").alias("sk_patient"))
    else:
        select_exprs.append(lit(0).alias("sk_patient"))
    
    # Dates
    if "date_entree" in hosp_bronze.columns:
        select_exprs.append(to_date(col("date_entree")).alias("date_entree"))
    else:
        select_exprs.append(lit(None).cast(DateType()).alias("date_entree"))
    
    if "date_sortie" in hosp_bronze.columns:
        select_exprs.append(to_date(col("date_sortie")).alias("date_sortie"))
    else:
        select_exprs.append(lit(None).cast(DateType()).alias("date_sortie"))
    
    # Durée séjour
    if "date_entree" in hosp_bronze.columns and "date_sortie" in hosp_bronze.columns:
        select_exprs.append(
            datediff(to_date(col("date_sortie")), to_date(col("date_entree"))).alias("duree_sejour_jours")
        )
    else:
        select_exprs.append(lit(0).alias("duree_sejour_jours"))
    
    # Métadonnées
    select_exprs.append(current_timestamp().alias("silver_created_at"))
    
    fact_hospitalisation = hosp_bronze.select(*select_exprs)
    
    write_silver_table(fact_hospitalisation, "fact_hospitalisation")
    return fact_hospitalisation


def create_fact_deces(spark):
    """Crée la table de faits décès."""
    print("\n🟢 Création fact_deces...")
    
    deces_bronze = read_bronze_table(spark, "deces_2019")
    if deces_bronze is None:
        print("⚠️  Pas de table deces_2019 dans Bronze")
        return None
    
    select_exprs = []
    
    # Clé de fait
    if "_sk_deces" in deces_bronze.columns:
        select_exprs.append(col("_sk_deces").alias("sk_deces"))
    else:
        select_exprs.append(row_number().over(Window.orderBy(lit(1))).alias("sk_deces"))
    
    # Date de décès
    date_candidates = ["date_deces", "date", "deces_date"]
    date_found = False
    for date_col in date_candidates:
        if date_col in deces_bronze.columns:
            select_exprs.append(to_date(col(date_col)).alias("date_deces"))
            date_found = True
            break
    
    if not date_found:
        select_exprs.append(lit(None).cast(DateType()).alias("date_deces"))
    
    # Géographie
    if "code_lieu_deces" in deces_bronze.columns:
        select_exprs.append(col("code_lieu_deces").alias("lieu_deces"))
    else:
        select_exprs.append(lit("INCONNU").alias("lieu_deces"))
    
    if "sexe" in deces_bronze.columns:
        select_exprs.append(col("sexe"))
    else:
        select_exprs.append(lit("I").alias("sexe"))
    
    if "age" in deces_bronze.columns:
        select_exprs.append(col("age").cast("int").alias("age"))
    else:
        select_exprs.append(lit(None).cast(IntegerType()).alias("age"))
    
    # Métadonnées
    select_exprs.append(current_timestamp().alias("silver_created_at"))
    
    fact_deces = deces_bronze.select(*select_exprs)
    
    write_silver_table(fact_deces, "fact_deces")
    return fact_deces


# ============================================================
# MÉTRIQUES
# ============================================================
def create_metrique_satisfaction(spark):
    """Crée la table de métriques satisfaction."""
    print("\n📊 Création metrique_satisfaction...")
    
    # Essayer les différentes tables de satisfaction
    satisfaction_tables = ["satisfaction_48h_2019", "satisfaction_mco_2019"]
    
    all_satisfaction = []
    
    for table_name in satisfaction_tables:
        sat_df = read_bronze_table(spark, table_name)
        if sat_df is not None:
            # Normalisation basique
            select_exprs = [
                lit(table_name).alias("source_enquete"),
                current_timestamp().alias("silver_created_at")
            ]
            
            # Ajouter toutes les colonnes existantes
            for col_name in sat_df.columns:
                if col_name not in ["_sk", "source_layer"]:
                    select_exprs.append(col(col_name))
            
            normalized = sat_df.select(*select_exprs)
            all_satisfaction.append(normalized)
    
    if all_satisfaction:
        metrique_satisfaction = all_satisfaction[0]
        for df in all_satisfaction[1:]:
            metrique_satisfaction = metrique_satisfaction.unionByName(df, allowMissingColumns=True)
        
        write_silver_table(metrique_satisfaction, "metrique_satisfaction")
        return metrique_satisfaction
    else:
        print("⚠️  Aucune table de satisfaction trouvée")
        return None


# ============================================================
# MAIN
# ============================================================
def main():
    """Pipeline complet Silver."""
    print("=" * 70)
    print("🏥 TRANSFORMATION SILVER - CHU DATA WAREHOUSE")
    print("=" * 70)
    print(f"📅 Démarrage: {datetime.now()}")
    print()
    
    spark = get_spark_session()
    
    try:
        # DIMENSIONS
        print("\n" + "=" * 70)
        print("📋 CRÉATION DES DIMENSIONS")
        print("=" * 70)
        
        dim_patient = create_dim_patient(spark)
        dim_etablissement = create_dim_etablissement(spark)
        dim_temps = create_dim_temps(spark)
        
        # FAITS
        print("\n" + "=" * 70)
        print("📊 CRÉATION DES TABLES DE FAITS")
        print("=" * 70)
        
        fact_consultation = create_fact_consultation(spark)
        fact_hospitalisation = create_fact_hospitalisation(spark)
        fact_deces = create_fact_deces(spark)
        
        # MÉTRIQUES
        print("\n" + "=" * 70)
        print("📈 CRÉATION DES MÉTRIQUES")
        print("=" * 70)
        
        metrique_satisfaction = create_metrique_satisfaction(spark)
        
        # RÉSUMÉ
        print("\n" + "=" * 70)
        print("✅ TRANSFORMATION SILVER TERMINÉE")
        print("=" * 70)
        print(f"📅 Fin: {datetime.now()}")
        print()
        print("Tables créées:")
        print("  📋 Dimensions: dim_patient, dim_etablissement, dim_temps")
        print("  📊 Faits: fact_consultation, fact_hospitalisation, fact_deces")
        print("  📈 Métriques: metrique_satisfaction")
        print()
        print("➡️  Prochaine étape: Gold Aggregation")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
