#!/usr/bin/env python3
"""
Script Gold - Modèle Dimensionnel en Étoile (Star Schema)
==========================================================
Architecture: Dimensions + Facts + Data Marts

Ce script implémente un modèle dimensionnel en étoile pour répondre
aux 8 exigences métier à travers des tables de dimension, de faits et
des data marts analytiques.

Date: 2025-10-24
"""

import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

# ============================================================================
# UTILITAIRES
# ============================================================================

def get_spark_session():
    """Session Spark optimisée pour Gold Layer"""
    try:
        # Charger les JARs nécessaires pour S3
        jars_dir = "/home/jovyan/jars"
        jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
        jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])

        spark = SparkSession.builder \
            .appName("Gold Layer - Star Schema") \
            .config("spark.jars", jars_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark Gold initialisé")
        return spark
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def read_silver_table(spark, table_name):
    """Lecture des tables Silver"""
    try:
        path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df = spark.read.parquet(path)
        count = df.count()
        print(f"  ✅ {table_name}: {count:,} lignes")
        return df
    except Exception as e:
        print(f"  ❌ Erreur lecture {table_name}: {e}")
        raise

def write_gold_table(df, table_name, partition_cols=None):
    """Écriture optimisée vers Gold"""
    try:
        path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"

        writer = df.coalesce(4).write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        writer.option("compression", "snappy").parquet(path)
        count = df.count()
        print(f"  ✅ {table_name}: {count:,} lignes écrites")
        return count
    except Exception as e:
        print(f"  ❌ Erreur écriture {table_name}: {e}")
        raise

# ============================================================================
# TABLES DE DIMENSION
# ============================================================================

def create_dim_temps(spark):
    """
    DIM_TEMPS: Dimension temporelle complète
    Couvre 2000-2030 pour supporter historique et prévisions
    """
    print("\n📅 Création DIM_TEMPS...")

    # Génération de dates (2000-2030)
    date_range = spark.range(0, 11323).select(  # 31 ans * 365.25 jours
        date_add(lit("2000-01-01"), col("id").cast("integer")).alias("date_complete")
    )

    dim_temps = date_range \
        .withColumn("date_id", date_format("date_complete", "yyyyMMdd").cast("integer")) \
        .withColumn("annee", year("date_complete")) \
        .withColumn("trimestre", quarter("date_complete")) \
        .withColumn("mois", month("date_complete")) \
        .withColumn("jour", dayofmonth("date_complete")) \
        .withColumn("semaine", weekofyear("date_complete")) \
        .withColumn("jour_semaine", dayofweek("date_complete")) \
        .withColumn("jour_annee", dayofyear("date_complete")) \
        .withColumn("nom_mois",
            when(col("mois") == 1, "Janvier")
            .when(col("mois") == 2, "Février")
            .when(col("mois") == 3, "Mars")
            .when(col("mois") == 4, "Avril")
            .when(col("mois") == 5, "Mai")
            .when(col("mois") == 6, "Juin")
            .when(col("mois") == 7, "Juillet")
            .when(col("mois") == 8, "Août")
            .when(col("mois") == 9, "Septembre")
            .when(col("mois") == 10, "Octobre")
            .when(col("mois") == 11, "Novembre")
            .when(col("mois") == 12, "Décembre")
        ) \
        .withColumn("nom_jour_semaine",
            when(col("jour_semaine") == 1, "Dimanche")
            .when(col("jour_semaine") == 2, "Lundi")
            .when(col("jour_semaine") == 3, "Mardi")
            .when(col("jour_semaine") == 4, "Mercredi")
            .when(col("jour_semaine") == 5, "Jeudi")
            .when(col("jour_semaine") == 6, "Vendredi")
            .when(col("jour_semaine") == 7, "Samedi")
        ) \
        .withColumn("est_weekend",
            when(col("jour_semaine").isin([1, 7]), True).otherwise(False)
        ) \
        .withColumn("saison",
            when(col("mois").isin([12, 1, 2]), "Hiver")
            .when(col("mois").isin([3, 4, 5]), "Printemps")
            .when(col("mois").isin([6, 7, 8]), "Été")
            .otherwise("Automne")
        ) \
        .withColumn("periode_annee",
            concat(col("annee"), lit("-T"), col("trimestre"))
        ) \
        .withColumn("periode_mois",
            date_format("date_complete", "yyyy-MM")
        )

    return dim_temps

def create_dim_patient(silver_patients):
    """
    DIM_PATIENT: Dimension démographique des patients
    Attributs: sexe, âge, catégorie_age, segment
    """
    print("\n👥 Création DIM_PATIENT...")

    dim_patient = silver_patients.select(
        col("_sk_patient").alias("patient_sk"),
        col("id_patient").alias("patient_id"),
        "sexe",
        "age",
        "categorie_age",
        "code_postal",
        "segment_patient",
        "statut_activite",
        "patient_hospitalise"
    ).distinct()

    return dim_patient

def create_dim_diagnostic(silver_diagnostics):
    """
    DIM_DIAGNOSTIC: Dimension des diagnostics médicaux
    """
    print("\n🔬 Création DIM_DIAGNOSTIC...")

    dim_diagnostic = silver_diagnostics.select(
        col("_sk").alias("diagnostic_sk"),  # CORRECTION: _sk au lieu de _sk_diagnostic
        "code_diag",
        "diagnostic",
        "type_pathologie",
        "gravite_pathologie",
        "prevalence_globale"
    ).distinct()

    return dim_diagnostic

def create_dim_etablissement(silver_etablissements):
    """
    DIM_ETABLISSEMENT: Dimension des établissements de santé
    """
    print("\n🏥 Création DIM_ETABLISSEMENT...")

    dim_etablissement = silver_etablissements.select(
        col("_sk").alias("etablissement_sk"),  # CORRECTION: _sk au lieu de _sk_etablissement
        "finess_site",
        "raison_sociale_site",
        "region",
        # CORRECTION: departement n'existe pas, on le crée depuis code_postal
        substring(col("code_postal"), 1, 2).alias("departement"),
        "code_postal",
        # CORRECTION: ville n'existe pas, on utilise commune
        col("commune").alias("ville"),
        "niveau_activite",
        "specialisation_etablissement",
        "performance_globale"
    ).distinct()

    return dim_etablissement

def create_dim_professionnel(silver_professionnels):
    """
    DIM_PROFESSIONNEL: Dimension des professionnels de santé
    """
    print("\n👨‍⚕️ Création DIM_PROFESSIONNEL...")

    dim_professionnel = silver_professionnels.select(
        col("_sk_professionnel").alias("professionnel_sk"),  # Cette colonne existe bien dans professionnels
        col("identifiant").alias("professionnel_id"),
        "profession",
        "categorie_professionnelle",
        "code_specialite",
        "niveau_activite",
        "specialisation_cas"
    ).distinct()

    return dim_professionnel

def create_dim_localisation(silver_deces, silver_etablissements):
    """
    DIM_LOCALISATION: Dimension géographique
    """
    print("\n🌍 Création DIM_LOCALISATION...")

    # Régions depuis décès
    regions_deces = silver_deces.select(
        col("region_deces").alias("region"),
        col("departement_deces").alias("departement")
    ).distinct()

    # Régions depuis établissements - CORRECTION: departement n'existe pas, on le crée
    regions_etab = silver_etablissements.select(
        col("region"),
        substring(col("code_postal"), 1, 2).alias("departement")
    ).distinct()

    # Union et déduplication
    dim_localisation = regions_deces.unionByName(regions_etab, allowMissingColumns=True) \
        .filter(col("region").isNotNull()) \
        .distinct() \
        .withColumn("localisation_sk", sha2(concat_ws("_", col("region"), coalesce(col("departement"), lit(""))), 256)) \
        .withColumn("zone_geographique",
            when(col("region") == "Ile-de-France", "Nord")
            .when(col("region").isin(["Hauts-de-France", "Normandie", "Bretagne"]), "Nord")
            .when(col("region").isin(["Grand Est", "Bourgogne-Franche-Comté"]), "Est")
            .when(col("region").isin(["Pays de la Loire", "Centre-Val de Loire"]), "Ouest")
            .when(col("region").isin(["Nouvelle-Aquitaine", "Occitanie"]), "Sud-Ouest")
            .when(col("region").isin(["Auvergne-Rhône-Alpes", "Provence-Alpes-Côte d'Azur"]), "Sud-Est")
            .otherwise("Autre")
        )

    return dim_localisation

# ============================================================================
# TABLES DE FAITS
# ============================================================================

def create_fact_consultation(silver_consultations, dim_temps):
    """
    FACT_CONSULTATION: Faits de consultation
    Mesures: durée, coût estimé, nombre de consultations
    """
    print("\n📊 Création FACT_CONSULTATION...")

    # Enrichissement avec date_id et colonnes de partitionnement
    fact_consultation = silver_consultations \
        .join(dim_temps.select("date_id", "date_complete", "annee", "mois"),
              col("date_consultation") == col("date_complete"), "left") \
        .select(
            # Clés étrangères (FK)
            col("date_id").alias("date_consultation_fk"),
            col("_sk_patient").alias("patient_fk"),
            col("_sk_diagnostic").alias("diagnostic_fk"),
            col("id_prof_sante").alias("professionnel_fk"),
            col("consultation_etablissement_finess").alias("etablissement_fk"),

            # Attributs descriptifs
            "date_consultation",
            "region",
            "sexe",
            "categorie_age",
            "saison_consultation",
            "periode_journee",

            # Colonnes de partitionnement (depuis dim_temps)
            col("annee").alias("consultation_annee"),
            col("mois").alias("consultation_mois"),

            # Mesures
            col("duree_consultation_heures").alias("duree_heures"),
            when(col("consultation_longue"), 1).otherwise(0).alias("est_consultation_longue"),
            lit(1).alias("nb_consultations")
        )

    return fact_consultation

def create_fact_hospitalisation(silver_hospitalisations, dim_temps):
    """
    FACT_HOSPITALISATION: Faits d'hospitalisation
    Mesures: durée séjour, coût estimé, gravité
    """
    print("\n📊 Création FACT_HOSPITALISATION...")

    # Calculer date_sortie (date_admission + durée)
    # Note: date_sortie n'existe pas dans silver_hospitalisations
    hosp_with_sortie = silver_hospitalisations \
        .withColumn("date_sortie_calc",
            expr("date_add(date_admission, jour_hospitalisation)")
        )

    fact_hospitalisation = hosp_with_sortie \
        .join(dim_temps.select("date_id", "date_complete", "annee", "mois"),
              col("date_admission") == col("date_complete"), "left") \
        .select(
            # Clés étrangères (FK)
            col("date_id").alias("date_admission_fk"),
            col("_sk_patient").alias("patient_fk"),
            col("_sk_diagnostic").alias("diagnostic_fk"),
            col("_sk_etablissement").alias("etablissement_fk"),

            # Attributs descriptifs
            "date_admission",
            col("date_sortie_calc").alias("date_sortie"),
            "region",
            "sexe",
            "categorie_age",
            "type_hospitalisation",
            "gravite_sejour",
            "saison_hospitalisation",

            # Colonnes de partitionnement (depuis dim_temps)
            col("annee").alias("admission_annee"),
            col("mois").alias("admission_mois"),

            # Mesures
            col("jour_hospitalisation").alias("duree_sejour_jours"),
            when(col("gravite_sejour") == "Long", 3)
                .when(col("gravite_sejour") == "Moyen", 2)
                .otherwise(1).alias("score_gravite"),
            lit(1).alias("nb_hospitalisations")
        )

    return fact_hospitalisation

def create_fact_deces(silver_deces, dim_temps):
    """
    FACT_DECES: Faits de décès
    Mesures: âge, localisation
    """
    print("\n📊 Création FACT_DECES...")

    fact_deces = silver_deces \
        .filter(col("date_deces_annee") == 2019) \
        .join(dim_temps.select("date_id", "date_complete"),
              col("date_deces") == col("date_complete"), "left") \
        .select(
            # Clés étrangères (FK)
            col("date_id").alias("date_deces_fk"),

            # Attributs descriptifs
            "date_deces",
            col("region_deces").alias("region"),
            col("departement_deces").alias("departement"),
            "sexe",
            "tranche_age_deces",
            "saison_deces",
            "esperance_vie_atteinte",

            # Mesures
            "age",
            lit(1).alias("nb_deces")
        )

    return fact_deces

# ============================================================================
# DATA MARTS ANALYTIQUES
# ============================================================================

def create_mart_performance_etablissement(fact_consultation, fact_hospitalisation, dim_etablissement, dim_temps):
    """
    MART 1: Performance des établissements
    Répond aux exigences 1, 3
    """
    print("\n📈 Création MART_PERFORMANCE_ETABLISSEMENT...")

    # Consultations par établissement avec jointure temps
    consult_stats = fact_consultation \
        .filter(col("etablissement_fk").isNotNull()) \
        .join(dim_temps.select("date_id", "annee", "trimestre", "mois", "periode_annee"),
              fact_consultation.date_consultation_fk == dim_temps.date_id, "left") \
        .groupBy("etablissement_fk", "annee", "trimestre", "mois", "periode_annee") \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("patient_fk").alias("nb_patients_consultations"),
            avg("duree_heures").alias("duree_moyenne_consultation")
        )

    # Hospitalisations par établissement avec jointure temps
    hosp_stats = fact_hospitalisation \
        .join(dim_temps.select("date_id", "annee", "trimestre", "mois", "periode_annee"),
              fact_hospitalisation.date_admission_fk == dim_temps.date_id, "left") \
        .groupBy("etablissement_fk", "annee", "trimestre", "mois", "periode_annee") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("patient_fk").alias("nb_patients_hospitalises"),
            avg("duree_sejour_jours").alias("duree_moyenne_sejour")
        )

    # Jointure outer sur etablissement + période temporelle
    mart = consult_stats \
        .join(hosp_stats, ["etablissement_fk", "annee", "trimestre", "mois", "periode_annee"], "outer") \
        .join(dim_etablissement, col("etablissement_fk") == col("etablissement_sk"), "left") \
        .select(
            "finess_site",
            "raison_sociale_site",
            "region",
            "annee",
            "trimestre",
            "mois",
            "periode_annee",
            coalesce(col("nb_consultations"), lit(0)).alias("nb_consultations"),
            coalesce(col("nb_patients_consultations"), lit(0)).alias("nb_patients_consultations"),
            coalesce(col("nb_hospitalisations"), lit(0)).alias("nb_hospitalisations"),
            coalesce(col("nb_patients_hospitalises"), lit(0)).alias("nb_patients_hospitalises"),
            "duree_moyenne_consultation",
            "duree_moyenne_sejour"
        ) \
        .withColumn("taux_consultation_par_patient",
            when(col("nb_patients_consultations") > 0,
                 round(col("nb_consultations") / col("nb_patients_consultations"), 2))
            .otherwise(0)
        ) \
        .withColumn("taux_hospitalisation_pct",
            when(col("nb_patients_consultations") > 0,
                 round((col("nb_patients_hospitalises") / col("nb_patients_consultations")) * 100, 2))
            .otherwise(0)
        )

    return mart

def create_mart_diagnostic_epidemio(fact_consultation, fact_hospitalisation, dim_diagnostic, dim_temps):
    """
    MART 2: Épidémiologie et diagnostics
    Répond aux exigences 2, 4
    """
    print("\n📈 Création MART_DIAGNOSTIC_EPIDEMIO...")

    # Approche simplifiée : agrégations directes puis union

    # Consultations par diagnostic et temps
    consult_stats = fact_consultation \
        .join(dim_temps.select(col("date_id"), col("annee"), col("trimestre"), col("periode_annee")),
              fact_consultation.date_consultation_fk == dim_temps.date_id, "left") \
        .join(dim_diagnostic, fact_consultation.diagnostic_fk == dim_diagnostic.diagnostic_sk, "left") \
        .groupBy("code_diag", "diagnostic", "type_pathologie", "gravite_pathologie", "annee", "trimestre", "periode_annee") \
        .agg(
            count("*").alias("nb_consultations"),
            countDistinct("patient_fk").alias("nb_patients_consultes")
        )

    # Hospitalisations par diagnostic et temps
    hosp_stats = fact_hospitalisation \
        .join(dim_temps.select(col("date_id"), col("annee"), col("trimestre"), col("periode_annee")),
              fact_hospitalisation.date_admission_fk == dim_temps.date_id, "left") \
        .join(dim_diagnostic, fact_hospitalisation.diagnostic_fk == dim_diagnostic.diagnostic_sk, "left") \
        .groupBy("code_diag", "diagnostic", "type_pathologie", "gravite_pathologie", "annee", "trimestre", "periode_annee") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("patient_fk").alias("nb_patients_hospitalises"),
            avg("duree_sejour_jours").alias("duree_moyenne_sejour_diag")
        )

    # Totaux par période
    total_consult_per_period = fact_consultation \
        .join(dim_temps.select("date_id", "annee", "trimestre"),
              fact_consultation.date_consultation_fk == dim_temps.date_id, "left") \
        .groupBy("annee", "trimestre") \
        .agg(count("*").alias("total_consultations_periode"))

    total_hosp_per_period = fact_hospitalisation \
        .join(dim_temps.select("date_id", "annee", "trimestre"),
              fact_hospitalisation.date_admission_fk == dim_temps.date_id, "left") \
        .groupBy("annee", "trimestre") \
        .agg(count("*").alias("total_hospitalisations_periode"))

    # Jointure finale
    mart = consult_stats \
        .join(hosp_stats, ["code_diag", "diagnostic", "type_pathologie", "gravite_pathologie", "annee", "trimestre", "periode_annee"], "outer") \
        .join(total_consult_per_period, ["annee", "trimestre"], "left") \
        .join(total_hosp_per_period, ["annee", "trimestre"], "left") \
        .fillna(0, ["nb_consultations", "nb_patients_consultes", "nb_hospitalisations", "nb_patients_hospitalises"]) \
        .withColumn("taux_consultation_diagnostic_pct",
            when(col("total_consultations_periode") > 0,
                 round((col("nb_consultations") / col("total_consultations_periode")) * 100, 2))
            .otherwise(0)
        ) \
        .withColumn("taux_hospitalisation_diagnostic_pct",
            when(col("total_hospitalisations_periode") > 0,
                 round((col("nb_hospitalisations") / col("total_hospitalisations_periode")) * 100, 2))
            .otherwise(0)
        )

    return mart

def create_mart_demographie(fact_hospitalisation, dim_patient, dim_temps):
    """
    MART 3: Analyses démographiques
    Répond à l'exigence 5
    """
    print("\n📈 Création MART_DEMOGRAPHIE...")

    # Utiliser les colonnes de fact_hospitalisation qui sont déjà présentes
    # (sexe et categorie_age sont dans fact_hospitalisation)
    mart = fact_hospitalisation \
        .join(dim_temps.select("date_id", "annee", "trimestre"),
              col("date_admission_fk") == col("date_id"), "left") \
        .groupBy(
            fact_hospitalisation.sexe,
            fact_hospitalisation.categorie_age,
            "annee",
            "trimestre"
        ) \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("patient_fk").alias("nb_patients_hospitalises"),
            avg("duree_sejour_jours").alias("duree_moyenne_sejour"),
            sum("duree_sejour_jours").alias("total_jours_hospitalisation")
        ) \
        .withColumn("taux_rehospitalisation_pct",
            when(col("nb_patients_hospitalises") > 0,
                 round(((col("nb_hospitalisations") - col("nb_patients_hospitalises")) /
                        col("nb_patients_hospitalises")) * 100, 2))
            .otherwise(0)
        )

    return mart

def create_mart_professionnel(fact_consultation, dim_professionnel):
    """
    MART 4: Performance des professionnels
    Répond à l'exigence 6
    """
    print("\n📈 Création MART_PROFESSIONNEL...")

    mart = fact_consultation \
        .filter(col("professionnel_fk").isNotNull()) \
        .groupBy("professionnel_fk") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("patient_fk").alias("nb_patients_uniques"),
            countDistinct("diagnostic_fk").alias("nb_diagnostics_distincts"),
            avg("duree_heures").alias("duree_moyenne_consultation"),
            sum("est_consultation_longue").alias("nb_consultations_longues"),
            countDistinct("region").alias("nb_regions_activite")
        ) \
        .join(dim_professionnel, col("professionnel_fk") == col("professionnel_sk"), "left") \
        .select(
            "professionnel_id",
            "profession",
            "categorie_professionnelle",
            "code_specialite",
            "nb_consultations_total",
            "nb_patients_uniques",
            "nb_diagnostics_distincts",
            "duree_moyenne_consultation",
            "nb_consultations_longues",
            "nb_regions_activite"
        ) \
        .withColumn("taux_consultation_par_patient",
            when(col("nb_patients_uniques") > 0,
                 round(col("nb_consultations_total") / col("nb_patients_uniques"), 2))
            .otherwise(0)
        ) \
        .withColumn("taux_consultations_longues_pct",
            when(col("nb_consultations_total") > 0,
                 round((col("nb_consultations_longues") / col("nb_consultations_total")) * 100, 2))
            .otherwise(0)
        )

    return mart

def create_mart_deces_localisation(fact_deces, dim_localisation):
    """
    MART 5: Décès par localisation (2019)
    Répond à l'exigence 7
    """
    print("\n📈 Création MART_DECES_LOCALISATION...")

    mart = fact_deces \
        .groupBy("region", "departement") \
        .agg(
            count("*").alias("nb_deces_total"),
            avg("age").alias("age_moyen_deces"),
            sum(when(col("sexe") == "M", 1).otherwise(0)).alias("nb_deces_hommes"),
            sum(when(col("sexe") == "F", 1).otherwise(0)).alias("nb_deces_femmes"),
            sum(when(col("tranche_age_deces") == "75+ ans", 1).otherwise(0)).alias("nb_deces_75_plus"),
            sum(when(col("esperance_vie_atteinte") == "Longevite", 1).otherwise(0)).alias("nb_deces_longevite")
        ) \
        .join(dim_localisation, ["region", "departement"], "left") \
        .withColumn("annee", lit(2019)) \
        .withColumn("taux_deces_hommes_pct",
            when(col("nb_deces_total") > 0,
                 round((col("nb_deces_hommes") / col("nb_deces_total")) * 100, 2))
            .otherwise(0)
        ) \
        .withColumn("taux_deces_femmes_pct",
            when(col("nb_deces_total") > 0,
                 round((col("nb_deces_femmes") / col("nb_deces_total")) * 100, 2))
            .otherwise(0)
        )

    return mart

def create_mart_satisfaction_region(silver_satisfaction):
    """
    MART 6: Satisfaction par région (2020)
    Répond à l'exigence 8
    """
    print("\n📈 Création MART_SATISFACTION_REGION...")

    mart = silver_satisfaction \
        .filter(col("score_all_ajust").isNotNull()) \
        .groupBy("region") \
        .agg(
            count("*").alias("nb_etablissements_evalues"),
            round(avg("score_all_ajust"), 2).alias("score_satisfaction_moyen"),
            round(avg("taux_reco_brut"), 2).alias("taux_recommandation_moyen"),
            round(min("score_all_ajust"), 2).alias("score_min"),
            round(max("score_all_ajust"), 2).alias("score_max"),
            round(stddev("score_all_ajust"), 2).alias("ecart_type"),
            sum(when(col("niveau_satisfaction") == "Excellente", 1).otherwise(0)).alias("nb_etab_excellente"),
            sum(when(col("niveau_satisfaction") == "Bonne", 1).otherwise(0)).alias("nb_etab_bonne")
        ) \
        .withColumn("annee", lit(2020)) \
        .withColumn("classement_global",
            when(col("score_satisfaction_moyen") >= 85, "Excellente")
            .when(col("score_satisfaction_moyen") >= 75, "Très Bonne")
            .when(col("score_satisfaction_moyen") >= 65, "Bonne")
            .otherwise("Satisfaisante")
        ) \
        .withColumn("taux_etablissements_satisfaisants_pct",
            when(col("nb_etablissements_evalues") > 0,
                 round(((col("nb_etab_excellente") + col("nb_etab_bonne")) / col("nb_etablissements_evalues")) * 100, 2))
            .otherwise(0)
        )

    return mart

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Pipeline principal Gold - Star Schema"""
    print("""
╔════════════════════════════════════════════════════════════════════════╗
║                    GOLD LAYER - STAR SCHEMA                           ║
║                   Modèle Dimensionnel en Étoile                       ║
╠════════════════════════════════════════════════════════════════════════╣
║  📐 Architecture:                                                      ║
║     • 6 Tables de Dimension (DIM)                                     ║
║     • 3 Tables de Faits (FACT)                                        ║
║     • 6 Data Marts Analytiques (MART)                                 ║
║                                                                        ║
║  🎯 Objectif:                                                          ║
║     Répondre aux 8 exigences métier via un modèle dimensionnel        ║
║     optimisé pour les analyses OLAP                                   ║
╚════════════════════════════════════════════════════════════════════════╝
    """)

    start_time = time.time()

    try:
        # Initialisation Spark
        spark = get_spark_session()

        # ====================================================================
        # ÉTAPE 1: LECTURE DES DONNÉES SILVER
        # ====================================================================
        print("\n" + "="*70)
        print("📥 ÉTAPE 1: LECTURE DES DONNÉES SILVER")
        print("="*70)

        silver_patients = read_silver_table(spark, "patients")
        silver_consultations = read_silver_table(spark, "consultations")
        silver_hospitalisations = read_silver_table(spark, "hospitalisations")
        silver_deces = read_silver_table(spark, "deces")
        silver_etablissements = read_silver_table(spark, "etablissements")
        silver_professionnels = read_silver_table(spark, "professionnels_sante")
        silver_diagnostics = read_silver_table(spark, "diagnostics")
        silver_satisfaction = read_silver_table(spark, "satisfaction")

        # ====================================================================
        # ÉTAPE 2: CRÉATION DES TABLES DE DIMENSION
        # ====================================================================
        print("\n" + "="*70)
        print("📐 ÉTAPE 2: CRÉATION DES TABLES DE DIMENSION")
        print("="*70)

        dim_temps = create_dim_temps(spark)
        write_gold_table(dim_temps, "dim_temps")

        dim_patient = create_dim_patient(silver_patients)
        write_gold_table(dim_patient, "dim_patient")

        dim_diagnostic = create_dim_diagnostic(silver_diagnostics)
        write_gold_table(dim_diagnostic, "dim_diagnostic")

        dim_etablissement = create_dim_etablissement(silver_etablissements)
        write_gold_table(dim_etablissement, "dim_etablissement")

        dim_professionnel = create_dim_professionnel(silver_professionnels)
        write_gold_table(dim_professionnel, "dim_professionnel")

        dim_localisation = create_dim_localisation(silver_deces, silver_etablissements)
        write_gold_table(dim_localisation, "dim_localisation")

        # ====================================================================
        # ÉTAPE 3: CRÉATION DES TABLES DE FAITS
        # ====================================================================
        print("\n" + "="*70)
        print("📊 ÉTAPE 3: CRÉATION DES TABLES DE FAITS")
        print("="*70)

        fact_consultation = create_fact_consultation(silver_consultations, dim_temps)
        write_gold_table(fact_consultation, "fact_consultation", ["consultation_annee", "consultation_mois"])

        fact_hospitalisation = create_fact_hospitalisation(silver_hospitalisations, dim_temps)
        write_gold_table(fact_hospitalisation, "fact_hospitalisation", ["admission_annee", "admission_mois"])

        fact_deces = create_fact_deces(silver_deces, dim_temps)
        write_gold_table(fact_deces, "fact_deces")

        # ====================================================================
        # ÉTAPE 4: CRÉATION DES DATA MARTS ANALYTIQUES
        # ====================================================================
        print("\n" + "="*70)
        print("📈 ÉTAPE 4: CRÉATION DES DATA MARTS ANALYTIQUES")
        print("="*70)

        mart_performance_etablissement = create_mart_performance_etablissement(
            fact_consultation, fact_hospitalisation, dim_etablissement, dim_temps)
        write_gold_table(mart_performance_etablissement, "mart_performance_etablissement", ["annee", "region"])

        mart_diagnostic_epidemio = create_mart_diagnostic_epidemio(
            fact_consultation, fact_hospitalisation, dim_diagnostic, dim_temps)
        write_gold_table(mart_diagnostic_epidemio, "mart_diagnostic_epidemio", ["annee"])

        mart_demographie = create_mart_demographie(
            fact_hospitalisation, dim_patient, dim_temps)
        write_gold_table(mart_demographie, "mart_demographie")

        mart_professionnel = create_mart_professionnel(
            fact_consultation, dim_professionnel)
        write_gold_table(mart_professionnel, "mart_professionnel")

        mart_deces_localisation = create_mart_deces_localisation(
            fact_deces, dim_localisation)
        write_gold_table(mart_deces_localisation, "mart_deces_localisation_2019")

        mart_satisfaction_region = create_mart_satisfaction_region(silver_satisfaction)
        write_gold_table(mart_satisfaction_region, "mart_satisfaction_region_2020")

        # ====================================================================
        # RAPPORT FINAL
        # ====================================================================
        duration = time.time() - start_time

        print(f"""

╔════════════════════════════════════════════════════════════════════════╗
║                      🎉 GOLD LAYER TERMINÉ !                          ║
╚════════════════════════════════════════════════════════════════════════╝

📐 TABLES DE DIMENSION (6):
   ✅ dim_temps              : Calendrier complet 2000-2030
   ✅ dim_patient            : Démographie des patients
   ✅ dim_diagnostic         : Catalogue des diagnostics
   ✅ dim_etablissement      : Établissements de santé
   ✅ dim_professionnel      : Professionnels de santé
   ✅ dim_localisation       : Géographie (régions, départements)

📊 TABLES DE FAITS (3):
   ✅ fact_consultation      : Événements de consultation
   ✅ fact_hospitalisation   : Événements d'hospitalisation
   ✅ fact_deces             : Événements de décès (2019)

📈 DATA MARTS ANALYTIQUES (6):
   ✅ mart_performance_etablissement  → Exigences 1, 3
   ✅ mart_diagnostic_epidemio        → Exigences 2, 4
   ✅ mart_demographie                → Exigence 5
   ✅ mart_professionnel              → Exigence 6
   ✅ mart_deces_localisation_2019    → Exigence 7
   ✅ mart_satisfaction_region_2020   → Exigence 8

🎯 COUVERTURE MÉTIER:
   ✓ Taux de consultation par établissement X période Y
   ✓ Taux de consultation par diagnostic X période Y
   ✓ Taux global d'hospitalisation période Y
   ✓ Taux d'hospitalisation par diagnostic période Y
   ✓ Taux d'hospitalisation par sexe et âge
   ✓ Taux de consultation par professionnel
   ✓ Nombre de décès par localisation (2019)
   ✓ Taux global de satisfaction par région (2020)

⏱️  Durée totale: {duration:.2f} secondes
📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

╔════════════════════════════════════════════════════════════════════════╗
║  Le modèle en étoile est optimisé pour:                              ║
║    • Requêtes analytiques rapides (OLAP)                             ║
║    • Agrégations et drill-down faciles                               ║
║    • Intégration BI/Superset                                         ║
║    • Maintenance et évolution simples                                ║
╚════════════════════════════════════════════════════════════════════════╝
        """)

        spark.stop()
        return True

    except Exception as e:
        print(f"\n💥 ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
