#!/usr/bin/env python3
"""
Script Gold corrigé pour répondre aux exigences métier
Auteur: Claude
Date: 2025-10-24
Description: Couche Gold corrigée répondant exactement aux 8 exigences métier spécifiées
"""

import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

def get_spark_session():
    """Session Spark optimisée pour Gold Layer"""
    try:
        spark = SparkSession.builder \
            .appName("Healthcare Gold Layer - Business Requirements") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def read_silver_table(spark, table_name):
    """Lecture des tables Silver"""
    try:
        path = f"s3a://{MINIO_CONFIG['silver_bucket']}/{table_name}"
        df = spark.read.parquet(path)
        print(f"✅ {table_name}: {df.count():,} lignes lues")
        return df
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        raise

def write_gold_table(df, table_name, partition_cols=None):
    """Écriture optimisée vers Gold"""
    try:
        path = f"s3a://{MINIO_CONFIG['gold_bucket']}/{table_name}"
        
        writer = df.coalesce(4).write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
            
        writer.option("compression", "snappy").parquet(path)
        print(f"✅ {table_name}: {df.count():,} lignes écrites")
        
    except Exception as e:
        print(f"❌ Erreur écriture {table_name}: {e}")
        raise

# ============================================================================
# EXIGENCE 1: Taux de consultation des patients dans un établissement X sur une période Y
# ============================================================================

def create_taux_consultation_etablissement(spark):
    """EXIGENCE 1: Taux de consultation par établissement et période"""
    print("\n🏥 EXIGENCE 1: Taux consultation par établissement")
    
    consultations = read_silver_table(spark, "consultations")
    
    # Ajout de colonnes temporelles pour analyse
    consultations_enriched = consultations \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation")) \
        .withColumn("mois", month("date_consultation"))
    
    # Calculs par établissement et période
    result = consultations_enriched \
        .filter(col("consultation_etablissement_finess").isNotNull()) \
        .groupBy(
            "consultation_etablissement_finess",
            "consultation_etablissement_nom",
            "region",
            "annee", "trimestre", "mois"
        ) \
        .agg(
            # Métriques de consultation
            count("*").alias("nb_consultations_total"),
            countDistinct("id_patient").alias("nb_patients_distincts"),
            countDistinct("id_prof_sante").alias("nb_professionnels_actifs"),
            
            # Taux de consultation = nb_consultations / nb_patients
            round(count("*") / countDistinct("id_patient"), 2).alias("taux_consultation_par_patient"),
            
            # Métriques complémentaires
            avg("duree_consultation_heures").alias("duree_moyenne_consultation"),
            round(avg("duree_consultation_heures"), 2).alias("duree_moyenne_consultation_h")
        ) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .withColumn("periode_complete", concat(col("annee"), lit("-"), 
                   lpad(col("mois"), 2, "0"))) \
        .orderBy("consultation_etablissement_finess", "annee", "trimestre", "mois")
    
    return result

# ============================================================================
# EXIGENCE 2: Taux de consultation par diagnostic X sur période Y
# ============================================================================

def create_taux_consultation_diagnostic(spark):
    """EXIGENCE 2: Taux de consultation par diagnostic et période"""
    print("\n🩺 EXIGENCE 2: Taux consultation par diagnostic")
    
    consultations = read_silver_table(spark, "consultations")
    diagnostics = read_silver_table(spark, "diagnostics")
    
    # Enrichissement temporel
    consultations_enriched = consultations \
        .withColumn("annee", year("date_consultation")) \
        .withColumn("trimestre", quarter("date_consultation"))
    
    # Consultations par diagnostic et période
    consult_diagnostic = consultations_enriched \
        .filter(col("code_diag").isNotNull()) \
        .groupBy("code_diag", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_consultations_diagnostic"),
            countDistinct("id_patient").alias("nb_patients_diagnostic")
        )
    
    # Total par période pour calcul des taux
    total_periode = consultations_enriched \
        .groupBy("annee", "trimestre") \
        .agg(
            count("*").alias("total_consultations_periode"),
            countDistinct("id_patient").alias("total_patients_periode")
        )
    
    # Jointure avec informations diagnostics
    result = consult_diagnostic \
        .join(total_periode, ["annee", "trimestre"]) \
        .join(diagnostics.select("code_diag", "diagnostic", "type_pathologie"), "code_diag") \
        .withColumn(
            "taux_consultation_diagnostic_pct",
            round((col("nb_consultations_diagnostic") / col("total_consultations_periode")) * 100, 2)
        ) \
        .withColumn(
            "taux_patients_diagnostic_pct", 
            round((col("nb_patients_diagnostic") / col("total_patients_periode")) * 100, 2)
        ) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "code_diag", "diagnostic", "type_pathologie",
            "annee", "trimestre", "periode",
            "nb_consultations_diagnostic", "nb_patients_diagnostic",
            "total_consultations_periode", "total_patients_periode",
            "taux_consultation_diagnostic_pct", "taux_patients_diagnostic_pct"
        ) \
        .orderBy("annee", "trimestre", desc("nb_consultations_diagnostic"))
    
    return result

# ============================================================================
# EXIGENCE 3: Taux global d'hospitalisation dans une période Y
# ============================================================================

def create_taux_hospitalisation_global(spark):
    """EXIGENCE 3: Taux global d'hospitalisation par période"""
    print("\n🏥 EXIGENCE 3: Taux global hospitalisation")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Population de référence
    population_totale = patients.count()
    
    # Enrichissement temporel
    hosp_enriched = hospitalisations \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission")) \
        .withColumn("mois", month("date_admission"))
    
    # Calculs globaux par période
    result = hosp_enriched \
        .groupBy("annee", "trimestre", "mois") \
        .agg(
            count("*").alias("nb_hospitalisations_total"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour_jours"),
            sum("jour_hospitalisation").alias("total_jours_hospitalisation"),
            countDistinct("identifiant_organisation").alias("nb_etablissements_impliques")
        ) \
        .withColumn("population_reference", lit(population_totale)) \
        .withColumn(
            "taux_hospitalisation_global_pct",
            round((col("nb_patients_hospitalises") / col("population_reference")) * 100, 3)
        ) \
        .withColumn(
            "taux_rehospitalisation_pct",
            round(((col("nb_hospitalisations_total") - col("nb_patients_hospitalises")) / 
                   col("nb_patients_hospitalises")) * 100, 2)
        ) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .withColumn("periode_mois", concat(col("annee"), lit("-"), 
                   lpad(col("mois"), 2, "0"))) \
        .orderBy("annee", "trimestre", "mois")
    
    return result

# ============================================================================
# EXIGENCE 4: Taux d'hospitalisation par diagnostic sur période
# ============================================================================

def create_taux_hospitalisation_diagnostic(spark):
    """EXIGENCE 4: Taux d'hospitalisation par diagnostic"""
    print("\n🔬 EXIGENCE 4: Taux hospitalisation par diagnostic")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    diagnostics = read_silver_table(spark, "diagnostics")
    
    # Debug: Vérifier les colonnes disponibles
    print(f"   📋 Colonnes hospitalisations: {hospitalisations.columns}")
    print(f"   📋 Colonnes diagnostics: {diagnostics.columns}")
    
    # Debug: Vérifier les valeurs nulles
    hosp_diag_non_null = hospitalisations.filter(col("diagnostic_principal").isNotNull()).count()
    print(f"   📊 Hospitalisations avec diagnostic non-null: {hosp_diag_non_null:,}")
    
    # Échantillon pour debug
    sample_diag_hosp = hospitalisations.select("diagnostic_principal").filter(col("diagnostic_principal").isNotNull()).distinct().limit(5)
    sample_diag_ref = diagnostics.select("code_diag").distinct().limit(5)
    
    print("   🔍 Échantillon diagnostics hospitalisations:")
    sample_diag_hosp.show(5, False)
    print("   🔍 Échantillon codes diagnostics référence:")
    sample_diag_ref.show(5, False)
    
    # Enrichissement temporel
    hosp_enriched = hospitalisations \
        .withColumn("annee", year("date_admission")) \
        .withColumn("trimestre", quarter("date_admission"))
    
    # Hospitalisations par diagnostic avec diagnostic simplifié
    hosp_diagnostic = hosp_enriched \
        .filter(col("diagnostic_principal").isNotNull()) \
        .groupBy("diagnostic_principal", "annee", "trimestre") \
        .agg(
            count("*").alias("nb_hospitalisations_diagnostic"),
            countDistinct("id_patient").alias("nb_patients_hospitalises_diagnostic"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour_diagnostic"),
            sum("jour_hospitalisation").alias("total_jours_diagnostic")
        )
    
    # Total par période
    total_hosp_periode = hosp_enriched \
        .groupBy("annee", "trimestre") \
        .agg(
            count("*").alias("total_hospitalisations_periode"),
            countDistinct("id_patient").alias("total_patients_hospitalises_periode")
        )
    
    # Jointure LEFT avec diagnostics pour préserver tous les diagnostics d'hospitalisation
    result = hosp_diagnostic \
        .join(total_hosp_periode, ["annee", "trimestre"]) \
        .join(diagnostics.select("code_diag", "diagnostic", "gravite_pathologie").alias("diag_ref"), 
              col("diagnostic_principal") == col("diag_ref.code_diag"), "left") \
        .withColumn(
            "diagnostic_nom",
            when(col("diag_ref.diagnostic").isNotNull(), col("diag_ref.diagnostic"))
            .otherwise(concat(lit("Diagnostic "), col("diagnostic_principal")))
        ) \
        .withColumn(
            "gravite_pathologie_clean",
            when(col("diag_ref.gravite_pathologie").isNotNull(), col("diag_ref.gravite_pathologie"))
            .otherwise("Non renseignée")
        ) \
        .withColumn(
            "taux_hospitalisation_diagnostic_pct",
            round((col("nb_hospitalisations_diagnostic") / col("total_hospitalisations_periode")) * 100, 2)
        ) \
        .withColumn(
            "taux_patients_hospitalises_diagnostic_pct",
            round((col("nb_patients_hospitalises_diagnostic") / col("total_patients_hospitalises_periode")) * 100, 2)
        ) \
        .withColumn("periode", concat(col("annee"), lit("-T"), col("trimestre"))) \
        .select(
            "diagnostic_principal", "diagnostic_nom", "gravite_pathologie_clean",
            "annee", "trimestre", "periode",
            "nb_hospitalisations_diagnostic", "nb_patients_hospitalises_diagnostic",
            "total_hospitalisations_periode", "total_patients_hospitalises_periode",
            "taux_hospitalisation_diagnostic_pct", "taux_patients_hospitalises_diagnostic_pct",
            "duree_moyenne_sejour_diagnostic", "total_jours_diagnostic"
        ) \
        .orderBy("annee", "trimestre", desc("nb_hospitalisations_diagnostic"))
    
    # Debug final: vérifier le nombre de lignes
    result_count = result.count()
    print(f"   ✅ Résultat final: {result_count:,} lignes")
    
    return result

# ============================================================================
# EXIGENCE 5: Taux d'hospitalisation par sexe et âge
# ============================================================================

def create_taux_hospitalisation_demographie(spark):
    """EXIGENCE 5: Taux d'hospitalisation par sexe et âge"""
    print("\n👥 EXIGENCE 5: Taux hospitalisation par démographie")
    
    hospitalisations = read_silver_table(spark, "hospitalisations")
    patients = read_silver_table(spark, "patients")
    
    # Hospitalisations par démographie
    hosp_demo = hospitalisations \
        .groupBy("sexe", "categorie_age") \
        .agg(
            count("*").alias("nb_hospitalisations"),
            countDistinct("id_patient").alias("nb_patients_hospitalises"),
            avg("jour_hospitalisation").alias("duree_moyenne_sejour"),
            sum("jour_hospitalisation").alias("total_jours_hospitalisation"),
            min("date_admission").alias("premiere_hospitalisation"),
            max("date_admission").alias("derniere_hospitalisation")
        )
    
    # Population par démographie
    pop_demo = patients \
        .groupBy("sexe", "categorie_age") \
        .agg(count("*").alias("population_categorie"))
    
    # Calculs des taux
    result = hosp_demo \
        .join(pop_demo, ["sexe", "categorie_age"]) \
        .withColumn(
            "taux_hospitalisation_categorie_pct",
            round((col("nb_patients_hospitalises") / col("population_categorie")) * 100, 2)
        ) \
        .withColumn(
            "taux_rehospitalisation_categorie_pct",
            round(((col("nb_hospitalisations") - col("nb_patients_hospitalises")) / 
                   col("nb_patients_hospitalises")) * 100, 2)
        ) \
        .withColumn(
            "jours_moyenne_par_patient",
            round(col("total_jours_hospitalisation") / col("nb_patients_hospitalises"), 1)
        ) \
        .orderBy("sexe", "categorie_age")
    
    return result

# ============================================================================
# EXIGENCE 6: Taux de consultation par professionnel
# ============================================================================

def create_taux_consultation_professionnel(spark):
    """EXIGENCE 6: Taux de consultation par professionnel"""
    print("\n👨‍⚕️ EXIGENCE 6: Taux consultation par professionnel")
    
    consultations = read_silver_table(spark, "consultations")
    professionnels = read_silver_table(spark, "professionnels_sante")
    
    # Consultations par professionnel
    consult_pro = consultations \
        .filter(col("id_prof_sante").isNotNull()) \
        .groupBy("id_prof_sante") \
        .agg(
            count("*").alias("nb_consultations_total"),
            countDistinct("id_patient").alias("nb_patients_uniques"),
            countDistinct("code_diag").alias("nb_diagnostics_distincts"),
            avg("duree_consultation_heures").alias("duree_moyenne_consultation"),
            countDistinct("region").alias("nb_regions_activite"),
            min("date_consultation").alias("premiere_consultation"),
            max("date_consultation").alias("derniere_consultation"),
            sum(when(col("consultation_longue"), 1).otherwise(0)).alias("nb_consultations_longues")
        )
    
    # Jointure avec données professionnels
    result = consult_pro \
        .join(professionnels.select("identifiant", "profession", "categorie_professionnelle", 
                                   "niveau_activite", "specialisation_cas"), 
              col("id_prof_sante") == col("identifiant")) \
        .withColumn(
            "taux_consultation_par_patient",
            round(col("nb_consultations_total") / col("nb_patients_uniques"), 2)
        ) \
        .withColumn(
            "taux_consultations_longues_pct",
            round((col("nb_consultations_longues") / col("nb_consultations_total")) * 100, 2)
        ) \
        .withColumn(
            "diversite_diagnostics_pct",
            round((col("nb_diagnostics_distincts") / col("nb_consultations_total")) * 100, 2)
        ) \
        .withColumn(
            "niveau_productivite",
            when(col("nb_consultations_total") >= 1000, "Très Élevée")
            .when(col("nb_consultations_total") >= 500, "Élevée")
            .when(col("nb_consultations_total") >= 100, "Moyenne")
            .when(col("nb_consultations_total") >= 10, "Faible")
            .otherwise("Très Faible")
        ) \
        .orderBy(desc("nb_consultations_total"))
    
    return result

# ============================================================================
# EXIGENCE 7: Nombre de décès par localisation (région) sur 2019
# ============================================================================

def create_deces_localisation_2019(spark):
    """EXIGENCE 7: Décès par localisation en 2019"""
    print("\n⚰️ EXIGENCE 7: Décès par localisation 2019")
    
    deces = read_silver_table(spark, "deces")
    
    # Filtrage 2019 et agrégation par localisation
    result = deces \
        .filter(col("date_deces_annee") == 2019) \
        .groupBy("region_deces", "departement_deces") \
        .agg(
            count("*").alias("nb_deces_total"),
            avg("age").alias("age_moyen_deces"),
            
            # Répartition par sexe
            sum(when(col("sexe") == "M", 1).otherwise(0)).alias("nb_deces_hommes"),
            sum(when(col("sexe") == "F", 1).otherwise(0)).alias("nb_deces_femmes"),
            
            # Répartition par tranche d'âge
            sum(when(col("tranche_age_deces") == "0-1 an", 1).otherwise(0)).alias("nb_deces_0_1"),
            sum(when(col("tranche_age_deces") == "1-17 ans", 1).otherwise(0)).alias("nb_deces_1_17"),
            sum(when(col("tranche_age_deces") == "18-29 ans", 1).otherwise(0)).alias("nb_deces_18_29"),
            sum(when(col("tranche_age_deces") == "30-44 ans", 1).otherwise(0)).alias("nb_deces_30_44"),
            sum(when(col("tranche_age_deces") == "45-59 ans", 1).otherwise(0)).alias("nb_deces_45_59"),
            sum(when(col("tranche_age_deces") == "60-74 ans", 1).otherwise(0)).alias("nb_deces_60_74"),
            sum(when(col("tranche_age_deces") == "75+ ans", 1).otherwise(0)).alias("nb_deces_75_plus"),
            
            # Répartition par saison
            sum(when(col("saison_deces") == "Hiver", 1).otherwise(0)).alias("nb_deces_hiver"),
            sum(when(col("saison_deces") == "Printemps", 1).otherwise(0)).alias("nb_deces_printemps"),
            sum(when(col("saison_deces") == "Ete", 1).otherwise(0)).alias("nb_deces_ete"),
            sum(when(col("saison_deces") == "Automne", 1).otherwise(0)).alias("nb_deces_automne"),
            
            # Espérance de vie
            sum(when(col("esperance_vie_atteinte") == "Longevite", 1).otherwise(0)).alias("nb_deces_longevite")
        ) \
        .withColumn("annee", lit(2019)) \
        .withColumn(
            "taux_deces_hommes_pct",
            round((col("nb_deces_hommes") / col("nb_deces_total")) * 100, 2)
        ) \
        .withColumn(
            "taux_deces_femmes_pct",
            round((col("nb_deces_femmes") / col("nb_deces_total")) * 100, 2)
        ) \
        .withColumn(
            "taux_longevite_pct",
            round((col("nb_deces_longevite") / col("nb_deces_total")) * 100, 2)
        ) \
        .orderBy(desc("nb_deces_total"))
    
    return result

# ============================================================================
# EXIGENCE 8: Taux global de satisfaction par région sur 2020
# ============================================================================

def create_satisfaction_region_2020(spark):
    """EXIGENCE 8: Satisfaction par région en 2020"""
    print("\n😊 EXIGENCE 8: Satisfaction par région 2020")
    
    satisfaction = read_silver_table(spark, "satisfaction")
    
    # Agrégation par région
    result = satisfaction \
        .filter(col("score_all_ajust").isNotNull()) \
        .groupBy("region") \
        .agg(
            count("*").alias("nb_etablissements_evalues"),
            
            # Scores moyens
            round(avg("score_all_ajust"), 2).alias("score_satisfaction_moyen"),
            round(avg("taux_reco_brut"), 2).alias("taux_recommandation_moyen"),
            
            # Distribution des scores
            round(min("score_all_ajust"), 2).alias("score_satisfaction_min"),
            round(max("score_all_ajust"), 2).alias("score_satisfaction_max"),
            round(stddev("score_all_ajust"), 2).alias("ecart_type_satisfaction"),
            
            # Répartition par niveau
            sum(when(col("niveau_satisfaction") == "Excellente", 1).otherwise(0)).alias("nb_etab_excellente"),
            sum(when(col("niveau_satisfaction") == "Bonne", 1).otherwise(0)).alias("nb_etab_bonne"),
            sum(when(col("niveau_satisfaction") == "Satisfaisante", 1).otherwise(0)).alias("nb_etab_satisfaisante"),
            sum(when(col("niveau_satisfaction") == "Insatisfaisante", 1).otherwise(0)).alias("nb_etab_insatisfaisante"),
            
            # Participation
            sum(when(col("taux_participation_categorise") == "Obligatoire", 1).otherwise(0)).alias("nb_participation_obligatoire"),
            sum(when(col("taux_participation_categorise") == "Volontaire", 1).otherwise(0)).alias("nb_participation_volontaire")
        ) \
        .withColumn("annee", lit(2020)) \
        .withColumn(
            "classement_satisfaction_global",
            when(col("score_satisfaction_moyen") >= 85, "Excellente")
            .when(col("score_satisfaction_moyen") >= 75, "Très Bonne")
            .when(col("score_satisfaction_moyen") >= 65, "Bonne")
            .when(col("score_satisfaction_moyen") >= 55, "Satisfaisante")
            .otherwise("Insatisfaisante")
        ) \
        .withColumn(
            "taux_etablissements_satisfaisants_pct",
            round(((col("nb_etab_excellente") + col("nb_etab_bonne")) / col("nb_etablissements_evalues")) * 100, 2)
        ) \
        .withColumn(
            "amplitude_satisfaction",
            round(col("score_satisfaction_max") - col("score_satisfaction_min"), 2)
        ) \
        .orderBy(desc("score_satisfaction_moyen"))
    
    return result

def main():
    """Fonction principale"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║              GOLD LAYER - EXIGENCES MÉTIER                      ║
║                 ENTREPÔT DE DONNÉES SANTÉ                       ║
║                                                                  ║
║  🎯 Objectif: Répondre aux 8 exigences métier spécifiées        ║
║  📊 Output: Tables Gold optimisées pour analyses métier         ║
║  🏥 Focus: Indicateurs de performance du système de santé       ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    start_time = time.time()
    
    try:
        # Initialisation
        spark = get_spark_session()
        print("✅ Session Spark initialisée")
        
        # Création des tables Gold selon les exigences
        print("\n🚀 Création des tables Gold selon exigences métier...")
        
        # EXIGENCE 1: Consultations par établissement
        gold1 = create_taux_consultation_etablissement(spark)
        write_gold_table(gold1, "exigence_1_consultation_etablissement", ["annee", "region"])
        
        # EXIGENCE 2: Consultations par diagnostic  
        gold2 = create_taux_consultation_diagnostic(spark)
        write_gold_table(gold2, "exigence_2_consultation_diagnostic", ["annee"])
        
        # EXIGENCE 3: Hospitalisation globale
        gold3 = create_taux_hospitalisation_global(spark)
        write_gold_table(gold3, "exigence_3_hospitalisation_global", ["annee"])
        
        # EXIGENCE 4: Hospitalisation par diagnostic
        gold4 = create_taux_hospitalisation_diagnostic(spark)
        write_gold_table(gold4, "exigence_4_hospitalisation_diagnostic", ["annee"])
        
        # EXIGENCE 5: Hospitalisation par démographie
        gold5 = create_taux_hospitalisation_demographie(spark)
        write_gold_table(gold5, "exigence_5_hospitalisation_demographie")
        
        # EXIGENCE 6: Consultation par professionnel
        gold6 = create_taux_consultation_professionnel(spark)
        write_gold_table(gold6, "exigence_6_consultation_professionnel")
        
        # EXIGENCE 7: Décès par localisation 2019
        gold7 = create_deces_localisation_2019(spark)
        write_gold_table(gold7, "exigence_7_deces_localisation_2019")
        
        # EXIGENCE 8: Satisfaction par région 2020
        gold8 = create_satisfaction_region_2020(spark)
        write_gold_table(gold8, "exigence_8_satisfaction_region_2020")
        
        # Rapport final
        duration = time.time() - start_time
        
        print(f"""
    
🎉 TOUTES LES EXIGENCES MÉTIER SATISFAITES!

📊 TABLES GOLD CRÉÉES:
   ✅ exigence_1_consultation_etablissement: Taux consultations par établissement X période Y
   ✅ exigence_2_consultation_diagnostic: Taux consultations par diagnostic X période Y  
   ✅ exigence_3_hospitalisation_global: Taux global hospitalisation période Y
   ✅ exigence_4_hospitalisation_diagnostic: Taux hospitalisation par diagnostic période Y
   ✅ exigence_5_hospitalisation_demographie: Taux hospitalisation par sexe/âge
   ✅ exigence_6_consultation_professionnel: Taux consultation par professionnel
   ✅ exigence_7_deces_localisation_2019: Décès par localisation région 2019
   ✅ exigence_8_satisfaction_region_2020: Satisfaction globale par région 2020

🎯 COUVERTURE MÉTIER COMPLÈTE:
   📈 Tous les indicateurs demandés sont calculés
   📊 Tous les taux et métriques sont disponibles
   🏥 Analyses par établissement, diagnostic, démographie
   📅 Analyses temporelles sur périodes spécifiées
   🌍 Analyses géographiques par région
   
⏱️  Traitement terminé en {duration:.2f} secondes
📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"💥 ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)