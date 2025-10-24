#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BENCHMARK PERFORMANCE - GOLD LAYER
===================================

Évalue les performances d'accès à la couche Gold en mesurant les temps de réponse
des requêtes SQL basées sur les 8 exigences métier.

Génère des rapports détaillés avec :
- Performances des requêtes
- Analyse des Data Marts Gold
- Statistiques complètes
- Graphiques de performance

Auteur: Claude Code
Date: 2025-10-25
"""

import sys
import time
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, countDistinct, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Configuration Matplotlib pour affichage sans GUI
import matplotlib
matplotlib.use('Agg')

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

GOLD_BUCKET = "s3a://gold"

# Nombre d'exécutions pour chaque requête (pour moyenne)
NB_ITERATIONS = 3

# Répertoire de sortie avec fallback
OUTPUT_DIRS = [
    "/home/jovyan/benchmark_results",
    "/tmp/benchmark_results",
    "./benchmark_results"
]

# Tables Gold à analyser
GOLD_TABLES = [
    "mart_performance_etablissement",
    "mart_diagnostic_epidemio", 
    "mart_demographie",
    "mart_professionnel",
    "mart_deces_localisation_2019",
    "mart_satisfaction_region_2020"
]

# ============================================================================
# INITIALISATION SPARK
# ============================================================================

def create_spark_session():
    """Crée la session Spark optimisée pour les benchmarks"""
    spark = SparkSession.builder \
        .appName("Gold_Performance_Benchmark") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================================
# REQUÊTES MÉTIER (basées sur les 8 exigences)
# ============================================================================

REQUETES_METIER = {
    # ========================================================================
    # EXIGENCE 1: Taux de consultation par établissement et période
    # ========================================================================
    "Q1_taux_consultation_etablissement": {
        "description": "Taux de consultation des patients dans un établissement X sur une période Y",
        "sql": """
            SELECT
                finess_site,
                raison_sociale_site,
                region,
                annee,
                trimestre,
                nb_consultations,
                nb_patients_consultations,
                taux_consultation_par_patient,
                duree_moyenne_consultation
            FROM mart_performance_etablissement
            WHERE annee >= 2019
                AND nb_consultations > 0
            ORDER BY nb_consultations DESC
            LIMIT 100
        """,
        "table": "mart_performance_etablissement",
        "type": "Simple agrégation avec filtre temporel"
    },

    # ========================================================================
    # EXIGENCE 2: Taux de consultation par diagnostic et période
    # ========================================================================
    "Q2_taux_consultation_diagnostic": {
        "description": "Taux de consultation des patients par rapport à un diagnostic X sur une période Y",
        "sql": """
            SELECT
                code_diag,
                diagnostic,
                type_pathologie,
                gravite_pathologie,
                annee,
                trimestre,
                nb_consultations,
                nb_patients_consultes,
                taux_consultation_diagnostic_pct
            FROM mart_diagnostic_epidemio
            WHERE annee >= 2019
                AND nb_consultations > 10
            ORDER BY taux_consultation_diagnostic_pct DESC
            LIMIT 50
        """,
        "table": "mart_diagnostic_epidemio",
        "type": "Agrégation avec pourcentage"
    },

    # ========================================================================
    # EXIGENCE 3: Taux global d'hospitalisation par période
    # ========================================================================
    "Q3_taux_global_hospitalisation": {
        "description": "Taux global d'hospitalisation des patients dans une période donnée",
        "sql": """
            SELECT
                annee,
                trimestre,
                SUM(nb_hospitalisations) as total_hospitalisations,
                SUM(nb_patients_hospitalises) as total_patients,
                AVG(taux_hospitalisation_pct) as taux_moyen_hospitalisation,
                AVG(duree_moyenne_sejour) as duree_moyenne_sejour_global
            FROM mart_performance_etablissement
            WHERE annee >= 2019
            GROUP BY annee, trimestre
            ORDER BY annee DESC, trimestre DESC
        """,
        "table": "mart_performance_etablissement",
        "type": "Agrégation globale multi-niveaux"
    },

    # ========================================================================
    # EXIGENCE 4: Taux d'hospitalisation par diagnostic
    # ========================================================================
    "Q4_taux_hospitalisation_diagnostic": {
        "description": "Taux d'hospitalisation des patients par rapport à des diagnostics",
        "sql": """
            SELECT
                code_diag,
                diagnostic,
                type_pathologie,
                gravite_pathologie,
                annee,
                SUM(nb_hospitalisations) as total_hospitalisations,
                SUM(nb_patients_hospitalises) as total_patients_hospitalises,
                AVG(taux_hospitalisation_diagnostic_pct) as taux_moyen_hospitalisation,
                AVG(duree_moyenne_sejour_diag) as duree_moyenne_sejour
            FROM mart_diagnostic_epidemio
            WHERE annee >= 2019
                AND nb_hospitalisations > 0
            GROUP BY code_diag, diagnostic, type_pathologie, gravite_pathologie, annee
            ORDER BY total_hospitalisations DESC
            LIMIT 30
        """,
        "table": "mart_diagnostic_epidemio",
        "type": "Agrégation avec GROUP BY multiple"
    },

    # ========================================================================
    # EXIGENCE 5: Taux d'hospitalisation par sexe et âge
    # ========================================================================
    "Q5_taux_hospitalisation_demo": {
        "description": "Taux d'hospitalisation par sexe et par âge",
        "sql": """
            SELECT
                sexe,
                categorie_age,
                annee,
                trimestre,
                SUM(nb_hospitalisations) as total_hospitalisations,
                SUM(nb_patients_hospitalises) as total_patients,
                AVG(duree_moyenne_sejour) as duree_moyenne,
                SUM(total_jours_hospitalisation) as total_jours,
                AVG(taux_rehospitalisation_pct) as taux_rehospitalisation_moyen
            FROM mart_demographie
            GROUP BY sexe, categorie_age, annee, trimestre
            ORDER BY annee DESC, total_hospitalisations DESC
        """,
        "table": "mart_demographie",
        "type": "Analyse démographique multi-dimensions"
    },

    # ========================================================================
    # EXIGENCE 6: Taux de consultation par professionnel
    # ========================================================================
    "Q6_taux_consultation_professionnel": {
        "description": "Taux de consultation par professionnel de santé",
        "sql": """
            SELECT
                professionnel_id,
                profession,
                categorie_professionnelle,
                code_specialite,
                nb_consultations_total,
                nb_patients_uniques,
                nb_diagnostics_distincts,
                taux_consultation_par_patient,
                duree_moyenne_consultation,
                taux_consultations_longues_pct,
                nb_regions_activite
            FROM mart_professionnel
            WHERE nb_consultations_total > 100
            ORDER BY taux_consultation_par_patient DESC
            LIMIT 50
        """,
        "table": "mart_professionnel",
        "type": "Analyse individuelle professionnels"
    },

    # ========================================================================
    # EXIGENCE 7: Nombre de décès par localisation (2019)
    # ========================================================================
    "Q7_deces_localisation_2019": {
        "description": "Nombre de décès par localisation et région (année 2019)",
        "sql": """
            SELECT
                region,
                departement,
                zone_geographique,
                nb_deces_total,
                age_moyen_deces,
                nb_deces_hommes,
                nb_deces_femmes,
                taux_deces_hommes_pct,
                taux_deces_femmes_pct,
                nb_deces_75_plus,
                nb_deces_longevite
            FROM mart_deces_localisation_2019
            WHERE annee = 2019
            ORDER BY nb_deces_total DESC
        """,
        "table": "mart_deces_localisation_2019",
        "type": "Analyse géographique année spécifique"
    },

    # ========================================================================
    # EXIGENCE 8: Taux de satisfaction par région (2020)
    # ========================================================================
    "Q8_satisfaction_region_2020": {
        "description": "Taux global de satisfaction par région (année 2020)",
        "sql": """
            SELECT
                region,
                nb_etablissements_evalues,
                score_satisfaction_moyen,
                taux_recommandation_moyen,
                score_min,
                score_max,
                ecart_type,
                nb_etab_excellente,
                nb_etab_bonne,
                classement_global,
                taux_etablissements_satisfaisants_pct
            FROM mart_satisfaction_region_2020
            WHERE annee = 2020
            ORDER BY score_satisfaction_moyen DESC
        """,
        "table": "mart_satisfaction_region_2020",
        "type": "Analyse satisfaction qualité"
    },

    # ========================================================================
    # REQUÊTE COMPLEXE: Jointure multi-tables
    # ========================================================================
    "Q9_analyse_complete_etablissement": {
        "description": "Analyse complète établissement (consultations + hospitalisations + satisfaction)",
        "sql": """
            SELECT
                pe.finess_site,
                pe.raison_sociale_site,
                pe.region,
                pe.annee,
                SUM(pe.nb_consultations) as total_consultations,
                SUM(pe.nb_hospitalisations) as total_hospitalisations,
                AVG(pe.taux_consultation_par_patient) as taux_moyen_consultation,
                AVG(pe.duree_moyenne_consultation) as duree_consultation_moy,
                AVG(pe.duree_moyenne_sejour) as duree_sejour_moy
            FROM mart_performance_etablissement pe
            WHERE pe.annee >= 2019
            GROUP BY pe.finess_site, pe.raison_sociale_site, pe.region, pe.annee
            HAVING SUM(pe.nb_consultations) > 50
            ORDER BY total_consultations DESC
            LIMIT 100
        """,
        "table": "mart_performance_etablissement",
        "type": "Requête complexe avec HAVING"
    },

    # ========================================================================
    # REQUÊTE D'ANALYSE TEMPORELLE
    # ========================================================================
    "Q10_evolution_temporelle_diagnostics": {
        "description": "Évolution temporelle des diagnostics les plus fréquents",
        "sql": """
            SELECT
                code_diag,
                diagnostic,
                annee,
                trimestre,
                periode_annee,
                SUM(nb_consultations) as consultations_periode,
                SUM(nb_hospitalisations) as hospitalisations_periode,
                AVG(taux_consultation_diagnostic_pct) as taux_consultation_moyen,
                AVG(taux_hospitalisation_diagnostic_pct) as taux_hospitalisation_moyen
            FROM mart_diagnostic_epidemio
            WHERE annee >= 2019
            GROUP BY code_diag, diagnostic, annee, trimestre, periode_annee
            ORDER BY annee, trimestre, consultations_periode DESC
        """,
        "table": "mart_diagnostic_epidemio",
        "type": "Analyse série temporelle"
    }
}

# ============================================================================
# FONCTIONS D'ANALYSE DES TABLES GOLD
# ============================================================================

def get_table_info(spark, table_name):
    """Récupère les informations d'une table Gold"""
    try:
        df = spark.read.parquet(f"{GOLD_BUCKET}/{table_name}")
        return df
    except Exception as e:
        print(f"  ❌ Erreur lecture table {table_name}: {e}")
        return None

def analyze_table_schema(df, table_name):
    """Analyse le schéma d'une table"""
    try:
        schema_data = []
        for field in df.schema.fields:
            schema_data.append({
                'Colonne': field.name,
                'Type': str(field.dataType),
                'Nullable': field.nullable
            })
        return pd.DataFrame(schema_data)
    except Exception as e:
        print(f"  ❌ Erreur analyse schéma {table_name}: {e}")
        return None

def analyze_data_quality(df, table_name, top_n=15):
    """Analyse la qualité des données d'une table"""
    try:
        quality_data = []
        for column in df.columns[:top_n]:  # Limiter aux premières colonnes pour performance
            try:
                total_count = df.count()
                non_null_count = df.filter(col(column).isNotNull()).count()
                distinct_count = df.select(column).distinct().count()
                
                quality_data.append({
                    'Colonne': column,
                    'Non_Null': non_null_count,
                    'Taux_Non_Null': f"{(non_null_count/total_count)*100:.1f}%" if total_count > 0 else "0%",
                    'Valeurs_Distinctes': distinct_count
                })
            except:
                # Si une colonne pose problème, on continue avec les autres
                continue
                
        return pd.DataFrame(quality_data)
    except Exception as e:
        print(f"  ❌ Erreur analyse qualité {table_name}: {e}")
        return None

def show_sample_data(df, table_name, n=3):
    """Affiche un échantillon des données"""
    try:
        sample_pd = df.limit(n).toPandas()
        return sample_pd
    except Exception as e:
        print(f"  ❌ Erreur échantillon {table_name}: {e}")
        return None

# ============================================================================
# GÉNÉRATION DU RAPPORT DÉTAILLÉ DES TABLES GOLD
# ============================================================================

def generate_detailed_report_all_tables(spark, tables, output_dir):
    """Génère un rapport détaillé pour TOUTES les tables Gold."""
    print(f"\n🎯 GÉNÉRATION DU RAPPORT DÉTAILLÉ GOLD POUR TOUTES LES TABLES...")
    print(f"📊 {len(tables)} tables Gold à analyser")
    
    # Créer un sous-répertoire pour les rapports Gold
    gold_report_dir = os.path.join(output_dir, "gold_tables_analysis")
    os.makedirs(gold_report_dir, exist_ok=True)
    
    # Fichier de rapport principal
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    main_report_path = f"{gold_report_dir}/gold_analysis_report_{timestamp}.md"
    
    with open(main_report_path, 'w', encoding='utf-8') as f:
        f.write(f"# RAPPORT D'ANALYSE COMPLÈTE - COUCHE GOLD\n\n")
        f.write(f"**Date de génération**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Nombre de tables analysées**: {len(tables)}\n")
        f.write(f"**Bucket Gold**: `{GOLD_BUCKET}`\n\n")
        
        # Résumé exécutif
        f.write("## 📊 RÉSUMÉ EXÉCUTIF\n\n")
        
        total_rows = 0
        total_columns = 0
        table_summaries = []
        
        for i, table_name in enumerate(tables, 1):
            print(f"🔍 Analyse de la table Gold {i}/{len(tables)}: {table_name}")
            
            df = get_table_info(spark, table_name)
            if df is not None:
                row_count = df.count()
                column_count = len(df.columns)
                total_rows += row_count
                total_columns += column_count
                
                table_summaries.append({
                    'table': table_name,
                    'rows': row_count,
                    'columns': column_count
                })
                
                f.write(f"### {i}. {table_name}\n")
                f.write(f"- **Lignes**: {row_count:,}\n")
                f.write(f"- **Colonnes**: {column_count}\n")
                
                # Schéma
                schema_df = analyze_table_schema(df, table_name)
                if schema_df is not None:
                    f.write(f"- **Schéma (premières colonnes)**:\n")
                    f.write("```\n")
                    f.write(schema_df.head(10).to_string(index=False))
                    f.write("\n```\n")
                
                # Qualité des données
                quality_df = analyze_data_quality(df, table_name, 10)
                if quality_df is not None:
                    f.write(f"- **Qualité des données (top 10 colonnes)**:\n")
                    f.write("```\n")
                    f.write(quality_df.to_string(index=False))
                    f.write("\n```\n")
                
                # Échantillon
                sample_df = show_sample_data(df, table_name, 2)
                if sample_df is not None:
                    f.write(f"- **Échantillon (2 lignes)**:\n")
                    f.write("```\n")
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    f.write(sample_df.to_string(index=False))
                    f.write("\n```\n")
                
                f.write("\n" + "-" * 50 + "\n\n")
        
        # Statistiques globales
        f.write("## 📈 STATISTIQUES GLOBALES GOLD\n\n")
        f.write(f"- **Total des lignes**: {total_rows:,}\n")
        f.write(f"- **Total des colonnes**: {total_columns}\n")
        f.write(f"- **Moyenne colonnes/table**: {total_columns/len(tables):.1f}\n")
        f.write(f"- **Moyenne lignes/table**: {total_rows/len(tables):.1f}\n\n")
        
        # Tables les plus volumineuses
        f.write("## 🏆 TABLES GOLD LES PLUS VOLUMINEUSES\n\n")
        sorted_tables = sorted(table_summaries, key=lambda x: x['rows'], reverse=True)
        for i, table in enumerate(sorted_tables[:10], 1):
            f.write(f"{i}. **{table['table']}**: {table['rows']:,} lignes, {table['columns']} colonnes\n")
        
        # Indicateurs métier disponibles
        f.write("\n## 💼 INDICATEURS MÉTIER DISPONIBLES\n\n")
        
        f.write("### 🏥 Performance Établissements:\n")
        f.write("- Taux de consultation par patient\n")
        f.write("- Durée moyenne des consultations\n")
        f.write("- Taux d'hospitalisation\n")
        f.write("- Durée moyenne de séjour\n")
        f.write("- Indicateurs de satisfaction\n\n")
        
        f.write("### 🩺 Épidémiologie et Diagnostics:\n")
        f.write("- Prévalence des diagnostics\n")
        f.write("- Taux de consultation par pathologie\n")
        f.write("- Taux d'hospitalisation par diagnostic\n")
        f.write("- Analyse par gravité et type de pathologie\n\n")
        
        f.write("### 👥 Démographie et Parcours:\n")
        f.write("- Analyse par sexe et âge\n")
        f.write("- Taux de réhospitalisation\n")
        f.write("- Durée de séjour par catégorie\n")
        f.write("- Indicateurs de longévité\n\n")
        
        f.write("### 👨‍⚕️ Performance Professionnels:\n")
        f.write("- Productivité des professionnels\n")
        f.write("- Nombre de patients uniques\n")
        f.write("- Diversité des diagnostics traités\n")
        f.write("- Couverture géographique\n\n")
        
        f.write("### 📍 Analyse Géographique:\n")
        f.write("- Performance par région\n")
        f.write("- Satisfaction par territoire\n")
        f.write("- Décès par localisation\n")
        f.write("- Indicateurs de longévité régionale\n\n")
        
        # Recommandations
        f.write("## 🎯 RECOMMANDATIONS POUR L'UTILISATION\n\n")
        f.write("### ✅ Points Forts:\n")
        f.write("- Données agrégées et optimisées pour l'analyse\n")
        f.write("- Indicateurs métier pré-calculés\n")
        f.write("- Structure dimensionnelle pour le reporting\n")
        f.write("- Données temporelles complètes (2019-2020+)\n")
        f.write("- Segments démographiques et géographiques définis\n\n")
        
        f.write("### 🚀 Optimisations Possibles:\n")
        f.write("- Partitionnement par année/région pour les grandes tables\n")
        f.write("- Indexation sur les colonnes fréquemment filtrées\n")
        f.write("- Mise en cache des Data Marts les plus utilisés\n")
        f.write("- Agrégats pré-calculés pour les requêtes complexes\n\n")
    
    print(f"\n✅ RAPPORT GOLD GÉNÉRÉ AVEC SUCCÈS!")
    print(f"📁 Fichier: {main_report_path}")
    print(f"📊 {len(tables)} tables Gold analysées")
    print(f"📈 {total_rows:,} lignes de données agrégées")
    print(f"💼 {total_columns} indicateurs métier disponibles")
    
    # Générer également un CSV récapitulatif
    summary_df = pd.DataFrame(table_summaries)
    csv_path = f"{gold_report_dir}/gold_tables_summary_{timestamp}.csv"
    summary_df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"📋 CSV récapitulatif Gold: {csv_path}")
    
    return gold_report_dir

# ============================================================================
# FONCTIONS DE BENCHMARK
# ============================================================================

def execute_query_with_timing(spark, query_id, query_info, iteration):
    """
    Exécute une requête et mesure son temps de réponse
    """
    table_name = query_info["table"]
    sql_query = query_info["sql"]

    # Lire la table depuis Gold
    try:
        df = spark.read.parquet(f"{GOLD_BUCKET}/{table_name}")
        df.createOrReplaceTempView(table_name)
    except Exception as e:
        print(f"  ⚠️  Erreur lecture table {table_name}: {e}")
        return None

    # Mesure du temps d'exécution
    start_time = time.time()

    try:
        result_df = spark.sql(sql_query)
        nb_lignes = result_df.count()
        end_time = time.time()
        execution_time = end_time - start_time

        # Calculer le débit (lignes/seconde)
        throughput = nb_lignes / execution_time if execution_time > 0 else 0

        return {
            "query_id": query_id,
            "iteration": iteration,
            "execution_time_ms": execution_time * 1000,
            "nb_lignes": nb_lignes,
            "throughput_rows_per_sec": throughput,
            "success": True
        }

    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"  ❌ Erreur exécution: {e}")
        return {
            "query_id": query_id,
            "iteration": iteration,
            "execution_time_ms": execution_time * 1000,
            "nb_lignes": 0,
            "throughput_rows_per_sec": 0,
            "success": False,
            "error": str(e)
        }

def run_benchmark(spark):
    """
    Exécute le benchmark complet sur toutes les requêtes
    """
    print("\n" + "="*80)
    print("🚀 DÉMARRAGE DU BENCHMARK GOLD LAYER")
    print("="*80)
    print(f"📊 Nombre de requêtes: {len(REQUETES_METIER)}")
    print(f"🔄 Itérations par requête: {NB_ITERATIONS}")
    print(f"📍 Bucket Gold: {GOLD_BUCKET}")
    print("="*80 + "\n")

    all_results = []

    for idx, (query_id, query_info) in enumerate(REQUETES_METIER.items(), 1):
        print(f"\n[{idx}/{len(REQUETES_METIER)}] 📋 {query_id}")
        print(f"    Description: {query_info['description']}")
        print(f"    Type: {query_info['type']}")
        print(f"    Table: {query_info['table']}")

        query_results = []

        for iteration in range(1, NB_ITERATIONS + 1):
            print(f"    🔄 Itération {iteration}/{NB_ITERATIONS}...", end=" ")

            result = execute_query_with_timing(spark, query_id, query_info, iteration)

            if result and result["success"]:
                print(f"✅ {result['execution_time_ms']:.2f} ms ({result['nb_lignes']} lignes)")
                query_results.append(result)
                all_results.append({
                    **result,
                    "description": query_info["description"],
                    "type": query_info["type"],
                    "table": query_info["table"]
                })
            else:
                print("❌ Échec")

        # Calculer les statistiques pour cette requête
        if query_results:
            avg_time = np.mean([r["execution_time_ms"] for r in query_results])
            min_time = np.min([r["execution_time_ms"] for r in query_results])
            max_time = np.max([r["execution_time_ms"] for r in query_results])
            print(f"    📊 Temps moyen: {avg_time:.2f} ms (min: {min_time:.2f}, max: {max_time:.2f})")

    print("\n" + "="*80)
    print("✅ BENCHMARK TERMINÉ")
    print("="*80)

    return all_results

# ============================================================================
# GESTION DES RÉPERTOIRES DE SORTIE
# ============================================================================

def get_output_directory():
    """
    Trouve un répertoire de sortie accessible
    """
    for output_dir in OUTPUT_DIRS:
        try:
            os.makedirs(output_dir, exist_ok=True)
            # Test d'écriture
            test_file = os.path.join(output_dir, "test_write.txt")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            print(f"📁 Répertoire de sortie sélectionné: {output_dir}")
            return output_dir
        except (PermissionError, OSError) as e:
            print(f"⚠️  Impossible d'utiliser {output_dir}: {e}")
            continue
    
    # Fallback au répertoire courant
    fallback_dir = "./benchmark_results"
    os.makedirs(fallback_dir, exist_ok=True)
    print(f"📁 Utilisation du répertoire fallback: {fallback_dir}")
    return fallback_dir

# ============================================================================
# GÉNÉRATION DE GRAPHIQUES
# ============================================================================

def create_performance_graphs(results_df, output_dir):
    """
    Génère des graphiques de performance
    """
    print("\n📊 GÉNÉRATION DES GRAPHIQUES...")

    # Configuration du style
    sns.set_style("whitegrid")
    sns.set_palette("husl")

    # Créer le sous-dossier pour les graphiques
    graphs_dir = os.path.join(output_dir, "graphs")
    os.makedirs(graphs_dir, exist_ok=True)

    try:
        # GRAPHIQUE 1: Temps de réponse moyen par requête
        print("  📈 Graphique 1: Temps de réponse par requête...")
        plt.figure(figsize=(14, 8))
        avg_times = results_df.groupby('query_id')['execution_time_ms'].mean().sort_values(ascending=False)
        colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(avg_times)))
        bars = plt.barh(range(len(avg_times)), avg_times.values, color=colors)
        plt.yticks(range(len(avg_times)), avg_times.index)
        plt.xlabel('Temps de réponse moyen (ms)', fontsize=12, fontweight='bold')
        plt.ylabel('Requête', fontsize=12, fontweight='bold')
        plt.title('⏱️  Temps de Réponse Moyen par Requête\nCouche Gold - Performance des Data Marts',
                  fontsize=14, fontweight='bold', pad=20)
        for i, (bar, value) in enumerate(zip(bars, avg_times.values)):
            plt.text(value + 5, i, f'{value:.1f} ms', va='center', fontsize=9, fontweight='bold')
        plt.tight_layout()
        plt.savefig(f"{graphs_dir}/01_temps_reponse_par_requete.png", dpi=300, bbox_inches='tight')
        plt.close()

        # GRAPHIQUE 2: Débit (lignes/seconde) par requête
        print("  📈 Graphique 2: Débit (lignes/seconde)...")
        plt.figure(figsize=(14, 8))
        avg_throughput = results_df.groupby('query_id')['throughput_rows_per_sec'].mean().sort_values(ascending=False)
        colors = plt.cm.YlGn(np.linspace(0.3, 0.9, len(avg_throughput)))
        bars = plt.barh(range(len(avg_throughput)), avg_throughput.values, color=colors)
        plt.yticks(range(len(avg_throughput)), avg_throughput.index)
        plt.xlabel('Débit (lignes/seconde)', fontsize=12, fontweight='bold')
        plt.ylabel('Requête', fontsize=12, fontweight='bold')
        plt.title('🚀 Débit Moyen par Requête\nNombre de lignes traitées par seconde',
                  fontsize=14, fontweight='bold', pad=20)
        for i, (bar, value) in enumerate(zip(bars, avg_throughput.values)):
            plt.text(value + 5, i, f'{value:.1f} rows/s', va='center', fontsize=9, fontweight='bold')
        plt.tight_layout()
        plt.savefig(f"{graphs_dir}/02_debit_par_requete.png", dpi=300, bbox_inches='tight')
        plt.close()

        print(f"    ✅ Graphiques sauvegardés dans: {graphs_dir}/")

    except Exception as e:
        print(f"❌ Erreur lors de la génération des graphiques: {e}")

# ============================================================================
# GÉNÉRATION DU RAPPORT DE PERFORMANCE
# ============================================================================

def generate_performance_report(results_df, output_dir):
    """
    Génère un rapport de performance détaillé
    """
    report_path = f"{output_dir}/PERFORMANCE_REPORT.md"

    print(f"\n📝 GÉNÉRATION DU RAPPORT: {report_path}")

    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 📊 Rapport de Performance - Gold Layer\n\n")
            f.write(f"**Date d'exécution**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Bucket Gold**: `{GOLD_BUCKET}`\n\n")
            f.write(f"**Nombre de requêtes testées**: {len(REQUETES_METIER)}\n\n")
            f.write(f"**Itérations par requête**: {NB_ITERATIONS}\n\n")
            f.write("---\n\n")

            # Statistiques globales
            f.write("## 📈 Statistiques Globales\n\n")
            f.write("| Métrique | Valeur |\n")
            f.write("|----------|--------|\n")
            f.write(f"| Temps total d'exécution | {results_df['execution_time_ms'].sum() / 1000:.2f} secondes |\n")
            f.write(f"| Temps moyen par requête | {results_df['execution_time_ms'].mean():.2f} ms |\n")
            f.write(f"| Temps min | {results_df['execution_time_ms'].min():.2f} ms |\n")
            f.write(f"| Temps max | {results_df['execution_time_ms'].max():.2f} ms |\n")
            f.write(f"| Écart-type | {results_df['execution_time_ms'].std():.2f} ms |\n")
            f.write(f"| Débit moyen | {results_df['throughput_rows_per_sec'].mean():.2f} lignes/s |\n")
            f.write(f"| Total lignes traitées | {results_df['nb_lignes'].sum():,} |\n\n")

            # Performance par requête
            f.write("## 🔍 Performance par Requête\n\n")
            f.write("| Requête | Description | Type | Temps Moyen (ms) | Lignes | Débit (rows/s) |\n")
            f.write("|---------|-------------|------|------------------|--------|----------------|\n")

            query_stats = results_df.groupby(['query_id', 'description', 'type']).agg({
                'execution_time_ms': 'mean',
                'nb_lignes': 'mean',
                'throughput_rows_per_sec': 'mean'
            }).reset_index().sort_values('execution_time_ms')

            for _, row in query_stats.iterrows():
                f.write(f"| `{row['query_id']}` | {row['description']} | {row['type']} | "
                       f"{row['execution_time_ms']:.2f} | {int(row['nb_lignes'])} | "
                       f"{row['throughput_rows_per_sec']:.2f} |\n")

            f.write("\n---\n\n")

            # Top 5 requêtes les plus rapides
            f.write("## ⚡ Top 5 Requêtes les Plus Rapides\n\n")
            top_fast = query_stats.nsmallest(5, 'execution_time_ms')
            for idx, row in enumerate(top_fast.itertuples(), 1):
                f.write(f"{idx}. **{row.query_id}** - {row.execution_time_ms:.2f} ms\n")
                f.write(f"   - {row.description}\n\n")

            # Top 5 requêtes les plus lentes
            f.write("## 🐌 Top 5 Requêtes les Plus Lentes\n\n")
            top_slow = query_stats.nlargest(5, 'execution_time_ms')
            for idx, row in enumerate(top_slow.itertuples(), 1):
                f.write(f"{idx}. **{row.query_id}** - {row.execution_time_ms:.2f} ms\n")
                f.write(f"   - {row.description}\n\n")

        print(f"✅ Rapport généré: {report_path}")

    except Exception as e:
        print(f"❌ Erreur lors de la génération du rapport: {e}")

# ============================================================================
# EXPORT DES REQUÊTES SQL
# ============================================================================

def export_sql_queries(output_dir):
    """
    Exporte toutes les requêtes SQL dans des fichiers séparés
    """
    sql_dir = os.path.join(output_dir, "sql_queries")
    os.makedirs(sql_dir, exist_ok=True)

    print(f"\n📁 EXPORT DES REQUÊTES SQL: {sql_dir}")

    for query_id, query_info in REQUETES_METIER.items():
        filename = f"{sql_dir}/{query_id}.sql"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"-- {query_id}\n")
                f.write(f"-- Description: {query_info['description']}\n")
                f.write(f"-- Type: {query_info['type']}\n")
                f.write(f"-- Table: {query_info['table']}\n")
                f.write("--\n")
                f.write(query_info['sql'].strip())
                f.write("\n")
            print(f"  ✅ {filename}")
        except Exception as e:
            print(f"  ❌ Erreur export {filename}: {e}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Point d'entrée principal"""

    print("\n" + "╔" + "="*78 + "╗")
    print("║" + " "*20 + "BENCHMARK PERFORMANCE - GOLD LAYER" + " "*24 + "║")
    print("║" + " "*20 + "Évaluation des Data Marts" + " "*33 + "║")
    print("╚" + "="*78 + "╝")

    # Obtenir un répertoire de sortie accessible
    output_dir = get_output_directory()

    # Créer la session Spark
    spark = create_spark_session()

    try:
        # 1. ANALYSE DES TABLES GOLD
        gold_report_dir = generate_detailed_report_all_tables(spark, GOLD_TABLES, output_dir)

        # 2. EXÉCUTION DU BENCHMARK
        results = run_benchmark(spark)

        if not results:
            print("\n❌ Aucun résultat à analyser")
            return 1

        # Convertir en DataFrame pandas
        results_df = pd.DataFrame(results)

        # 3. SAUVEGARDE DES RÉSULTATS BRUTS
        results_df.to_csv(f"{output_dir}/benchmark_results.csv", index=False)
        print(f"\n💾 Résultats bruts sauvegardés: {output_dir}/benchmark_results.csv")

        # 4. GÉNÉRATION DES GRAPHIQUES
        create_performance_graphs(results_df, output_dir)

        # 5. GÉNÉRATION DU RAPPORT
        generate_performance_report(results_df, output_dir)

        # 6. EXPORT DES REQUÊTES SQL
        export_sql_queries(output_dir)

        print("\n" + "="*80)
        print("🎉 BENCHMARK TERMINÉ AVEC SUCCÈS!")
        print("="*80)
        print(f"\n📂 TOUS LES FICHIERS SONT DISPONIBLES DANS: {output_dir}/")
        print("\n📋 FICHIERS GÉNÉRÉS:")
        print("  - benchmark_results.csv           (données brutes du benchmark)")
        print("  - PERFORMANCE_REPORT.md           (rapport de performance)")
        print("  - sql_queries/                    (requêtes SQL individuelles)")
        print("  - graphs/                         (graphiques de performance)")
        print("  - gold_tables_analysis/           (analyse détaillée des Data Marts)")
        print("    ├── gold_analysis_report_*.md   (rapport complet)")
        print("    └── gold_tables_summary_*.csv   (récapitulatif)")
        print("\n📊 RÉCAPITULATIF:")
        print(f"  - {len(GOLD_TABLES)} tables Gold analysées")
        print(f"  - {len(REQUETES_METIER)} requêtes métier benchmarkées")
        print(f"  - {results_df['nb_lignes'].sum():,} lignes traitées au total")
        print(f"  - Temps moyen: {results_df['execution_time_ms'].mean():.2f} ms")
        print("\n")

        return 0

    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())