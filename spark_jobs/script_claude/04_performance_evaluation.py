#!/usr/bin/env python3
"""
Script d'évaluation de performance et génération de graphiques
Auteur: Claude
Date: 2025-10-24
Description: Évalue les performances d'accès et génère des graphiques depuis la couche Gold
"""

import os
import sys
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, 
    min as spark_min, max as spark_max, when, lit, 
    year, month, quarter, desc, row_number, 
    concat, regexp_extract, hour, datediff, floor
)
from pyspark.sql.types import *

# Configuration
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "silver_bucket": "silver",
    "gold_bucket": "gold"
}

# Configuration pour les graphiques
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def get_spark_session():
    """Session Spark pour évaluation de performance"""
    try:
        spark = SparkSession.builder \
            .appName("Healthcare Performance Evaluation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def measure_query_performance(spark, query_func, query_name, iterations=3):
    """Mesure la performance d'une requête avec plusieurs itérations"""
    times = []
    results = []
    
    print(f"📊 Test performance: {query_name}")
    
    for i in range(iterations):
        start_time = time.time()
        try:
            result = query_func()
            if hasattr(result, 'count'):
                count = result.count()
            elif hasattr(result, 'collect'):
                count = len(result.collect())
            else:
                count = len(result) if isinstance(result, list) else 1
            
            execution_time = time.time() - start_time
            times.append(execution_time)
            results.append(count)
            
            print(f"   Itération {i+1}: {execution_time:.2f}s ({count:,} résultats)")
            
        except Exception as e:
            print(f"   ❌ Erreur itération {i+1}: {e}")
            times.append(float('inf'))
            results.append(0)
    
    # Statistiques
    valid_times = [t for t in times if t != float('inf')]
    if valid_times:
        avg_time = np.mean(valid_times)
        min_time = np.min(valid_times)
        max_time = np.max(valid_times)
        std_time = np.std(valid_times)
        
        print(f"   📈 Résumé: {avg_time:.2f}s (±{std_time:.2f}s), min: {min_time:.2f}s, max: {max_time:.2f}s")
        
        return {
            "query_name": query_name,
            "avg_time": avg_time,
            "min_time": min_time,
            "max_time": max_time,
            "std_time": std_time,
            "iterations": len(valid_times),
            "avg_results": np.mean(results),
            "success_rate": len(valid_times) / iterations * 100
        }
    else:
        print(f"   ❌ Toutes les itérations ont échoué")
        return {
            "query_name": query_name,
            "avg_time": float('inf'),
            "success_rate": 0
        }

def create_performance_test_queries(spark):
    """Crée les requêtes de test de performance sur les tables GOLD"""
    
    # Lecture des tables GOLD - exigences métier
    try:
        exigence_1 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_1_consultation_etablissement")
        exigence_2 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_2_consultation_diagnostic")
        exigence_3 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_3_hospitalisation_global")
        exigence_4 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_4_hospitalisation_diagnostic")
        exigence_5 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_5_hospitalisation_demographie")
        exigence_6 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_6_consultation_professionnel")
        exigence_7 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_7_deces_localisation_2019")
        exigence_8 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_8_satisfaction_region_2020")
        
        print("✅ Tables Gold chargées pour tests de performance")
    except Exception as e:
        print(f"❌ Erreur chargement tables Gold: {e}")
        return {}
    
    # Définition des requêtes de test sur les tables GOLD
    queries = {}
    
    # Query 1: Simple COUNT sur exigence 1 - Consultation par établissement
    def query_gold_consultation_etablissement():
        return exigence_1.count()
    queries["Q1_Gold_Consultation_Etablissement"] = query_gold_consultation_etablissement
    
    # Query 2: Filtrage temporel sur exigence 2 - Consultation par diagnostic
    def query_gold_consultation_diagnostic_filter():
        return exigence_2.filter(col("annee") == 2016)
    queries["Q2_Gold_Consultation_Diagnostic"] = query_gold_consultation_diagnostic_filter
    
    # Query 3: Agrégation sur exigence 3 - Hospitalisation globale
    def query_gold_hospitalisation_global():
        return exigence_3.groupBy("annee").agg(
            spark_sum("nb_hospitalisations_total").alias("total_hosp_annee"),
            avg("taux_hospitalisation_global_pct").alias("taux_moyen_annee")
        )
    queries["Q3_Gold_Hospitalisation_Global"] = query_gold_hospitalisation_global
    
    # Query 4: Analyse exigence 4 - Hospitalisation par diagnostic
    def query_gold_hospitalisation_diagnostic():
        return exigence_4.filter(col("taux_hospitalisation_diagnostic_pct") > 1.0)
    queries["Q4_Gold_Hospitalisation_Diagnostic"] = query_gold_hospitalisation_diagnostic
    
    # Query 5: Analyse démographique - Exigence 5
    def query_gold_demographie():
        return exigence_5.orderBy(desc("taux_hospitalisation_categorie_pct")).limit(10)
    queries["Q5_Gold_Demographie"] = query_gold_demographie
    
    # Query 6: Top professionnels - Exigence 6 (ajusté pour avoir des résultats)
    def query_gold_professionnels():
        # D'abord vérifier les valeurs disponibles, puis prendre le top 10
        return exigence_6.filter(col("nb_consultations_total") > 100) \
                        .orderBy(desc("nb_consultations_total")) \
                        .limit(10)
    queries["Q6_Gold_Professionnels"] = query_gold_professionnels
    
    # Query 7: Analyse géographique décès 2019 - Exigence 7
    def query_gold_deces_2019():
        return exigence_7.groupBy("region_deces").agg(
            spark_sum("nb_deces_total").alias("total_deces_region"),
            avg("age_moyen_deces").alias("age_moyen_region")
        ).orderBy(desc("total_deces_region"))
    queries["Q7_Gold_Deces_2019"] = query_gold_deces_2019
    
    # Query 8: Satisfaction par région 2020 - Exigence 8
    def query_gold_satisfaction_2020():
        return exigence_8.filter(col("score_satisfaction_moyen") > 70) \
                        .orderBy(desc("score_satisfaction_moyen"))
    queries["Q8_Gold_Satisfaction_2020"] = query_gold_satisfaction_2020
    
    # Query 9: Jointure Gold - Consultation et Hospitalisation
    def query_gold_cross_analysis():
        consult_summary = exigence_1.groupBy("region").agg(
            spark_sum("nb_consultations_total").alias("total_consultations_region")
        )
        hosp_summary = exigence_3.groupBy("region").agg(
            spark_sum("nb_hospitalisations_total").alias("total_hospitalisations_region")
        ) if "region" in exigence_3.columns else exigence_3.limit(0)
        
        if hosp_summary.count() > 0:
            return consult_summary.join(hosp_summary, "region", "inner")
        else:
            return consult_summary
    queries["Q9_Gold_Cross_Analysis"] = query_gold_cross_analysis
    
    # Query 10: Analyse métier complexe - Performance globale
    def query_gold_performance_globale():
        # Analyse de performance croisée entre différentes exigences
        best_regions_satisfaction = exigence_8.select("region", "score_satisfaction_moyen") \
                                              .filter(col("score_satisfaction_moyen") > 75)
        
        consultation_performance = exigence_1.groupBy("region").agg(
            avg("taux_consultation_par_patient").alias("taux_consult_moyen_region")
        )
        
        return best_regions_satisfaction.join(consultation_performance, "region", "inner")
    queries["Q10_Gold_Performance_Globale"] = query_gold_performance_globale
    
    return queries

def run_performance_tests(spark):
    """Exécute tous les tests de performance"""
    print("\n🚀 TESTS DE PERFORMANCE")
    print("=" * 60)
    
    queries = create_performance_test_queries(spark)
    performance_results = []
    
    for query_name, query_func in queries.items():
        result = measure_query_performance(spark, query_func, query_name, iterations=3)
        performance_results.append(result)
        time.sleep(1)  # Pause entre les tests
    
    return performance_results

def analyze_performance_by_complexity(performance_results):
    """Analyse les performances par niveau de complexité"""
    
    # Classification des requêtes par complexité
    complexity_mapping = {
        "Q1_Simple_Count": "Simple",
        "Q2_Filter_Date": "Simple", 
        "Q3_Group_Region": "Moyen",
        "Q4_Simple_Join": "Moyen",
        "Q5_Complex_Aggregation": "Complexe",
        "Q6_Multiple_Joins": "Complexe",
        "Q7_Window_Functions": "Complexe",
        "Q8_Business_Complex": "Très Complexe",
        "Q9_Gold_Direct": "Simple",
        "Q10_Full_Scan_Transform": "Complexe"
    }
    
    # Ajouter la complexité aux résultats
    for result in performance_results:
        result["complexity"] = complexity_mapping.get(result["query_name"], "Moyen")
    
    # Analyse par complexité
    complexity_analysis = {}
    for complexity in ["Simple", "Moyen", "Complexe", "Très Complexe"]:
        complex_queries = [r for r in performance_results if r["complexity"] == complexity]
        if complex_queries:
            avg_times = [r["avg_time"] for r in complex_queries if r["avg_time"] != float('inf')]
            if avg_times:
                complexity_analysis[complexity] = {
                    "count": len(complex_queries),
                    "avg_time": np.mean(avg_times),
                    "min_time": np.min(avg_times),
                    "max_time": np.max(avg_times)
                }
    
    return complexity_analysis

def create_performance_visualizations(performance_results):
    """Crée les visualisations de performance"""
    print("\n📊 GÉNÉRATION GRAPHIQUES PERFORMANCE")
    print("=" * 60)
    
    # Préparation des données
    valid_results = [r for r in performance_results if r["avg_time"] != float('inf')]
    
    if not valid_results:
        print("❌ Pas de données valides pour visualisation")
        return
    
    # Créer une figure avec plusieurs sous-graphiques
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Analyse Performance - Entrepôt de Données Santé', fontsize=16, fontweight='bold')
    
    # Graphique 1: Temps de réponse par requête
    query_names = [r["query_name"].replace("_", "\n") for r in valid_results]
    avg_times = [r["avg_time"] for r in valid_results]
    colors = plt.cm.viridis(np.linspace(0, 1, len(query_names)))
    
    bars1 = ax1.bar(query_names, avg_times, color=colors)
    ax1.set_title('Temps de Réponse par Type de Requête')
    ax1.set_ylabel('Temps (secondes)')
    ax1.tick_params(axis='x', rotation=45)
    
    # Ajouter les valeurs sur les barres
    for bar, time_val in zip(bars1, avg_times):
        height = bar.get_height()
        ax1.annotate(f'{time_val:.2f}s',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)
    
    # Graphique 2: Performance par complexité
    complexity_data = analyze_performance_by_complexity(valid_results)
    
    if complexity_data:
        complexities = list(complexity_data.keys())
        complex_avg_times = [complexity_data[c]["avg_time"] for c in complexities]
        
        bars2 = ax2.bar(complexities, complex_avg_times, 
                       color=['#2ecc71', '#f39c12', '#e74c3c', '#9b59b6'])
        ax2.set_title('Performance Moyenne par Niveau de Complexité')
        ax2.set_ylabel('Temps Moyen (secondes)')
        
        # Ajouter les valeurs
        for bar, time_val in zip(bars2, complex_avg_times):
            height = bar.get_height()
            ax2.annotate(f'{time_val:.2f}s',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom')
    
    # Graphique 3: Distribution des temps de réponse
    all_times = [r["avg_time"] for r in valid_results]
    ax3.hist(all_times, bins=min(10, len(all_times)), alpha=0.7, color='skyblue', edgecolor='black')
    ax3.set_title('Distribution des Temps de Réponse')
    ax3.set_xlabel('Temps (secondes)')
    ax3.set_ylabel('Fréquence')
    
    # Ajouter statistiques
    mean_time = np.mean(all_times)
    median_time = np.median(all_times)
    ax3.axvline(mean_time, color='red', linestyle='--', label=f'Moyenne: {mean_time:.2f}s')
    ax3.axvline(median_time, color='green', linestyle='--', label=f'Médiane: {median_time:.2f}s')
    ax3.legend()
    
    # Graphique 4: Évolution théorique des performances (simulation)
    # Simulation de l'évolution des performances avec l'optimisation
    scenarios = ['Actuel', 'Partitionné', 'Bucketé', 'Optimisé']
    baseline_time = np.mean(all_times)
    performance_evolution = [
        baseline_time,
        baseline_time * 0.4,  # 60% amélioration avec partitionnement
        baseline_time * 0.25, # 75% amélioration avec bucketing
        baseline_time * 0.15  # 85% amélioration avec optimisation complète
    ]
    
    line = ax4.plot(scenarios, performance_evolution, marker='o', linewidth=3, markersize=8)
    ax4.set_title('Évolution Théorique des Performances')
    ax4.set_ylabel('Temps Moyen (secondes)')
    ax4.grid(True, alpha=0.3)
    
    # Ajouter les valeurs et pourcentages d'amélioration
    for i, (scenario, time_val) in enumerate(zip(scenarios, performance_evolution)):
        improvement = ((baseline_time - time_val) / baseline_time * 100) if i > 0 else 0
        label = f'{time_val:.2f}s' if i == 0 else f'{time_val:.2f}s\n(-{improvement:.0f}%)'
        ax4.annotate(label,
                    xy=(i, time_val),
                    xytext=(0, 10),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Créer un répertoire pour les graphiques de performance
    performance_dir = "/home/jovyan/performance_reports"
    os.makedirs(performance_dir, exist_ok=True)
    chart_path = f"{performance_dir}/performance_analysis.png"
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    print(f"✅ Graphique performance sauvegardé: {chart_path}")
    
    return chart_path

def create_business_metrics_visualizations(spark):
    """Crée des visualisations des métriques métier depuis les tables Gold"""
    print("\n📈 GRAPHIQUES MÉTRIQUES MÉTIER - COUCHE GOLD")
    print("=" * 60)
    
    try:
        # Lire les données depuis les tables Gold - exigences métier
        exigence_1 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_1_consultation_etablissement")
        exigence_3 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_3_hospitalisation_global")
        exigence_7 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_7_deces_localisation_2019")
        exigence_8 = spark.read.parquet(f"s3a://{MINIO_CONFIG['gold_bucket']}/exigence_8_satisfaction_region_2020")
        
        # Créer une figure pour les métriques métier Gold
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Indicateurs Métier Gold - Système de Santé', fontsize=16, fontweight='bold')
        
        # Métrique 1: Top 10 établissements par nb consultations (Gold Exigence 1)
        top_etablissements = exigence_1 \
            .groupBy("consultation_etablissement_nom", "region") \
            .agg(spark_sum("nb_consultations_total").alias("total_consultations")) \
            .orderBy(desc("total_consultations")) \
            .limit(10) \
            .toPandas()
        
        if not top_etablissements.empty:
            ax1.barh(range(len(top_etablissements)), top_etablissements['total_consultations'])
            ax1.set_yticks(range(len(top_etablissements)))
            ax1.set_yticklabels([nom[:25] + '...' if len(nom) > 25 else nom 
                               for nom in top_etablissements['consultation_etablissement_nom']])
            ax1.set_title('Top 10 Établissements - Total Consultations (Gold)')
            ax1.set_xlabel('Nombre de Consultations')
            
            # Ajouter les valeurs
            for i, v in enumerate(top_etablissements['total_consultations']):
                ax1.text(v + max(top_etablissements['total_consultations']) * 0.01, i, f'{v:,}', 
                        va='center', fontsize=8)
        
        # Métrique 2: Évolution taux hospitalisation global par année (Gold Exigence 3)
        hosp_evolution = exigence_3 \
            .groupBy("annee") \
            .agg(avg("taux_hospitalisation_global_pct").alias("taux_moyen")) \
            .orderBy("annee") \
            .toPandas()
        
        if not hosp_evolution.empty:
            ax2.plot(hosp_evolution['annee'], hosp_evolution['taux_moyen'], 
                    marker='o', linewidth=3, markersize=8, color='#e74c3c')
            ax2.set_title('Évolution Taux Hospitalisation Global (Gold)')
            ax2.set_xlabel('Année')
            ax2.set_ylabel('Taux Hospitalisation (%)')
            ax2.grid(True, alpha=0.3)
            
            # Ajouter les valeurs
            for i, (annee, taux) in hosp_evolution.iterrows():
                ax2.annotate(f'{taux:.2f}%', (annee, taux), 
                           textcoords="offset points", xytext=(0,10), ha='center')
        
        # Métrique 3: Décès par région 2019 (Gold Exigence 7)
        deces_2019_gold = exigence_7 \
            .select("region_deces", "nb_deces_total") \
            .orderBy(desc("nb_deces_total")) \
            .limit(8) \
            .toPandas()
        
        if not deces_2019_gold.empty:
            wedges, texts, autotexts = ax3.pie(deces_2019_gold['nb_deces_total'], 
                                              labels=deces_2019_gold['region_deces'],
                                              autopct='%1.1f%%', startangle=90)
            ax3.set_title('Répartition des Décès par Région 2019 (Gold)')
            
            # Améliorer la lisibilité
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
        
        # Métrique 4: Satisfaction par région 2020 (Gold Exigence 8)
        satisfaction_2020_gold = exigence_8 \
            .select("region", "score_satisfaction_moyen") \
            .orderBy(desc("score_satisfaction_moyen")) \
            .toPandas()
        
        if not satisfaction_2020_gold.empty:
            bars = ax4.bar(range(len(satisfaction_2020_gold)), satisfaction_2020_gold['score_satisfaction_moyen'])
            ax4.set_xticks(range(len(satisfaction_2020_gold)))
            ax4.set_xticklabels(satisfaction_2020_gold['region'], rotation=45, ha='right')
            ax4.set_title('Score Satisfaction par Région 2020 (Gold)')
            ax4.set_ylabel('Score de Satisfaction')
            
            # Colorier selon le score
            for i, (bar, score) in enumerate(zip(bars, satisfaction_2020_gold['score_satisfaction_moyen'])):
                if score >= 80:
                    bar.set_color('#2ecc71')  # Vert
                elif score >= 70:
                    bar.set_color('#f39c12')  # Orange
                else:
                    bar.set_color('#e74c3c')  # Rouge
                
                # Ajouter la valeur
                height = bar.get_height()
                ax4.annotate(f'{score:.1f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        
        # Créer un répertoire pour les métriques métier
        business_dir = "/home/jovyan/business_reports"
        os.makedirs(business_dir, exist_ok=True)
        business_chart_path = f"{business_dir}/business_metrics.png"
        plt.savefig(business_chart_path, dpi=300, bbox_inches='tight')
        print(f"✅ Graphique métriques métier sauvegardé: {business_chart_path}")
        
        return business_chart_path
        
    except Exception as e:
        print(f"❌ Erreur création graphiques métier: {e}")
        return None

def generate_performance_report(performance_results, complexity_analysis):
    """Génère un rapport détaillé de performance"""
    
    valid_results = [r for r in performance_results if r["avg_time"] != float('inf')]
    
    print(f"""
    
╔══════════════════════════════════════════════════════════════════╗
║                    RAPPORT PERFORMANCE                          ║
║                ENTREPÔT DE DONNÉES SANTÉ                       ║
╚══════════════════════════════════════════════════════════════════╝

📊 RÉSUMÉ EXÉCUTIF:
   🔍 Requêtes testées: {len(performance_results)}
   ✅ Requêtes réussies: {len(valid_results)}
   📈 Taux de succès: {len(valid_results)/len(performance_results)*100:.1f}%

⏱️  TEMPS DE RÉPONSE:
""")
    
    if valid_results:
        all_times = [r["avg_time"] for r in valid_results]
        avg_overall = np.mean(all_times)
        median_overall = np.median(all_times)
        min_overall = np.min(all_times)
        max_overall = np.max(all_times)
        
        print(f"   📊 Temps moyen global: {avg_overall:.2f}s")
        print(f"   📊 Temps médian: {median_overall:.2f}s")
        print(f"   ⚡ Plus rapide: {min_overall:.2f}s")
        print(f"   🐌 Plus lent: {max_overall:.2f}s")
        
        # Performance par requête
        print(f"\n📋 DÉTAIL PAR REQUÊTE:")
        for result in sorted(valid_results, key=lambda x: x["avg_time"]):
            print(f"   {result['query_name']}: {result['avg_time']:.2f}s (±{result.get('std_time', 0):.2f}s)")
        
        # Performance par complexité
        if complexity_analysis:
            print(f"\n🎯 PERFORMANCE PAR COMPLEXITÉ:")
            for complexity, stats in complexity_analysis.items():
                print(f"   {complexity}: {stats['avg_time']:.2f}s (moyenne de {stats['count']} requêtes)")
        
        # Recommandations
        print(f"\n💡 RECOMMANDATIONS D'OPTIMISATION:")
        
        if avg_overall > 5:
            print(f"   🔴 CRITIQUE: Temps moyen élevé ({avg_overall:.2f}s)")
            print(f"      → Partitionnement par date recommandé")
            print(f"      → Bucketing sur clés de jointure")
            print(f"      → Optimisation des requêtes complexes")
        elif avg_overall > 2:
            print(f"   🟡 ATTENTION: Performances moyennes ({avg_overall:.2f}s)")
            print(f"      → Partitionnement recommandé")
            print(f"      → Cache des tables fréquentes")
        else:
            print(f"   ✅ EXCELLENT: Bonnes performances ({avg_overall:.2f}s)")
            print(f"      → Maintenir les optimisations actuelles")
        
        # Requêtes les plus lentes
        slowest = sorted(valid_results, key=lambda x: x["avg_time"], reverse=True)[:3]
        print(f"\n🐌 TOP 3 REQUÊTES À OPTIMISER:")
        for i, result in enumerate(slowest, 1):
            print(f"   {i}. {result['query_name']}: {result['avg_time']:.2f}s")
        
        # Estimation des gains
        print(f"\n📈 GAINS POTENTIELS AVEC OPTIMISATION:")
        print(f"   🎯 Partitionnement: -60% temps (estimation)")
        print(f"   🪣 Bucketing: -75% sur jointures (estimation)")
        print(f"   📦 Compression: -30% I/O (estimation)")
        print(f"   ⚡ Optimisation complète: -85% temps global (estimation)")
    
    print(f"""

🎯 PROCHAINES ÉTAPES:
   1. Implémenter partitionnement par année/région
   2. Configurer bucketing sur id_patient
   3. Optimiser les requêtes les plus lentes
   4. Mettre en place monitoring continu

📅 Rapport généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔍 Script: performance_evaluation.py
""")

def main():
    """Fonction principale"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║              ÉVALUATION PERFORMANCE - TABLES GOLD              ║
║               ENTREPÔT DE DONNÉES SANTÉ                         ║
║                                                                  ║
║  🎯 Objectif: Mesurer performances couche GOLD métier           ║
║  📊 Tests: 10 requêtes sur exigences métier                     ║
║  📈 Output: Graphiques Gold + recommandations optimisation      ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    start_time = time.time()
    
    try:
        # Initialisation
        spark = get_spark_session()
        print("✅ Session Spark initialisée")
        
        # Tests de performance
        print("🚀 Lancement des tests de performance...")
        performance_results = run_performance_tests(spark)
        
        # Analyse
        complexity_analysis = analyze_performance_by_complexity(performance_results)
        
        # Visualisations
        perf_chart = create_performance_visualizations(performance_results)
        business_chart = create_business_metrics_visualizations(spark)
        
        # Rapport
        generate_performance_report(performance_results, complexity_analysis)
        
        # Nettoyage
        spark.stop()
        
        duration = time.time() - start_time
        print(f"\n⏱️  Évaluation terminée en {duration:.2f} secondes")
        
        # Résumé final
        valid_results = [r for r in performance_results if r["avg_time"] != float('inf')]
        if valid_results:
            avg_time = np.mean([r["avg_time"] for r in valid_results])
            print(f"📊 Performance moyenne globale: {avg_time:.2f}s")
            print(f"📈 Graphiques générés: {'/home/jovyan/performance_reports/, /home/jovyan/business_reports/'}")
        
        return True
        
    except Exception as e:
        print(f"💥 ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)