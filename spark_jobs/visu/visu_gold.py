import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, asc, mean, stddev, min, max, 
    countDistinct, isnan, isnull, sum as spark_sum, lit,
    input_file_name, regexp_extract, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Configuration MinIO Gold
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "bucket": "gold"
}

def get_spark_session():
    """Session Spark pour la visualisation des données Gold."""
    try:
        jars_dir = "/home/jovyan/jars"
        jar_files = [
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
            f"{jars_dir}/hadoop-common-3.3.4.jar"
        ]
        
        # Vérification des JARs
        existing_jars = []
        for jar in jar_files:
            if os.path.exists(jar):
                existing_jars.append(jar)
            else:
                print(f"⚠️  JAR manquant: {jar}")
        
        jars_path = ",".join(existing_jars)
        
        # Configuration Spark
        builder = SparkSession.builder \
            .appName("Gold Data Visualizer") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        
        # Ajouter les JARs seulement s'ils existent
        if jars_path:
            builder = builder.config("spark.jars", jars_path)
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Gold Visualizer initialisé avec configuration S3A")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        raise

def list_all_gold_tables(spark):
    """Liste TOUTES les tables Gold disponibles."""
    try:
        gold_path = f"s3a://{MINIO_CONFIG['bucket']}/"
        print(f"🔍 Recherche de TOUTES les tables Gold dans: {gold_path}")
        
        # Liste des tables Gold attendues (mise à jour complète)
        expected_gold_tables = [
            # Dimensions
            "dim_temps",
            "dim_patient",
            "dim_diagnostic",
            "dim_etablissement",
            "dim_professionnel",
            "dim_localisation",
            "dim_mutuelle",
            "dim_medicaments",
            "dim_laboratoire",
            "dim_salle",
            "dim_specialites",
            # Tables de faits
            "fact_consultation",
            "fact_hospitalisation",
            "fact_deces",
            "fact_prescription",
            # Data Marts
            "mart_performance_etablissement",
            "mart_diagnostic_epidemio",
            "mart_demographie",
            "mart_professionnel",
            "mart_deces_localisation_2019",
            "mart_satisfaction_region_2020",
            "mart_superset_consolide",
        ]
        
        found_tables = []
        
        for table in expected_gold_tables:
            try:
                table_path = f"{gold_path}{table}"
                # Essayer de lire une seule ligne pour vérifier si la table existe
                test_df = spark.read.parquet(table_path).limit(1)
                count = test_df.count()
                found_tables.append(table)
                print(f"  ✅ {table}")
            except Exception as e:
                print(f"  ❌ {table}: {e}")
        
        if found_tables:
            print(f"🎯 {len(found_tables)} tables Gold trouvées")
            return found_tables
        else:
            print("❌ Aucune table Gold trouvée")
            return []
        
    except Exception as e:
        print(f"❌ Erreur listing tables Gold: {e}")
        return []

def get_gold_table_info(spark, table_name):
    """Récupère les informations détaillées d'une table Gold."""
    try:
        gold_path = f"s3a://{MINIO_CONFIG['bucket']}/{table_name}"
        
        # Lecture des données
        df = spark.read.option("mergeSchema", "true").parquet(gold_path)
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lecture table Gold {table_name}: {e}")
        return None

def analyze_gold_schema(df, table_name):
    """Analyse le schéma et les types de données Gold."""
    schema_data = []
    for field in df.schema.fields:
        schema_data.append({
            "Colonne": field.name,
            "Type": str(field.dataType),
            "Nullable": field.nullable
        })
    
    schema_df = pd.DataFrame(schema_data)
    return schema_df

def analyze_gold_data_quality(df, table_name):
    """Analyse la qualité des données Gold."""
    quality_data = []
    total_rows = df.count()
    
    if total_rows == 0:
        return None
    
    for column in df.columns:
        try:
            # Statistiques de base
            null_count = df.filter(col(column).isNull()).count()
            distinct_count = df.select(column).distinct().count()
            completeness = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
            
            # Type spécifique
            dtype = str(df.schema[column].dataType)
            
            # Statistiques numériques si applicable
            numeric_stats = ""
            if "Double" in dtype or "Long" in dtype or "Integer" in dtype:
                try:
                    stats = df.select(
                        mean(col(column)).alias('mean'),
                        stddev(col(column)).alias('stddev'),
                        min(col(column)).alias('min'),
                        max(col(column)).alias('max')
                    ).collect()[0]
                    
                    if stats['mean'] is not None:
                        numeric_stats = f"μ:{stats['mean']:.2f} σ:{stats['stddev']:.2f}"
                except:
                    numeric_stats = "N/A"
            
            quality_data.append({
                "Colonne": column,
                "Type": dtype,
                "Complétude": f"{completeness:.1f}%",
                "Nulles": null_count,
                "Distinctes": distinct_count,
                "Taux Distinct": f"{(distinct_count/total_rows*100):.1f}%" if total_rows > 0 else "0%",
                "Stats": numeric_stats
            })
        except Exception as e:
            print(f"⚠️  Erreur analyse colonne {column}: {e}")
    
    if quality_data:
        quality_df = pd.DataFrame(quality_data)
        return quality_df
    else:
        return None

def show_gold_sample_data(df, table_name, sample_size=5):
    """Affiche un échantillon des données Gold."""
    try:
        # Convertir en Pandas pour un affichage plus lisible
        sample_df = df.limit(sample_size).toPandas()
        
        if sample_df.empty:
            return None
            
        return sample_df
        
    except Exception as e:
        print(f"❌ Erreur affichage échantillon Gold: {e}")
        return None

def generate_gold_comprehensive_report(spark, table_name):
    """Génère un rapport complet pour une table Gold."""
    print(f"\n{'='*80}")
    print(f"🌟 RAPPORT GOLD COMPLET: {table_name}")
    print(f"{'='*80}")
    
    df = get_gold_table_info(spark, table_name)
    if df is None:
        return
    
    # Informations de base
    row_count = df.count()
    column_count = len(df.columns)
    
    print(f"📈 STATISTIQUES GOLD (INDICATEURS MÉTIER):")
    print(f"   • Lignes: {row_count:,}")
    print(f"   • Colonnes: {column_count}")
    
    # Afficher les premières colonnes
    if df.columns:
        columns_preview = ", ".join(df.columns[:8])
        if len(df.columns) > 8:
            columns_preview += f" ... (+{len(df.columns) - 8} autres)"
        print(f"   • Colonnes: {columns_preview}")
    
    # Analyses successives
    schema_df = analyze_gold_schema(df, table_name)
    print(f"\n🏗️  SCHÉMA DES INDICATEURS MÉTIER - '{table_name}':")
    print("-" * 80)
    print(schema_df.to_string(index=False))
    
    quality_df = analyze_gold_data_quality(df, table_name)
    if quality_df is not None:
        print(f"\n🔍 QUALITÉ DES INDICATEURS MÉTIER - '{table_name}':")
        print("-" * 80)
        print(quality_df.to_string(index=False))
    
    sample_df = show_gold_sample_data(df, table_name, 5)
    if sample_df is not None:
        print(f"\n📋 ÉCHANTILLON DES INDICATEURS MÉTIER - '{table_name}' (5 lignes):")
        print("-" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(sample_df.to_string(index=False))

def create_gold_dashboard(spark, table_name):
    """Crée un dashboard visuel pour une table Gold."""
    df = get_gold_table_info(spark, table_name)
    if df is None:
        return
    
    pdf = df.toPandas()
    
    print(f"\n📊 DASHBOARD VISUEL GOLD: {table_name}")
    print("=" * 60)
    
    # Déterminer le type de dashboard basé sur le nom de la table
    if "consultation" in table_name.lower():
        create_consultation_dashboard(pdf, table_name)
    elif "hospitalisation" in table_name.lower():
        create_hospitalisation_dashboard(pdf, table_name)
    elif "deces" in table_name.lower():
        create_deces_dashboard(pdf, table_name)
    elif "satisfaction" in table_name.lower():
        create_satisfaction_dashboard(pdf, table_name)
    else:
        create_general_dashboard(pdf, table_name)

def create_consultation_dashboard(pdf, table_name):
    """Dashboard pour les indicateurs de consultation."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'Dashboard Consultation - {table_name}', fontsize=16, fontweight='bold')
    
    try:
        # Graphique 1: Consultations par région
        if 'region' in pdf.columns and 'nb_consultations' in pdf.columns:
            region_consult = pdf.groupby('region')['nb_consultations'].sum().nlargest(10)
            axes[0,0].barh(range(len(region_consult)), region_consult.values)
            axes[0,0].set_yticks(range(len(region_consult)))
            axes[0,0].set_yticklabels(region_consult.index)
            axes[0,0].set_title('Top 10 Régions - Consultations')
            axes[0,0].set_xlabel('Nombre de consultations')
        
        # Graphique 2: Taux de consultation
        if 'taux_consultation_patients' in pdf.columns:
            pdf['taux_consultation_patients'].hist(bins=20, ax=axes[0,1], alpha=0.7)
            axes[0,1].axvline(pdf['taux_consultation_patients'].mean(), color='red', linestyle='--', label=f'Moyenne: {pdf["taux_consultation_patients"].mean():.2f}')
            axes[0,1].set_title('Distribution des Taux de Consultation')
            axes[0,1].set_xlabel('Taux de consultation')
            axes[0,1].legend()
        
        # Graphique 3: Évolution temporelle
        if 'annee' in pdf.columns and 'trimestre' in pdf.columns and 'nb_consultations' in pdf.columns:
            pdf['periode'] = pdf['annee'].astype(str) + '-T' + pdf['trimestre'].astype(str)
            evolution = pdf.groupby('periode')['nb_consultations'].sum()
            axes[1,0].plot(evolution.index, evolution.values, marker='o', linewidth=2)
            axes[1,0].set_title('Évolution des Consultations')
            axes[1,0].set_xlabel('Période')
            axes[1,0].set_ylabel('Nombre de consultations')
            axes[1,0].tick_params(axis='x', rotation=45)
        
        # Graphique 4: Top établissements
        if 'consultation_etablissement_nom' in pdf.columns and 'nb_consultations' in pdf.columns:
            top_etabs = pdf.groupby('consultation_etablissement_nom')['nb_consultations'].sum().nlargest(8)
            axes[1,1].pie(top_etabs.values, labels=top_etabs.index, autopct='%1.1f%%', startangle=90)
            axes[1,1].set_title('Répartition par Établissement (Top 8)')
        
    except Exception as e:
        print(f"⚠️  Erreur création dashboard: {e}")
    
    plt.tight_layout()
    plt.show()

def create_hospitalisation_dashboard(pdf, table_name):
    """Dashboard pour les indicateurs d'hospitalisation."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'Dashboard Hospitalisation - {table_name}', fontsize=16, fontweight='bold')
    
    try:
        # Graphique 1: Taux par catégorie d'âge
        if 'categorie_age' in pdf.columns and 'taux_hospitalisation_categorie' in pdf.columns:
            age_taux = pdf.groupby('categorie_age')['taux_hospitalisation_categorie'].mean().sort_values(ascending=False)
            axes[0,0].bar(range(len(age_taux)), age_taux.values)
            axes[0,0].set_xticks(range(len(age_taux)))
            axes[0,0].set_xticklabels(age_taux.index, rotation=45)
            axes[0,0].set_title('Taux d\'Hospitalisation par Âge')
            axes[0,0].set_ylabel('Taux d\'hospitalisation')
        
        # Graphique 2: Durée moyenne de séjour
        if 'duree_moyenne_sejour' in pdf.columns:
            pdf['duree_moyenne_sejour'].hist(bins=15, ax=axes[0,1], alpha=0.7, color='orange')
            axes[0,1].axvline(pdf['duree_moyenne_sejour'].mean(), color='red', linestyle='--', label=f'Moyenne: {pdf["duree_moyenne_sejour"].mean():.1f} jours')
            axes[0,1].set_title('Distribution Durée de Séjour')
            axes[0,1].set_xlabel('Jours')
            axes[0,1].legend()
        
        # Graphique 3: Hospitalisations par sexe
        if 'sexe' in pdf.columns and 'nb_hospitalisations' in pdf.columns:
            sexe_hosp = pdf.groupby('sexe')['nb_hospitalisations'].sum()
            axes[1,0].bar(sexe_hosp.index, sexe_hosp.values, color=['lightblue', 'lightpink'])
            axes[1,0].set_title('Hospitalisations par Sexe')
            axes[1,0].set_ylabel('Nombre d\'hospitalisations')
        
        # Graphique 4: Top diagnostics
        if 'diagnostic' in pdf.columns and 'nb_hospitalisations_diagnostic' in pdf.columns:
            top_diag = pdf.groupby('diagnostic')['nb_hospitalisations_diagnostic'].sum().nlargest(8)
            axes[1,1].barh(range(len(top_diag)), top_diag.values)
            axes[1,1].set_yticks(range(len(top_diag)))
            axes[1,1].set_yticklabels([d[:30] + '...' if len(d) > 30 else d for d in top_diag.index])
            axes[1,1].set_title('Top 8 Diagnostics - Hospitalisations')
            axes[1,1].set_xlabel('Nombre d\'hospitalisations')
        
    except Exception as e:
        print(f"⚠️  Erreur création dashboard hospitalisation: {e}")
    
    plt.tight_layout()
    plt.show()

def create_satisfaction_dashboard(pdf, table_name):
    """Dashboard pour les indicateurs de satisfaction."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'Dashboard Satisfaction - {table_name}', fontsize=16, fontweight='bold')
    
    try:
        # Graphique 1: Score de satisfaction par région
        if 'region' in pdf.columns and 'score_satisfaction_moyen' in pdf.columns:
            region_satisf = pdf.groupby('region')['score_satisfaction_moyen'].mean().sort_values(ascending=False)
            colors = ['green' if x >= 80 else 'orange' if x >= 70 else 'red' for x in region_satisf.values]
            bars = axes[0,0].bar(range(len(region_satisf)), region_satisf.values, color=colors)
            axes[0,0].set_xticks(range(len(region_satisf)))
            axes[0,0].set_xticklabels(region_satisf.index, rotation=45)
            axes[0,0].set_title('Score de Satisfaction par Région')
            axes[0,0].set_ylabel('Score de satisfaction')
            axes[0,0].axhline(80, color='green', linestyle='--', alpha=0.7, label='Excellent (80+)')
            axes[0,0].axhline(70, color='orange', linestyle='--', alpha=0.7, label='Bon (70+)')
            axes[0,0].legend()
        
        # Graphique 2: Distribution des scores
        if 'score_satisfaction_moyen' in pdf.columns:
            pdf['score_satisfaction_moyen'].hist(bins=15, ax=axes[0,1], alpha=0.7, color='lightgreen')
            axes[0,1].axvline(pdf['score_satisfaction_moyen'].mean(), color='red', linestyle='--', label=f'Moyenne: {pdf["score_satisfaction_moyen"].mean():.1f}')
            axes[0,1].set_title('Distribution des Scores de Satisfaction')
            axes[0,1].set_xlabel('Score')
            axes[0,1].legend()
        
        # Graphique 3: Taux de recommandation
        if 'taux_recommandation_moyen' in pdf.columns:
            pdf['taux_recommandation_moyen'].hist(bins=15, ax=axes[1,0], alpha=0.7, color='lightblue')
            axes[1,0].axvline(pdf['taux_recommandation_moyen'].mean(), color='red', linestyle='--', label=f'Moyenne: {pdf["taux_recommandation_moyen"].mean():.1f}%')
            axes[1,0].set_title('Distribution Taux de Recommandation')
            axes[1,0].set_xlabel('Taux de recommandation (%)')
            axes[1,0].legend()
        
        # Graphique 4: Correlation satisfaction/recommandation
        if 'score_satisfaction_moyen' in pdf.columns and 'taux_recommandation_moyen' in pdf.columns:
            axes[1,1].scatter(pdf['score_satisfaction_moyen'], pdf['taux_recommandation_moyen'], alpha=0.6)
            axes[1,1].set_xlabel('Score de Satisfaction')
            axes[1,1].set_ylabel('Taux de Recommandation')
            axes[1,1].set_title('Correlation Satisfaction vs Recommandation')
            
            # Ajouter une ligne de tendance
            z = np.polyfit(pdf['score_satisfaction_moyen'], pdf['taux_recommandation_moyen'], 1)
            p = np.poly1d(z)
            axes[1,1].plot(pdf['score_satisfaction_moyen'], p(pdf['score_satisfaction_moyen']), "r--", alpha=0.8)
        
    except Exception as e:
        print(f"⚠️  Erreur création dashboard satisfaction: {e}")
    
    plt.tight_layout()
    plt.show()

def create_deces_dashboard(pdf, table_name):
    """Dashboard pour les indicateurs de décès."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'Dashboard Décès - {table_name}', fontsize=16, fontweight='bold')
    
    try:
        # Graphique 1: Décès par région
        if 'region_deces' in pdf.columns and 'nb_deces' in pdf.columns:
            region_deces = pdf.groupby('region_deces')['nb_deces'].sum().nlargest(10)
            axes[0,0].bar(range(len(region_deces)), region_deces.values, color='gray')
            axes[0,0].set_xticks(range(len(region_deces)))
            axes[0,0].set_xticklabels(region_deces.index, rotation=45)
            axes[0,0].set_title('Top 10 Régions - Nombre de Décès')
            axes[0,0].set_ylabel('Nombre de décès')
        
        # Graphique 2: Âge moyen par région
        if 'region_deces' in pdf.columns and 'age_moyen_deces' in pdf.columns:
            age_region = pdf.groupby('region_deces')['age_moyen_deces'].mean().sort_values(ascending=False)
            axes[0,1].bar(range(len(age_region)), age_region.values, color='lightcoral')
            axes[0,1].set_xticks(range(len(age_region)))
            axes[0,1].set_xticklabels(age_region.index, rotation=45)
            axes[0,1].set_title('Âge Moyen au Décès par Région')
            axes[0,1].set_ylabel('Âge moyen')
        
        # Graphique 3: Distribution des âges
        if 'age_moyen_deces' in pdf.columns:
            pdf['age_moyen_deces'].hist(bins=15, ax=axes[1,0], alpha=0.7, color='lightcoral')
            axes[1,0].axvline(pdf['age_moyen_deces'].mean(), color='red', linestyle='--', label=f'Moyenne: {pdf["age_moyen_deces"].mean():.1f} ans')
            axes[1,0].set_title('Distribution des Âges au Décès')
            axes[1,0].set_xlabel('Âge')
            axes[1,0].legend()
        
        # Graphique 4: Décès par département
        if 'departement_deces' in pdf.columns and 'nb_deces' in pdf.columns:
            dept_deces = pdf.groupby('departement_deces')['nb_deces'].sum().nlargest(8)
            axes[1,1].pie(dept_deces.values, labels=dept_deces.index, autopct='%1.1f%%', startangle=90)
            axes[1,1].set_title('Répartition par Département (Top 8)')
        
    except Exception as e:
        print(f"⚠️  Erreur création dashboard décès: {e}")
    
    plt.tight_layout()
    plt.show()

def create_general_dashboard(pdf, table_name):
    """Dashboard générique pour les autres tables Gold."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'Dashboard Gold - {table_name}', fontsize=16, fontweight='bold')
    
    try:
        # Graphique 1: Distribution des premières colonnes numériques
        numeric_cols = pdf.select_dtypes(include=[np.number]).columns[:4]
        for i, col in enumerate(numeric_cols[:4]):
            if i < 4:
                row, col_idx = i // 2, i % 2
                pdf[col].hist(bins=15, ax=axes[row, col_idx], alpha=0.7)
                axes[row, col_idx].set_title(f'Distribution {col}')
                axes[row, col_idx].set_xlabel(col)
        
    except Exception as e:
        print(f"⚠️  Erreur création dashboard général: {e}")
    
    plt.tight_layout()
    plt.show()

def generate_gold_summary_report(spark, tables):
    """Génère un rapport récapitulatif de toutes les tables Gold."""
    print(f"\n🎯 GÉNÉRATION DU RAPPORT GOLD COMPLET...")
    print(f"📊 {len(tables)} tables Gold à analyser")
    
    # Créer un répertoire pour les rapports Gold
    report_dir = "/home/jovyan/gold_reports"
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{report_dir}/gold_analysis_report_{timestamp}.md"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"# RAPPORT D'ANALYSE COMPLÈTE - COUCHE GOLD\n\n")
        f.write(f"**Date de génération**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Nombre de tables analysées**: {len(tables)}\n\n")
        
        f.write("## 📊 RÉSUMÉ EXÉCUTIF GOLD\n\n")
        
        total_rows = 0
        total_columns = 0
        table_summaries = []
        
        for i, table_name in enumerate(tables, 1):
            print(f"🔍 Analyse de la table Gold {i}/{len(tables)}: {table_name}")
            
            df = get_gold_table_info(spark, table_name)
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
                
                # Description de l'indicateur
                indicator_desc = get_gold_indicator_description(table_name)
                f.write(f"- **Description**: {indicator_desc}\n")
                
                # Schéma
                schema_df = analyze_gold_schema(df, table_name)
                if schema_df is not None:
                    f.write(f"- **Schéma des indicateurs**:\n")
                    f.write("```\n")
                    f.write(schema_df.to_string(index=False))
                    f.write("\n```\n")
                
                # Échantillon
                sample_df = show_gold_sample_data(df, table_name, 3)
                if sample_df is not None:
                    f.write(f"- **Échantillon (3 lignes)**:\n")
                    f.write("```\n")
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    f.write(sample_df.to_string(index=False))
                    f.write("\n```\n")
                
                f.write("\n" + "-" * 50 + "\n\n")
        
        # Statistiques globales
        f.write("## 📈 STATISTIQUES GLOBALES GOLD\n\n")
        f.write(f"- **Total des indicateurs calculés**: {total_rows:,}\n")
        f.write(f"- **Total des métriques**: {total_columns}\n")
        f.write(f"- **Moyenne métriques/table**: {total_columns/len(tables):.1f}\n")
        f.write(f"- **Moyenne indicateurs/table**: {total_rows/len(tables):.1f}\n\n")
        
        # Tables par catégorie
        f.write("## 🏆 CATÉGORIES D'INDICATEURS GOLD\n\n")
        categories = categorize_gold_tables(tables)
        for category, tables_in_category in categories.items():
            f.write(f"### {category}\n")
            for table in tables_in_category:
                f.write(f"- `{table}`\n")
            f.write("\n")
        
        f.write("## 🎯 PRÊT POUR L'ANALYSE BI\n\n")
        f.write("✅ **Tous les indicateurs métier sont calculés**\n")
        f.write("✅ **Données agrégées et optimisées**\n")
        f.write("✅ **Prêt pour la visualisation dans Superset**\n")
        f.write("✅ **KPI business définis et calculés**\n")
    
    print(f"\n✅ RAPPORT GOLD GÉNÉRÉ AVEC SUCCÈS!")
    print(f"📁 Fichier: {report_path}")
    print(f"📊 {len(tables)} tables Gold analysées")
    print(f"📈 {total_rows:,} indicateurs calculés")
    print(f"🎯 {total_columns} métriques business")

def get_gold_indicator_description(table_name):
    """Retourne la description de l'indicateur Gold."""
    descriptions = {
        "gold_taux_consultation_etablissement": "Taux de consultation des patients dans un établissement X sur une période Y",
        "gold_taux_consultation_diagnostic": "Taux de consultation des patients par rapport à un diagnostic X sur une période Y",
        "gold_taux_hospitalisation_global": "Taux global d'hospitalisation des patients dans une période donnée Y",
        "gold_taux_hospitalisation_diagnostic": "Taux d'hospitalisation des patients par rapport à des diagnostics sur une période donnée",
        "gold_taux_hospitalisation_demographie": "Taux d'hospitalisation par sexe, par âge",
        "gold_taux_consultation_professionnel": "Taux de consultation par professionnel",
        "gold_deces_localisation_2019": "Nombre de décès par localisation (région) et sur l'année 2019",
        "gold_satisfaction_region_2020": "Taux global de satisfaction par région sur l'année 2020",
        "gold_fait_principal_bi": "Table de faits principale pour analyses BI complètes"
    }
    return descriptions.get(table_name, "Indicateur métier calculé")

def categorize_gold_tables(tables):
    """Catégorise les tables Gold par type d'indicateur."""
    categories = {
        "📊 Consultation": [],
        "🏥 Hospitalisation": [],
        "😊 Satisfaction": [],
        "⚰️ Décès": [],
        "📈 Vue Consolidée": []
    }
    
    for table in tables:
        if "consultation" in table.lower():
            categories["📊 Consultation"].append(table)
        elif "hospitalisation" in table.lower():
            categories["🏥 Hospitalisation"].append(table)
        elif "satisfaction" in table.lower():
            categories["😊 Satisfaction"].append(table)
        elif "deces" in table.lower():
            categories["⚰️ Décès"].append(table)
        elif "fait" in table.lower() or "principal" in table.lower():
            categories["📈 Vue Consolidée"].append(table)
        else:
            categories["📈 Vue Consolidée"].append(table)
    
    return categories

def interactive_gold_explorer(spark):
    """Mode interactif pour explorer les données Gold."""
    while True:
        print(f"\n{'='*50}")
        print("🌟 EXPLORATEUR DE DONNÉES GOLD (INDICATEURS MÉTIER)")
        print(f"{'='*50}")
        
        # Lister TOUTES les tables Gold disponibles
        print("🔄 Recherche de toutes les tables Gold...")
        tables = list_all_gold_tables(spark)
        
        if not tables:
            print("❌ Aucune table Gold trouvée.")
            break
        
        print(f"\n🎯 {len(tables)} INDICATEURS GOLD DISPONIBLES:")
        print("-" * 50)
        for i, table in enumerate(sorted(tables), 1):
            desc = get_gold_indicator_description(table)
            print(f"{i:2d}. {table}")
            print(f"    📝 {desc}")
        print("-" * 50)
        
        print(f"\n📋 Menu Gold (Indicateurs Métier):")
        print("1. 📊 Aperçu rapide de tous les indicateurs Gold")
        print("2. 🔍 Analyser un indicateur Gold spécifique")
        print("3. 📈 Dashboard visuel pour un indicateur")
        print("4. 📑 RAPPORT COMPLET TOUS LES INDICATEURS GOLD")
        print("5. 🎯 Catégories d'indicateurs")
        print("6. 🔄 Rechercher à nouveau les tables")
        print("7. 🚪 Quitter")
        
        choice = input("\n🎯 Votre choix (1-7): ").strip()
        
        if choice == "1":
            print(f"\n🚀 APERÇU RAPIDE DES {len(tables)} INDICATEURS GOLD:")
            print("=" * 60)
            for i, table in enumerate(tables, 1):
                print(f"\n{i}/{len(tables)}: {table}")
                quick_gold_overview(spark, table)
                if i < len(tables):
                    input("\n⏎ Appuyez sur Entrée pour continuer...")
                
        elif choice == "2":
            print(f"\n📋 Indicateurs Gold disponibles ({len(tables)}):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table} - {get_gold_indicator_description(table)}")
            
            try:
                table_choice = input("\n📝 Numéro ou nom de l'indicateur Gold: ").strip()
                if table_choice.isdigit():
                    table_index = int(table_choice) - 1
                    if 0 <= table_index < len(tables):
                        table_name = tables[table_index]
                    else:
                        print("❌ Numéro invalide")
                        continue
                else:
                    table_name = table_choice
                
                if table_name in tables:
                    generate_gold_comprehensive_report(spark, table_name)
                else:
                    print(f"❌ Indicateur Gold '{table_name}' non trouvé")
                    
            except ValueError:
                print("❌ Entrée invalide")
                
        elif choice == "3":
            print(f"\n📋 Indicateurs Gold disponibles ({len(tables)}):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table} - {get_gold_indicator_description(table)}")
            
            try:
                table_choice = input("\n📝 Numéro ou nom de l'indicateur Gold: ").strip()
                if table_choice.isdigit():
                    table_index = int(table_choice) - 1
                    if 0 <= table_index < len(tables):
                        table_name = tables[table_index]
                    else:
                        print("❌ Numéro invalide")
                        continue
                else:
                    table_name = table_choice
                
                if table_name in tables:
                    create_gold_dashboard(spark, table_name)
                else:
                    print(f"❌ Indicateur Gold '{table_name}' non trouvé")
                    
            except ValueError:
                print("❌ Entrée invalide")
                
        elif choice == "4":
            print(f"\n📑 LANCEMENT DU RAPPORT COMPLET POUR {len(tables)} INDICATEURS GOLD...")
            generate_gold_summary_report(spark, tables)
                
        elif choice == "5":
            categories = categorize_gold_tables(tables)
            print(f"\n🎯 CATÉGORIES D'INDICATEURS GOLD:")
            print("=" * 50)
            for category, tables_in_category in categories.items():
                print(f"\n{category}:")
                for table in tables_in_category:
                    print(f"  • {table}")
                
        elif choice == "6":
            print("🔄 Nouvelle recherche des indicateurs Gold...")
            continue
                
        elif choice == "7":
            print("👋 Au revoir!")
            break
        else:
            print("❌ Choix invalide")

def quick_gold_overview(spark, table_name):
    """Aperçu rapide d'un indicateur Gold."""
    try:
        df = get_gold_table_info(spark, table_name)
        if df is not None:
            print(f"   • Lignes: {df.count():,}")
            print(f"   • Colonnes: {len(df.columns)}")
            print(f"   • Description: {get_gold_indicator_description(table_name)}")
            
            # Afficher quelques métriques clés
            numeric_cols = [f.name for f in df.schema.fields if "Double" in str(f.dataType) or "Long" in str(f.dataType) or "Integer" in str(f.dataType)]
            if numeric_cols:
                stats = df.select([mean(col(c)).alias(f'{c}_mean') for c in numeric_cols[:2]]).collect()[0]
                print(f"   • Métriques clés:")
                for i, col_name in enumerate(numeric_cols[:2]):
                    mean_val = stats[f'{col_name}_mean']
                    if mean_val is not None:
                        print(f"     - {col_name}: {mean_val:.2f}")
    except Exception as e:
        print(f"   ❌ Erreur: {e}")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║               GOLD DATA VISUALIZER                          ║
    ║  Exploration et visualisation des indicateurs métier Gold   ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        print("\n🎯 Démarrage de l'explorateur d'indicateurs Gold...")
        
        # Mode interactif
        interactive_gold_explorer(spark)
        
        spark.stop()
        print("\n✅ Session Spark Gold fermée")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()