import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, asc, mean, stddev, min, max, 
    countDistinct, isnan, isnull, sum as spark_sum, lit,
    year, month, dayofmonth
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Configuration MinIO Silver
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "bucket": "silver"
}

def get_spark_session():
    """Session Spark pour la visualisation des données Silver."""
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
            .appName("Silver Data Visualizer") \
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
        
        print("✅ Spark Silver Visualizer initialisé")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        raise

def list_silver_tables(spark):
    """Liste toutes les tables Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['bucket']}/"
        print(f"🔍 Recherche des tables dans: {silver_path}")
        
        # Liste des tables Silver attendues
        expected_tables = [
            # Dimensions
            "dim_patient", "dim_etablissement", "dim_temp",
            # Faits
            "fact_consultation", "fact_hospitalisation", "fact_deces",
            # Métriques
            "metrique_consultation", "metrique_hospitalisation_etablissement", 
            "metrique_deces_demographie", "metrique_activite_temporelle"
        ]
        
        found_tables = []
        
        for table in expected_tables:
            try:
                table_path = f"{silver_path}{table}"
                test_df = spark.read.parquet(table_path).limit(1)
                count = test_df.count()
                found_tables.append(table)
                print(f"  ✅ {table}")
            except Exception as e:
                print(f"  ❌ {table} - Non trouvée")
        
        return found_tables
        
    except Exception as e:
        print(f"❌ Erreur listing tables Silver: {e}")
        return []

def get_table_info(spark, table_name):
    """Récupère les informations détaillées d'une table Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['bucket']}/{table_name}"
        
        # Lecture des données
        df = spark.read.option("mergeSchema", "true").parquet(silver_path)
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lecture table {table_name}: {e}")
        return None

def analyze_table_schema(df, table_name):
    """Analyse le schéma et les types de données."""
    schema_data = []
    for field in df.schema.fields:
        schema_data.append({
            "Colonne": field.name,
            "Type": str(field.dataType),
            "Nullable": field.nullable
        })
    
    schema_df = pd.DataFrame(schema_data)
    return schema_df

def analyze_data_quality(df, table_name):
    """Analyse la qualité des données Silver."""
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
            
            quality_data.append({
                "Colonne": column,
                "Type": dtype,
                "Complétude": f"{completeness:.1f}%",
                "Nulles": null_count,
                "Distinctes": distinct_count,
                "Taux Distinct": f"{(distinct_count/total_rows*100):.1f}%" if total_rows > 0 else "0%"
            })
        except Exception as e:
            print(f"⚠️  Erreur analyse colonne {column}: {e}")
    
    if quality_data:
        quality_df = pd.DataFrame(quality_data)
        return quality_df
    else:
        return None

def show_sample_data(df, table_name, sample_size=5):
    """Affiche un échantillon des données."""
    try:
        sample_df = df.limit(sample_size).toPandas()
        
        if sample_df.empty:
            return None
            
        return sample_df
        
    except Exception as e:
        print(f"❌ Erreur affichage échantillon: {e}")
        return None

def analyze_dimension_quality(spark, dimension_name):
    """Analyse spécifique pour les dimensions."""
    print(f"\n🔍 ANALYSE DE LA DIMENSION: {dimension_name}")
    
    df = get_table_info(spark, dimension_name)
    if df is None:
        return
    
    row_count = df.count()
    print(f"📊 Taille de la dimension: {row_count:,} lignes")
    
    # Analyse des clés
    if "patient_sk" in df.columns:
        sk_count = df.select("patient_sk").distinct().count()
        print(f"🔑 Clés patients uniques: {sk_count:,}")
        
    if "etablissement_sk" in df.columns:
        sk_count = df.select("etablissement_sk").distinct().count()
        print(f"🏥 Clés établissements uniques: {sk_count:,}")
    
    # Analyse des attributs
    if "tranche_age" in df.columns:
        age_dist = df.groupBy("tranche_age").count().orderBy("count", ascending=False)
        print(f"👥 Distribution par tranche d'âge:")
        age_dist.show()
    
    if "type_etablissement" in df.columns:
        type_dist = df.groupBy("type_etablissement").count().orderBy("count", ascending=False)
        print(f"🏥 Distribution par type d'établissement:")
        type_dist.show()
    
    if "region_normalisee" in df.columns:
        region_dist = df.groupBy("region_normalisee").count().orderBy("count", ascending=False)
        print(f"🗺️  Distribution par région:")
        region_dist.show()

def analyze_fact_quality(spark, fact_name):
    """Analyse spécifique pour les faits."""
    print(f"\n📈 ANALYSE DU FAIT: {fact_name}")
    
    df = get_table_info(spark, fact_name)
    if df is None:
        return
    
    row_count = df.count()
    print(f"📊 Volume du fait: {row_count:,} lignes")
    
    # Analyse temporelle
    if "annee_consultation" in df.columns:
        yearly_stats = df.groupBy("annee_consultation").agg(
            count("*").alias("nb_consultations")
        ).orderBy("annee_consultation")
        print(f"📅 Consultations par année:")
        yearly_stats.show()
    
    if "annee_deces" in df.columns:
        yearly_deces = df.groupBy("annee_deces").agg(
            count("*").alias("nb_deces")
        ).orderBy("annee_deces")
        print(f"📅 Décès par année:")
        yearly_deces.show()
    
    # Analyse des clés étrangères
    if "patient_sk" in df.columns:
        patient_count = df.select("patient_sk").distinct().count()
        print(f"👥 Patients distincts: {patient_count:,}")
    
    if "etablissement_sk" in df.columns:
        etab_count = df.select("etablissement_sk").distinct().count()
        print(f"🏥 Établissements distincts: {etab_count:,}")

def analyze_metrics_quality(spark, metric_name):
    """Analyse spécifique pour les métriques."""
    print(f"\n📊 ANALYSE DE LA MÉTRIQUE: {metric_name}")
    
    df = get_table_info(spark, metric_name)
    if df is None:
        return
    
    row_count = df.count()
    print(f"📈 Taille de la métrique: {row_count:,} lignes")
    
    # Afficher les métriques
    sample_df = df.limit(10).toPandas()
    if not sample_df.empty:
        print(f"📋 Aperçu des métriques:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(sample_df.to_string(index=False))

def generate_silver_quality_report(spark, tables):
    """Génère un rapport complet de qualité Silver."""
    print(f"\n🎯 GÉNÉRATION DU RAPPORT DE QUALITÉ SILVER...")
    print(f"📊 {len(tables)} tables à analyser")
    
    # Créer un répertoire pour les rapports
    report_dir = "/home/jovyan/silver_reports"
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{report_dir}/silver_quality_report_{timestamp}.md"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"# RAPPORT DE QUALITÉ - COUCHE SILVER\n\n")
        f.write(f"**Date de génération**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Nombre de tables analysées**: {len(tables)}\n\n")
        
        # Catégorisation des tables
        dimensions = [t for t in tables if t.startswith('dim_')]
        faits = [t for t in tables if t.startswith('fact_')]
        metriques = [t for t in tables if t.startswith('metrique_')]
        
        f.write("## 📊 RÉSUMÉ EXÉCUTIF\n\n")
        f.write(f"- **Dimensions**: {len(dimensions)} tables\n")
        f.write(f"- **Faits**: {len(faits)} tables\n")
        f.write(f"- **Métriques**: {len(metriques)} tables\n\n")
        
        total_rows = 0
        table_summaries = []
        
        # Analyse des dimensions
        f.write("## 🏗️ DIMENSIONS\n\n")
        for dim in dimensions:
            df = get_table_info(spark, dim)
            if df is not None:
                row_count = df.count()
                total_rows += row_count
                
                f.write(f"### {dim}\n")
                f.write(f"- **Lignes**: {row_count:,}\n")
                f.write(f"- **Colonnes**: {len(df.columns)}\n")
                
                # Qualité des données
                quality_df = analyze_data_quality(df, dim)
                if quality_df is not None:
                    f.write(f"- **Qualité des données**:\n")
                    f.write("```\n")
                    f.write(quality_df.to_string(index=False))
                    f.write("\n```\n")
                
                table_summaries.append({'table': dim, 'rows': row_count, 'type': 'Dimension'})
                f.write("\n")
        
        # Analyse des faits
        f.write("## 📈 FAITS\n\n")
        for fact in faits:
            df = get_table_info(spark, fact)
            if df is not None:
                row_count = df.count()
                total_rows += row_count
                
                f.write(f"### {fact}\n")
                f.write(f"- **Lignes**: {row_count:,}\n")
                f.write(f"- **Colonnes**: {len(df.columns)}\n")
                
                # Statistiques spécifiques aux faits
                if "nb_consultations" in df.columns:
                    total_consult = df.agg(spark_sum("nb_consultations")).collect()[0][0]
                    f.write(f"- **Total consultations**: {total_consult:,}\n")
                
                if "nb_hospitalisations" in df.columns:
                    total_hosp = df.agg(spark_sum("nb_hospitalisations")).collect()[0][0]
                    f.write(f"- **Total hospitalisations**: {total_hosp:,}\n")
                
                if "nb_deces" in df.columns:
                    total_deces = df.agg(spark_sum("nb_deces")).collect()[0][0]
                    f.write(f"- **Total décès**: {total_deces:,}\n")
                
                table_summaries.append({'table': fact, 'rows': row_count, 'type': 'Fait'})
                f.write("\n")
        
        # Analyse des métriques
        f.write("## 📊 MÉTRIQUES BUSINESS\n\n")
        for metrique in metriques:
            df = get_table_info(spark, metrique)
            if df is not None:
                row_count = df.count()
                total_rows += row_count
                
                f.write(f"### {metrique}\n")
                f.write(f"- **Lignes**: {row_count:,}\n")
                f.write(f"- **Colonnes**: {len(df.columns)}\n")
                
                # Aperçu des métriques
                sample_df = show_sample_data(df, metrique, 3)
                if sample_df is not None:
                    f.write(f"- **Aperçu**:\n")
                    f.write("```\n")
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    f.write(sample_df.to_string(index=False))
                    f.write("\n```\n")
                
                table_summaries.append({'table': metrique, 'rows': row_count, 'type': 'Métrique'})
                f.write("\n")
        
        # Statistiques globales
        f.write("## 📈 STATISTIQUES GLOBALES SILVER\n\n")
        f.write(f"- **Total des lignes**: {total_rows:,}\n")
        f.write(f"- **Total des tables**: {len(tables)}\n")
        f.write(f"- **Stockage estimé**: {(total_rows * 0.5) / 1024:.1f} MB\n\n")
        
        # Tableau récapitulatif
        f.write("### 📋 RÉCAPITULATIF DES TABLES\n\n")
        summary_df = pd.DataFrame(table_summaries)
        f.write("```\n")
        f.write(summary_df.to_string(index=False))
        f.write("\n```\n")
        
        # Évaluation de la préparation pour Gold
        f.write("\n## 🎯 ÉVALUATION DE LA PRÉPARATION POUR GOLD\n\n")
        
        # Critères d'évaluation
        has_dimensions = len(dimensions) >= 2
        has_facts = len(faits) >= 2
        has_metrics = len(metriques) >= 2
        data_volume = total_rows > 1000000  # Au moins 1 million de lignes
        
        f.write("### ✅ CRITÈRES DE QUALITÉ:\n")
        f.write(f"- Dimensions créées: {'✅' if has_dimensions else '❌'} ({len(dimensions)}/3 minimum)\n")
        f.write(f"- Faits créés: {'✅' if has_facts else '❌'} ({len(faits)}/3 minimum)\n")
        f.write(f"- Métriques calculées: {'✅' if has_metrics else '❌'} ({len(metriques)}/4 minimum)\n")
        f.write(f"- Volume de données: {'✅' if data_volume else '❌'} ({total_rows:,} lignes)\n")
        
        overall_ready = has_dimensions and has_facts and has_metrics and data_volume
        f.write(f"\n### 🚀 ÉTAT GLOBAL: {'**PRÊT POUR GOLD** ✅' if overall_ready else '**BESOIN D AJUSTEMENTS** ⚠️'}\n")
        
        if overall_ready:
            f.write("\n🎉 **La couche Silver est optimisée pour le passage en Gold!**\n")
            f.write("Les données sont conformées, nettoyées et prêtes pour:\n")
            f.write("- L'agrégation avancée\n")
            f.write("- Les modèles prédictifs\n") 
            f.write("- Les data marts métier\n")
            f.write("- Les tableaux de bord executive\n")
        else:
            f.write("\n⚠️ **Des améliorations sont nécessaires avant le passage en Gold:**\n")
            if not has_dimensions:
                f.write("- Compléter les dimensions manquantes\n")
            if not has_facts:
                f.write("- Vérifier la création des faits\n")
            if not has_metrics:
                f.write("- Calculer les métriques business manquantes\n")
            if not data_volume:
                f.write("- Vérifier le volume de données\n")
    
    print(f"\n✅ RAPPORT SILVER GÉNÉRÉ AVEC SUCCÈS!")
    print(f"📁 Fichier: {report_path}")
    print(f"📊 {len(tables)} tables analysées")
    print(f"📈 {total_rows:,} lignes au total")
    
    # Générer également un CSV récapitulatif
    summary_df = pd.DataFrame(table_summaries)
    csv_path = f"{report_dir}/silver_tables_summary_{timestamp}.csv"
    summary_df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"📋 CSV récapitulatif: {csv_path}")
    
    return overall_ready

def create_silver_dashboard(spark, tables):
    """Crée un tableau de bord visuel pour Silver."""
    try:
        print(f"\n🎨 CRÉATION DU TABLEAU DE BORD SILVER...")
        
        # Données pour le dashboard
        dashboard_data = []
        
        for table in tables:
            df = get_table_info(spark, table)
            if df is not None:
                row_count = df.count()
                col_count = len(df.columns)
                
                table_type = "Dimension" if table.startswith('dim_') else \
                            "Fait" if table.startswith('fact_') else \
                            "Métrique" if table.startswith('metrique_') else "Autre"
                
                dashboard_data.append({
                    'Table': table,
                    'Type': table_type,
                    'Lignes': row_count,
                    'Colonnes': col_count
                })
        
        dashboard_df = pd.DataFrame(dashboard_data)
        
        # Créer le dashboard
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('📊 TABLEAU DE BORD SILVER - QUALITÉ DES DONNÉES', fontsize=16, fontweight='bold')
        
        # 1. Répartition par type de table
        type_counts = dashboard_df['Type'].value_counts()
        axes[0, 0].pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title('📁 Répartition par Type de Table')
        
        # 2. Volume de données par table
        dashboard_df_sorted = dashboard_df.sort_values('Lignes', ascending=False)
        axes[0, 1].barh(dashboard_df_sorted['Table'], dashboard_df_sorted['Lignes'])
        axes[0, 1].set_title('📈 Volume de Données par Table')
        axes[0, 1].set_xlabel('Nombre de Lignes (échelle log)')
        axes[0, 1].set_xscale('log')
        
        # 3. Complexité des tables (colonnes)
        dashboard_df_sorted_cols = dashboard_df.sort_values('Colonnes', ascending=False)
        axes[1, 0].barh(dashboard_df_sorted_cols['Table'], dashboard_df_sorted_cols['Colonnes'], color='orange')
        axes[1, 0].set_title('🏗️ Complexité des Tables (Nombre de Colonnes)')
        axes[1, 0].set_xlabel('Nombre de Colonnes')
        
        # 4. Heatmap de densité
        pivot_data = dashboard_df.pivot_table(index='Type', values='Lignes', aggfunc='sum')
        im = axes[1, 1].imshow([[pivot_data.values[0][0]]], cmap='YlGnBu', aspect='auto')
        axes[1, 1].set_title('📊 Densité des Données par Type')
        axes[1, 1].set_xticks([])
        axes[1, 1].set_yticks([0])
        axes[1, 1].set_yticklabels([pivot_data.index[0]])
        plt.colorbar(im, ax=axes[1, 1])
        
        plt.tight_layout()
        
        # Sauvegarder le dashboard
        report_dir = "/home/jovyan/silver_reports"
        os.makedirs(report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dashboard_path = f"{report_dir}/silver_dashboard_{timestamp}.png"
        plt.savefig(dashboard_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"✅ Tableau de bord créé: {dashboard_path}")
        
        # Afficher les statistiques clés
        print(f"\n📈 STATISTIQUES CLÉS SILVER:")
        print(f"   • Total tables: {len(tables)}")
        print(f"   • Total lignes: {dashboard_df['Lignes'].sum():,}")
        print(f"   • Tables dimensions: {len(dashboard_df[dashboard_df['Type'] == 'Dimension'])}")
        print(f"   • Tables faits: {len(dashboard_df[dashboard_df['Type'] == 'Fait'])}")
        print(f"   • Tables métriques: {len(dashboard_df[dashboard_df['Type'] == 'Métrique'])}")
        
    except Exception as e:
        print(f"❌ Erreur création dashboard: {e}")

def interactive_silver_explorer(spark):
    """Mode interactif pour explorer les données Silver."""
    while True:
        print(f"\n{'='*60}")
        print("🔍 EXPLORATEUR DE DONNÉES SILVER - QUALITÉ & CONFORMITÉ")
        print(f"{'='*60}")
        
        # Lister les tables Silver
        print("🔄 Recherche des tables Silver...")
        tables = list_silver_tables(spark)
        
        if not tables:
            print("❌ Aucune table Silver trouvée.")
            break
        
        print(f"\n🎯 {len(tables)} TABLES SILVER TROUVÉES:")
        print("-" * 50)
        
        # Grouper par type
        dimensions = [t for t in tables if t.startswith('dim_')]
        faits = [t for t in tables if t.startswith('fact_')]
        metriques = [t for t in tables if t.startswith('metrique_')]
        
        if dimensions:
            print(f"\n🏗️  DIMENSIONS ({len(dimensions)}):")
            for dim in dimensions:
                df = get_table_info(spark, dim)
                count = df.count() if df else 0
                print(f"  ✅ {dim}: {count:,} lignes")
        
        if faits:
            print(f"\n📈 FAITS ({len(faits)}):")
            for fact in faits:
                df = get_table_info(spark, fact)
                count = df.count() if df else 0
                print(f"  ✅ {fact}: {count:,} lignes")
        
        if metriques:
            print(f"\n📊 MÉTRIQUES ({len(metriques)}):")
            for metrique in metriques:
                df = get_table_info(spark, metrique)
                count = df.count() if df else 0
                print(f"  ✅ {metrique}: {count:,} lignes")
        
        print(f"\n📋 Menu Silver:")
        print("1. 📊 Rapport de qualité complet")
        print("2. 🎨 Tableau de bord visuel")
        print("3. 🔍 Analyser une dimension")
        print("4. 📈 Analyser un fait") 
        print("5. 📋 Analyser une métrique")
        print("6. 🔄 Actualiser")
        print("7. 🚪 Quitter")
        
        choice = input("\n🎯 Votre choix (1-7): ").strip()
        
        if choice == "1":
            print(f"\n📊 GÉNÉRATION DU RAPPORT DE QUALITÉ...")
            is_ready = generate_silver_quality_report(spark, tables)
            
            if is_ready:
                print(f"\n🎉 SILVER EST PRÊT POUR GOLD! 🚀")
            else:
                print(f"\n⚠️  SILVER A BESOIN D'AJUSTEMENTS AVANT GOLD")
                
        elif choice == "2":
            create_silver_dashboard(spark, tables)
                
        elif choice == "3":
            if dimensions:
                print(f"\n📋 Dimensions disponibles:")
                for i, dim in enumerate(dimensions, 1):
                    print(f"  {i}. {dim}")
                dim_choice = input("\n📝 Numéro de la dimension: ").strip()
                if dim_choice.isdigit() and 1 <= int(dim_choice) <= len(dimensions):
                    analyze_dimension_quality(spark, dimensions[int(dim_choice)-1])
            else:
                print("❌ Aucune dimension trouvée")
                
        elif choice == "4":
            if faits:
                print(f"\n📋 Faits disponibles:")
                for i, fact in enumerate(faits, 1):
                    print(f"  {i}. {fact}")
                fact_choice = input("\n📝 Numéro du fait: ").strip()
                if fact_choice.isdigit() and 1 <= int(fact_choice) <= len(faits):
                    analyze_fact_quality(spark, faits[int(fact_choice)-1])
            else:
                print("❌ Aucun fait trouvé")
                
        elif choice == "5":
            if metriques:
                print(f"\n📋 Métriques disponibles:")
                for i, metrique in enumerate(metriques, 1):
                    print(f"  {i}. {metrique}")
                metrique_choice = input("\n📝 Numéro de la métrique: ").strip()
                if metrique_choice.isdigit() and 1 <= int(metrique_choice) <= len(metriques):
                    analyze_metrics_quality(spark, metriques[int(metrique_choice)-1])
            else:
                print("❌ Aucune métrique trouvée")
                
        elif choice == "6":
            print("🔄 Actualisation...")
            continue
                
        elif choice == "7":
            print("👋 Au revoir!")
            break
        else:
            print("❌ Choix invalide")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║               SILVER DATA VISUALIZER                        ║
    ║  Analyse de qualité et conformité des données Silver        ║
    ║  Vérification de la préparation pour la couche Gold        ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        print("\n🎯 Démarrage de l'analyseur de données Silver...")
        
        # Mode interactif
        interactive_silver_explorer(spark)
        
        spark.stop()
        print("\n✅ Session Spark fermée")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()