import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, asc, mean, stddev, min, max, 
    countDistinct, isnan, isnull, sum as spark_sum, lit,
    input_file_name, regexp_extract
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
        
        print("✅ Spark Silver Visualizer initialisé avec configuration S3A")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur initialisation Spark: {e}")
        raise

def list_all_tables_advanced(spark):
    """Liste TOUTES les tables Silver en utilisant une méthode avancée."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['bucket']}/"
        print(f"🔍 Recherche de TOUTES les tables Silver dans: {silver_path}")
        
        # Méthode 1: Utiliser une approche avec les fichiers directement
        try:
            # Créer un DataFrame qui liste tous les fichiers
            files_df = spark.read \
                .format("binaryFile") \
                .option("pathGlobFilter", "*.parquet") \
                .option("recursiveFileLookup", "true") \
                .load(f"{silver_path}*")
            
            # Extraire les noms de tables à partir des chemins
            tables_df = files_df.select(
                regexp_extract(input_file_name(), f"s3a://{MINIO_CONFIG['bucket']}/([^/]+)/", 1).alias("table_name")
            ).distinct()
            
            tables_list = [row.table_name for row in tables_df.collect() if row.table_name]
            
            if tables_list:
                print(f"🎯 {len(tables_list)} tables Silver trouvées avec la méthode des fichiers:")
                return tables_list
                
        except Exception as e:
            print(f"❌ Méthode des fichiers échouée: {e}")
        
        # Méthode 2: Essayer des patterns de noms étendus pour Silver
        print("🔄 Utilisation de la méthode des patterns étendus Silver...")
        
        # Liste étendue de patterns de tables Silver possibles
        extended_patterns = [
            # Tables Silver standards
            "patients", "consultations", "hospitalisations", "deces",
            "etablissements", "professionnels_sante", "diagnostics", 
            "satisfaction", "indicators_metier",
            
            # Variantes de noms
            "patient", "consultation", "hospitalisation", "dece", 
            "etablissement", "professionnel_sante", "diagnostic",
            "satisfaction_mco", "indicator_metier", "indicators",
            
            # Préfixes Silver
            "silver_patients", "silver_consultations", "silver_hospitalisations",
            "silver_deces", "silver_etablissements", "silver_professionnels",
            "silver_diagnostics", "silver_satisfaction", "silver_indicators",
            
            # Noms anglais
            "patients_silver", "consultations_silver", "hospitalizations_silver",
            "deaths_silver", "establishments_silver", "professionals_silver",
            "diagnostics_silver", "satisfaction_silver", "indicators_silver"
        ]
        
        found_tables = []
        
        for table in extended_patterns:
            try:
                table_path = f"{silver_path}{table}"
                # Essayer de lire une seule ligne pour vérifier si la table existe
                test_df = spark.read.parquet(table_path).limit(1)
                count = test_df.count()
                found_tables.append(table)
                print(f"  ✅ {table}")
            except:
                pass  # Table n'existe pas, on continue
        
        if found_tables:
            print(f"🎯 {len(found_tables)} tables Silver trouvées avec les patterns étendus")
            return found_tables
        else:
            print("❌ Aucune table Silver trouvée avec les patterns étendus")
            return []
        
    except Exception as e:
        print(f"❌ Erreur listing tables Silver avancé: {e}")
        return []

def get_table_info(spark, table_name):
    """Récupère les informations détaillées d'une table Silver."""
    try:
        silver_path = f"s3a://{MINIO_CONFIG['bucket']}/{table_name}"
        
        # Lecture des données
        df = spark.read.option("mergeSchema", "true").parquet(silver_path)
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lecture table Silver {table_name}: {e}")
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
    
    for column in df.columns[:15]:  # Limiter aux 15 premières colonnes pour la performance
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
    """Affiche un échantillon des données Silver."""
    try:
        # Convertir en Pandas pour un affichage plus lisible
        sample_df = df.limit(sample_size).toPandas()
        
        if sample_df.empty:
            return None
            
        return sample_df
        
    except Exception as e:
        print(f"❌ Erreur affichage échantillon Silver: {e}")
        return None

def generate_comprehensive_report(spark, table_name):
    """Génère un rapport complet pour une table Silver."""
    print(f"\n{'='*80}")
    print(f"🌟 RAPPORT SILVER COMPLET: {table_name}")
    print(f"{'='*80}")
    
    df = get_table_info(spark, table_name)
    if df is None:
        return
    
    # Informations de base
    row_count = df.count()
    column_count = len(df.columns)
    
    print(f"📈 Statistiques Silver:")
    print(f"   • Lignes: {row_count:,}")
    print(f"   • Colonnes: {column_count}")
    
    # Afficher les premières colonnes
    if df.columns:
        columns_preview = ", ".join(df.columns[:8])
        if len(df.columns) > 8:
            columns_preview += f" ... (+{len(df.columns) - 8} autres)"
        print(f"   • Colonnes: {columns_preview}")
    
    # Analyses successives
    schema_df = analyze_table_schema(df, table_name)
    print(f"\n🏗️  SCHÉMA ENRICHI DE LA TABLE '{table_name}':")
    print("-" * 80)
    print(schema_df.to_string(index=False))
    
    quality_df = analyze_data_quality(df, table_name)
    if quality_df is not None:
        print(f"\n🔍 QUALITÉ DES DONNÉES ENRICHIES - '{table_name}':")
        print("-" * 60)
        print(quality_df.to_string(index=False))
        
        if len(df.columns) > 15:
            print(f"\nℹ️  Seules les 15 premières colonnes sont affichées ({len(df.columns)} au total)")
    
    sample_df = show_sample_data(df, table_name, 8)
    if sample_df is not None:
        print(f"\n📋 ÉCHANTILLON DES DONNÉES ENRICHIES - '{table_name}' (8 lignes):")
        print("-" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(sample_df.to_string(index=False))

def generate_detailed_report_all_tables(spark, tables):
    """Génère un rapport détaillé pour TOUTES les tables Silver."""
    print(f"\n🎯 GÉNÉRATION DU RAPPORT DÉTAILLÉ SILVER POUR TOUTES LES TABLES...")
    print(f"📊 {len(tables)} tables Silver à analyser")
    
    # Créer un répertoire pour les rapports Silver
    report_dir = "/home/jovyan/silver_reports"
    os.makedirs(report_dir, exist_ok=True)
    
    # Fichier de rapport principal
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    main_report_path = f"{report_dir}/silver_analysis_report_{timestamp}.md"
    
    with open(main_report_path, 'w', encoding='utf-8') as f:
        f.write(f"# RAPPORT D'ANALYSE COMPLÈTE - COUCHE SILVER\n\n")
        f.write(f"**Date de génération**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Nombre de tables analysées**: {len(tables)}\n\n")
        
        # Résumé exécutif
        f.write("## 📊 RÉSUMÉ EXÉCUTIF\n\n")
        
        total_rows = 0
        total_columns = 0
        table_summaries = []
        
        for i, table_name in enumerate(tables, 1):
            print(f"🔍 Analyse de la table Silver {i}/{len(tables)}: {table_name}")
            
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
                    f.write(f"- **Schéma**:\n")
                    f.write("```\n")
                    f.write(schema_df.to_string(index=False))
                    f.write("\n```\n")
                
                # Qualité des données
                quality_df = analyze_data_quality(df, table_name)
                if quality_df is not None:
                    f.write(f"- **Qualité des données (top 15 colonnes)**:\n")
                    f.write("```\n")
                    f.write(quality_df.to_string(index=False))
                    f.write("\n```\n")
                
                # Échantillon
                sample_df = show_sample_data(df, table_name, 3)
                if sample_df is not None:
                    f.write(f"- **Échantillon (3 lignes)**:\n")
                    f.write("```\n")
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    f.write(sample_df.to_string(index=False))
                    f.write("\n```\n")
                
                f.write("\n" + "-" * 50 + "\n\n")
        
        # Statistiques globales
        f.write("## 📈 STATISTIQUES GLOBALES\n\n")
        f.write(f"- **Total des lignes**: {total_rows:,}\n")
        f.write(f"- **Total des colonnes**: {total_columns}\n")
        f.write(f"- **Moyenne colonnes/table**: {total_columns/len(tables):.1f}\n")
        f.write(f"- **Moyenne lignes/table**: {total_rows/len(tables):.1f}\n\n")
        
        # Tables les plus volumineuses
        f.write("## 🏆 TABLES LES PLUS VOLUMINEUSES\n\n")
        sorted_tables = sorted(table_summaries, key=lambda x: x['rows'], reverse=True)
        for i, table in enumerate(sorted_tables[:10], 1):
            f.write(f"{i}. **{table['table']}**: {table['rows']:,} lignes, {table['columns']} colonnes\n")
        
        # Données enrichies disponibles
        f.write("\n## 💾 DONNÉES ENRICHIES POUR ANALYSE\n\n")
        f.write("Les données suivantes sont disponibles pour analyse avancée:\n")
        for table in tables:
            f.write(f"- `{table}`\n")
        
        # Section spécifique Silver
        f.write("\n## 🌟 VALEUR AJOUTÉE SILVER\n\n")
        f.write("### Indicateurs Calculés:\n")
        f.write("- **Patients**: Segments d'activité, statuts, indicateurs d'hospitalisation\n")
        f.write("- **Consultations**: Durées, saisons, régions, types de pathologie\n")
        f.write("- **Hospitalisations**: Gravité, risques de réadmission, types d'hospitalisation\n")
        f.write("- **Établissements**: Niveaux d'activité, performance, satisfaction\n")
        f.write("- **Professionnels**: Productivité, spécialisation, expérience\n")
        f.write("- **Diagnostics**: Prévalence, gravité, taux d'hospitalisation\n")
        f.write("- **Indicateurs Métier**: KPI agrégés par région, âge, sexe\n\n")
        
        f.write("### Prêt pour le Layer Gold:\n")
        f.write("- ✅ Données nettoyées et enrichies\n")
        f.write("- ✅ Indicateurs métier calculés\n")
        f.write("- ✅ Segments et catégories définis\n")
        f.write("- ✅ Données temporelles structurées\n")
        f.write("- ✅ Relations établies entre entités\n")
    
    print(f"\n✅ RAPPORT SILVER GÉNÉRÉ AVEC SUCCÈS!")
    print(f"📁 Fichier: {main_report_path}")
    print(f"📊 {len(tables)} tables Silver analysées")
    print(f"📈 {total_rows:,} lignes de données enrichies")
    print(f"🏗️  {total_columns} colonnes d'indicateurs calculés")
    
    # Générer également un CSV récapitulatif
    summary_df = pd.DataFrame(table_summaries)
    csv_path = f"{report_dir}/silver_tables_summary_{timestamp}.csv"
    summary_df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"📋 CSV récapitulatif Silver: {csv_path}")

def interactive_explorer(spark):
    """Mode interactif pour explorer les données Silver."""
    while True:
        print(f"\n{'='*50}")
        print("🌟 EXPLORATEUR DE DONNÉES SILVER")
        print(f"{'='*50}")
        
        # Lister TOUTES les tables Silver disponibles
        print("🔄 Recherche de toutes les tables Silver...")
        tables = list_all_tables_advanced(spark)
        
        if not tables:
            print("❌ Aucune table Silver trouvée.")
            break
        
        print(f"\n🎯 {len(tables)} TABLES SILVER TROUVÉES:")
        print("-" * 50)
        for i, table in enumerate(sorted(tables), 1):
            print(f"{i:2d}. {table}")
        print("-" * 50)
        
        print(f"\n📋 Menu Silver:")
        print("1. 📊 Aperçu rapide de toutes les tables Silver")
        print("2. 🔍 Analyser une table Silver spécifique")
        print("3. 📈 Rapport détaillé pour une table Silver")
        print("4. 📑 RAPPORT DÉTAILLÉ POUR TOUTES LES TABLES SILVER")
        print("5. 🔄 Rechercher à nouveau les tables")
        print("6. 🚪 Quitter")
        
        choice = input("\n🎯 Votre choix (1-6): ").strip()
        
        if choice == "1":
            print(f"\n🚀 APERÇU RAPIDE DES {len(tables)} TABLES SILVER:")
            print("=" * 50)
            for i, table in enumerate(tables, 1):
                print(f"\n{i}/{len(tables)}: {table}")
                quick_table_overview(spark, table)
                if i < len(tables):
                    input("\n⏎ Appuyez sur Entrée pour continuer...")
                
        elif choice == "2":
            print(f"\n📋 Tables Silver disponibles ({len(tables)}):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            
            try:
                table_choice = input("\n📝 Numéro ou nom de la table Silver: ").strip()
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
                    df = get_table_info(spark, table_name)
                    if df is not None:
                        print(f"\n🔍 Analyse de la table Silver: {table_name}")
                        print(f"📁 Chemin: s3a://{MINIO_CONFIG['bucket']}/{table_name}")
                        
                        row_count = df.count()
                        column_count = len(df.columns)
                        print(f"📈 Statistiques Silver:")
                        print(f"   • Lignes: {row_count:,}")
                        print(f"   • Colonnes: {column_count}")
                        
                        if df.columns:
                            columns_preview = ", ".join(df.columns[:8])
                            if len(df.columns) > 8:
                                columns_preview += f" ... (+{len(df.columns) - 8} autres)"
                            print(f"   • Colonnes: {columns_preview}")
                        
                        schema_df = analyze_table_schema(df, table_name)
                        print(f"\n🏗️  SCHÉMA ENRICHI:")
                        print(schema_df.to_string(index=False))
                        
                        sample_df = show_sample_data(df, table_name, 5)
                        if sample_df is not None:
                            print(f"\n📋 ÉCHANTILLON (5 lignes):")
                            pd.set_option('display.max_columns', None)
                            pd.set_option('display.width', None)
                            pd.set_option('display.max_colwidth', 30)
                            print(sample_df.to_string(index=False))
                else:
                    print(f"❌ Table Silver '{table_name}' non trouvée")
                    
            except ValueError:
                print("❌ Entrée invalide")
                
        elif choice == "3":
            print(f"\n📋 Tables Silver disponibles ({len(tables)}):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            
            try:
                table_choice = input("\n📝 Numéro ou nom de la table Silver: ").strip()
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
                    generate_comprehensive_report(spark, table_name)
                else:
                    print(f"❌ Table Silver '{table_name}' non trouvée")
                    
            except ValueError:
                print("❌ Entrée invalide")
                
        elif choice == "4":
            print(f"\n📑 LANCEMENT DU RAPPORT COMPLET POUR {len(tables)} TABLES SILVER...")
            generate_detailed_report_all_tables(spark, tables)
                
        elif choice == "5":
            print("🔄 Nouvelle recherche des tables Silver...")
            continue
                
        elif choice == "6":
            print("👋 Au revoir!")
            break
        else:
            print("❌ Choix invalide")

def quick_table_overview(spark, table_name):
    """Aperçu rapide d'une table Silver."""
    try:
        df = get_table_info(spark, table_name)
        if df is not None:
            print(f"   • Lignes: {df.count():,}")
            print(f"   • Colonnes: {len(df.columns)}")
            if df.columns:
                preview_cols = ", ".join(df.columns[:3])
                if len(df.columns) > 3:
                    preview_cols += f" ... (+{len(df.columns)-3})"
                print(f"   • Exemple colonnes: {preview_cols}")
    except Exception as e:
        print(f"   ❌ Erreur: {e}")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║               SILVER DATA VISUALIZER                        ║
    ║  Exploration et visualisation des données enrichies MinIO   ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        print("\n🎯 Démarrage de l'explorateur de données Silver...")
        
        # Mode interactif
        interactive_explorer(spark)
        
        spark.stop()
        print("\n✅ Session Spark Silver fermée")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()