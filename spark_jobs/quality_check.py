#!/usr/bin/env python3
"""
Script de vérification de la qualité des données
===============================================
Vérifie les 3 layers pour identifier les problèmes de Superset
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}

def get_spark_session():
    """Session Spark pour la vérification"""
    try:
        # Charger les JARs nécessaires pour S3
        jars_dir = "/home/jovyan/jars"
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
            jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
        else:
            jars_path = ""
        
        builder = SparkSession.builder \
            .appName("Quality Check")
        
        if jars_path:
            builder = builder.config("spark.jars", jars_path)
        
        spark = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise

def check_table_quality(spark, layer, table_name):
    """Vérifie la qualité d'une table"""
    try:
        path = f"s3a://{layer}/{table_name}"
        df = spark.read.parquet(path)
        
        print(f"\n📊 {layer.upper()}/{table_name}")
        print("="*50)
        
        # Statistiques basiques
        count = df.count()
        cols = len(df.columns)
        print(f"   • Lignes: {count:,}")
        print(f"   • Colonnes: {cols}")
        
        # Colonnes avec beaucoup de nulls
        null_rates = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_rate = (null_count / count * 100) if count > 0 else 0
            if null_rate > 10:
                null_rates[col_name] = null_rate
        
        if null_rates:
            print(f"   ⚠️  Colonnes >10% nulls:")
            for col_name, rate in sorted(null_rates.items(), key=lambda x: x[1], reverse=True):
                print(f"      - {col_name}: {rate:.1f}%")
        
        # Vérifier les clés pour Superset
        key_columns = [c for c in df.columns if c.endswith('_id') or c.endswith('_sk') or c == '_id']
        if key_columns:
            print(f"   🔑 Clés disponibles: {key_columns}")
        else:
            print(f"   ❌ PROBLÈME: Aucune clé numérique pour Superset!")
        
        # Vérifier les colonnes d'établissement
        if 'etablissement' in table_name.lower() or 'consultation' in table_name.lower():
            etab_cols = [c for c in df.columns if 'etablissement' in c.lower() or 'finess' in c.lower()]
            if etab_cols:
                print(f"   🏥 Colonnes établissement: {etab_cols}")
                # Vérifier s'il y a des noms d'établissement
                nom_cols = [c for c in etab_cols if 'nom' in c.lower() or 'raison' in c.lower()]
                if nom_cols:
                    non_null_noms = df.filter(col(nom_cols[0]).isNotNull()).count()
                    print(f"   📛 Noms établissement non-nulls: {non_null_noms:,}/{count:,} ({non_null_noms/count*100:.1f}%)")
                else:
                    print(f"   ❌ PROBLÈME: Pas de noms d'établissement!")
            else:
                print(f"   ❌ PROBLÈME: Pas de lien établissement!")
        
        return True
        
    except Exception as e:
        print(f"   ❌ ERREUR: {e}")
        return False

def main():
    """Vérification complète"""
    print("""
╔════════════════════════════════════════════════════════════════════════╗
║                    VÉRIFICATION QUALITÉ DONNÉES                       ║
║                   Diagnostic pour Superset                            ║
╚════════════════════════════════════════════════════════════════════════╝
    """)
    
    spark = get_spark_session()
    
    # Tables à vérifier
    tables_to_check = {
        "bronze": [
            "patient", "consultation", "etablissement_sante", 
            "professionnel_de_sante", "diagnostic"
        ],
        "silver": [
            "patient", "consultation", "etablissement_sante", 
            "professionnel_de_sante", "diagnostic"
        ],
        "gold": [
            "dim_patient", "dim_etablissement", "dim_diagnostic",
            "fact_consultation", "mart_performance_etablissement"
        ]
    }
    
    # Vérifications par layer
    for layer, tables in tables_to_check.items():
        print(f"\n🔍 LAYER {layer.upper()}")
        print("="*70)
        
        for table in tables:
            check_table_quality(spark, layer, table)
    
    print(f"""
╔════════════════════════════════════════════════════════════════════════╗
║                           RECOMMANDATIONS                             ║
╚════════════════════════════════════════════════════════════════════════╝

🎯 POUR SUPERSET:
   1. Utiliser les colonnes _id (numériques) comme clés primaires
   2. Joindre les tables via les _sk_* (clés étrangères)
   3. Éviter les colonnes avec >50% de nulls pour les visualisations
   4. Privilégier les tables Gold (mart_*) pour les dashboards

🔧 CORRECTIONS PRIORITAIRES:
   1. Ajouter des noms d'établissement dans les consultations
   2. Créer des vues Superset avec jointures pré-calculées
   3. Utiliser les Data Marts pour les analyses complexes

📊 TABLES RECOMMANDÉES POUR SUPERSET:
   • mart_performance_etablissement: KPIs établissements
   • mart_diagnostic_epidemio: Analyses épidémiologiques  
   • mart_demographie: Analyses démographiques
   • fact_consultation: Données de base consultations
    """)
    
    spark.stop()

if __name__ == "__main__":
    main()