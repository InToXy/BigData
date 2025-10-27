#!/usr/bin/env python3
"""
Script de test pour vérifier l'accès aux fichiers CSV depuis Spark
"""
import os
from pyspark.sql import SparkSession

def test_csv_access():
    """Test l'accès aux fichiers CSV"""
    
    # Initialiser Spark
    jars_dir = "/home/jovyan/jars"
    jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
    jars_path = ",".join([f"{jars_dir}/{jar}" for jar in jar_files])
    
    spark = SparkSession.builder \
        .appName("Test CSV Access") \
        .config("spark.jars", jars_path) \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark initialisé\n")
    
    # Liste des fichiers CSV à tester
    test_files = [
        "file:///data/source/csv/etablissement_sante.csv",
        "file:///data/source/csv/professionnel_sante.csv",
        "file:///data/source/csv/Hospitalisations.csv"
    ]
    
    for csv_file in test_files:
        filename = os.path.basename(csv_file)
        print(f"🔍 Test: {filename}")
        
        try:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", ";") \
                .option("encoding", "UTF-8") \
                .csv(csv_file)
            
            count = df.count()
            cols = len(df.columns)
            
            print(f"   ✅ Succès: {count} lignes, {cols} colonnes")
            df.show(3, truncate=False)
            print()
            
        except Exception as e:
            print(f"   ❌ Erreur: {str(e)}\n")
    
    spark.stop()
    print("✅ Test terminé")

if __name__ == "__main__":
    test_csv_access()
