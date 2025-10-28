#!/usr/bin/env python3
"""
Pipeline Bronze - Ingestion complète de toutes les sources vers MinIO
Filtre décès 2019 uniquement
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, year, monotonically_increasing_id, regexp_extract
)
import uuid

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "bronze"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder \
        .appName("Bronze Complete Ingestion") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def add_technical_columns(df, source_name, batch_id):
    """Ajoute les colonnes techniques Bronze."""
    df = df.withColumn("_source_system", lit("CSV"))
    df = df.withColumn("_source_table", lit(source_name))
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_batch_id", lit(batch_id))
    df = df.withColumn("_version", lit(1))
    df = df.withColumn("_is_current", lit(True))
    df = df.withColumn("_is_deleted", lit(False))
    df = df.withColumn("_sk", monotonically_increasing_id())
    return df

def process_csv_generic(spark, path, output_name, source_name, batch_id, encoding="utf-8", delimiter=";", pii_columns=None, year_filter=None):
    """Traite un fichier CSV générique."""
    print(f"\n🎯 TRAITEMENT: {output_name}")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", delimiter) \
            .option("encoding", encoding) \
            .option("inferSchema", "true") \
            .csv(path)
        
        count_initial = df.count()
        print(f"   📊 Lignes initiales: {count_initial}")
        
        # Filtrage par année si spécifié
        if year_filter:
            # Chercher spécifiquement 'date_deces' ou similaire
            date_cols = [c for c in df.columns if "deces" in c.lower() or "death" in c.lower()]
            if not date_cols:
                date_cols = [c for c in df.columns if "date" in c.lower()]
            if date_cols:
                date_col = date_cols[0]
                print(f"   🔍 Filtrage sur colonne: {date_col}")
                # Convertir en date avec gestion d'erreurs
                from pyspark.sql.functions import to_date
                df = df.withColumn(f"{date_col}_parsed", to_date(col(date_col), "yyyy-MM-dd"))
                df = df.filter(year(col(f"{date_col}_parsed")) == year_filter)
                count_filtered = df.count()
                print(f"   ✅ Lignes {year_filter}: {count_filtered}")
        
        # Anonymisation PII
        if pii_columns:
            for pii_col in pii_columns:
                if pii_col in df.columns:
                    df = df.withColumn(pii_col, sha2(col(pii_col), 256))
        
        df = add_technical_columns(df, source_name, batch_id)
        
        output_path = f"s3a://{BUCKET}/{output_name}/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Écrit dans {output_path}")
        return True
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════╗
    ║    PIPELINE BRONZE - TOUTES SOURCES       ║
    ║        DÉCÈS 2019 UNIQUEMENT              ║
    ╚═══════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        batch_id = str(uuid.uuid4())
        print(f"📦 Batch ID: {batch_id}")
        
        # Test MinIO
        print("\n🔍 Test MinIO...")
        test_df = spark.createDataFrame([(1, "test")], ["id", "data"])
        test_df.write.mode("overwrite").parquet(f"s3a://{BUCKET}/test/")
        print("✅ MinIO OK")
        
        results = []
        
        # ===== DONNÉES PRINCIPALES =====
        
        # Décès 2019
        results.append(("deces", process_csv_generic(
            spark, 
            "file:///data/source/DECES EN FRANCE/deces.csv",
            "deces",
            "deces",
            batch_id,
            encoding="utf-8",
            pii_columns=["nom", "prenom", "adresse", "ville"],
            year_filter=2019
        )))
        
        # Établissements
        results.append(("etablissements", process_csv_generic(
            spark,
            "file:///data/source/Etablissement de SANTE/etablissement_sante.csv",
            "etablissements",
            "etablissements",
            batch_id,
            encoding="utf-8",
            pii_columns=["email", "telephone", "telephone_2", "adresse"]
        )))
        
        # Professionnels de santé
        results.append(("professionnels_sante", process_csv_generic(
            spark,
            "file:///data/source/Etablissement de SANTE/professionnel_sante.csv",
            "professionnels_sante",
            "professionnels_sante",
            batch_id,
            encoding="utf-8",
            pii_columns=["Nom", "Prenom"]
        )))
        
        # Activité professionnels
        results.append(("activite_professionnels", process_csv_generic(
            spark,
            "file:///data/source/Etablissement de SANTE/activite_professionnel_sante.csv",
            "activite_professionnels",
            "activite_professionnels",
            batch_id,
            encoding="utf-8"
        )))
        
        # Hospitalisations
        results.append(("hospitalisations", process_csv_generic(
            spark,
            "file:///data/source/Hospitalisation/Hospitalisations.csv",
            "hospitalisations",
            "hospitalisations",
            batch_id,
            encoding="ISO-8859-1"
        )))
        
        # ===== SATISFACTION PAR ANNÉE =====
        
        # 2014 - DPA SSR
        results.append(("satisfaction_2014_dpa_ssr_es", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2014/DPA_SSR_recueil2014_donnee2013_table_es.csv",
            "satisfaction_2014_dpa_ssr_es",
            "satisfaction_2014",
            batch_id
        )))
        
        results.append(("satisfaction_2014_dpa_ssr_participant", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2014/DPA_SSR_recueil2014_donnee2013_table_participant.csv",
            "satisfaction_2014_dpa_ssr_participant",
            "satisfaction_2014",
            batch_id
        )))
        
        # 2014 - RCP MCO
        results.append(("satisfaction_2014_rcp_mco_es", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2014/RCP_MCO_recueil2014_donnee2013_table_es.csv",
            "satisfaction_2014_rcp_mco_es",
            "satisfaction_2014",
            batch_id
        )))
        
        results.append(("satisfaction_2014_rcp_mco_participant", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2014/RCP_MCO_recueil2014_donnee2013_table_participant.csv",
            "satisfaction_2014_rcp_mco_participant",
            "satisfaction_2014",
            batch_id
        )))
        
        # 2015
        results.append(("satisfaction_2015_hpp_mco", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2015/hpp_mco_recueil2015_donnee2014_tables_es.csv",
            "satisfaction_2015_hpp_mco",
            "satisfaction_2015",
            batch_id
        )))
        
        results.append(("satisfaction_2015_idm_mco", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2015/idm_mco_recueil2015_donnee2014_tables_es.csv",
            "satisfaction_2015_idm_mco",
            "satisfaction_2015",
            batch_id
        )))
        
        # 2016
        results.append(("satisfaction_2016_dan_mco", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2016/dan_mco_recueil2016_donnee2015_donnees.csv",
            "satisfaction_2016_dan_mco",
            "satisfaction_2016",
            batch_id
        )))
        
        results.append(("satisfaction_2016_dpa_had", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2016/dpa_had_recueil2016_donnee2015_donnees.csv",
            "satisfaction_2016_dpa_had",
            "satisfaction_2016",
            batch_id
        )))
        
        # 2017
        results.append(("satisfaction_2017_esatis48h", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv",
            "satisfaction_2017_esatis48h",
            "satisfaction_2017",
            batch_id
        )))
        
        # 2017-2018
        results.append(("satisfaction_2017_2018_dpa_ssr", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2017-2018/dpa-ssr-recueil2018-donnee2017-donnees.csv",
            "satisfaction_2017_2018_dpa_ssr",
            "satisfaction_2017_2018",
            batch_id
        )))
        
        results.append(("satisfaction_2017_2018_ete_ortho", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2017-2018/ete-ortho-ipaqss-2017-2018-donnees.csv",
            "satisfaction_2017_2018_ete_ortho",
            "satisfaction_2017_2018",
            batch_id
        )))
        
        results.append(("satisfaction_2017_2018_rcp_mco", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2017-2018/rcp-mco-recueil2018-donnee2017-donnees.csv",
            "satisfaction_2017_2018_rcp_mco",
            "satisfaction_2017_2018",
            batch_id
        )))
        
        # 2019
        results.append(("satisfaction_2019_esatis48h", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2019/resultats-esatis48h-mco-open-data-2019.csv",
            "satisfaction_2019_esatis48h",
            "satisfaction_2019",
            batch_id
        )))
        
        results.append(("satisfaction_2019_esatisca", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2019/resultats-esatisca-mco-open-data-2019.csv",
            "satisfaction_2019_esatisca",
            "satisfaction_2019",
            batch_id
        )))
        
        results.append(("satisfaction_2019_iqss", process_csv_generic(
            spark,
            "file:///data/source/Satisfaction/2019/resultats-iqss-open-data-2019.csv",
            "satisfaction_2019_iqss",
            "satisfaction_2019",
            batch_id
        )))
        
        # Résumé
        print("\n" + "="*60)
        print("🎉 PIPELINE BRONZE TERMINÉ")
        print("="*60)
        
        success = [r[0] for r in results if r[1]]
        failed = [r[0] for r in results if not r[1]]
        
        print(f"\n✅ Succès: {len(success)} tables")
        for table in success:
            print(f"   ✅ {table}")
        
        if failed:
            print(f"\n❌ Échecs: {len(failed)} tables")
            for table in failed:
                print(f"   ❌ {table}")
        
        spark.stop()
        sys.exit(0 if not failed else 1)
        
    except Exception as e:
        print(f"\n💥 ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
