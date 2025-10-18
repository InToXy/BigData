import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col, current_timestamp, lit, concat_ws
from pyspark.sql.utils import AnalysisException

def get_spark_session():
    """Initialise et retourne une session Spark configurée pour notre stack."""
    return SparkSession.builder \
        .appName("Bronze Ingestion Pipeline") \
        .enableHiveSupport() \
        .getOrCreate()

def clean_col_names(df):
    """Nettoie et standardise les noms de colonnes d'un DataFrame."""
    new_cols = [c.strip().replace(' ', '_').replace(';', '_').lower() for c in df.columns]
    return df.toDF(*new_cols)

def process_source(spark, config):
    """
    Fonction générique pour lire une source, la traiter et l'écrire dans la couche Bronze.
    """
    source_type = config["type"]
    source_path = config["path"]
    output_table_name = config["output_table"]
    
    print(f"--- Début du traitement pour : {output_table_name} ---")
    print(f"Configuration: {config}")

    try:
        # 1. Lecture de la source
        if source_type == "csv":
            # Vérification de l'existence du fichier avant la lecture par Spark
            if not source_path.startswith("file:///") or os.path.exists(source_path.replace("file://", "")):
                print(f"Fichier local trouvé: {source_path}")
            else:
                print(f"ERREUR : Fichier local non trouvé à l'emplacement : {source_path}")
                raise FileNotFoundError(source_path)

            reader = spark.read.option("header", True).option("inferSchema", True).option("delimiter", config.get("delimiter", ","))
            if "decimal" in config:
                reader = reader.option("decimal", config["decimal"])
            df = reader.csv(source_path)

        elif source_type == "excel":
            if not source_path.startswith("file:///") or os.path.exists(source_path.replace("file://", "")):
                print(f"Fichier local trouvé: {source_path}")
            else:
                print(f"ERREUR : Fichier local non trouvé à l'emplacement : {source_path}")
                raise FileNotFoundError(source_path)

            reader = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true")
            
            if "sheet_name" in config:
                reader = reader.option("dataAddress", f"'{config['sheet_name']}'!A1")

            df = reader.load(source_path)

        elif source_type == "postgres":
            df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{os.getenv('EXTERNAL_POSTGRES_HOST')}:{os.getenv('EXTERNAL_POSTGRES_PORT')}/{os.getenv('EXTERNAL_POSTGRES_DB')}") \
                .option("dbtable", source_path) \
                .option("user", os.getenv('EXTERNAL_POSTGRES_USER')) \
                .option("password", os.getenv('EXTERNAL_POSTGRES_PASSWORD')) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        else:
            print(f"Type de source non supporté : {source_type}")
            return

        df = clean_col_names(df)
        df = df.withColumn("_ingestion_date", current_timestamp()) \
               .withColumn("_source", lit(config["source_name"]))

        for pii_col in config.get("pii_columns", []):
            if pii_col in df.columns:
                df = df.withColumn(pii_col, sha2(col(pii_col).cast("string"), 256))

        sk_cols = config.get("sk_columns", [])
        if sk_cols:
            if all(c in df.columns for c in sk_cols):
                df = df.withColumn("sk_id", sha2(concat_ws("||", *sk_cols), 256))
            else:
                print(f"Avertissement : Colonnes SK {sk_cols} non trouvées pour {output_table_name}. Clé non créée.")

        bronze_path = f"hdfs://namenode:9000/bronze/{output_table_name}"
        df.write.mode("overwrite").parquet(bronze_path)
        
        print(f"Succès : La table '{output_table_name}' a été écrite dans la couche Bronze.")
        df.printSchema()

    except (AnalysisException, FileNotFoundError) as e:
        print(f"ERREUR : Fichier ou table source non trouvé pour '{output_table_name}' à l'emplacement '{source_path}'.")
        print(e)
    except Exception as e:
        print(f"ERREUR inattendue lors du traitement de '{output_table_name}':")
        print(e)
    
    print(f"--- Fin du traitement pour : {output_table_name} ---")

if __name__ == "__main__":
    spark = get_spark_session()
    
    source_configs = [
        {"type": "csv", "source_name": "activite_professionnel_sante.csv", "path": "file:///data/source/csv/activite_professionnel_sante.csv", "delimiter": ";", "output_table": "activite_professionnel_sante", "pii_columns": ["identifiant"], "sk_columns": ["identifiant", "identifiant_organisation"]},
        {"type": "csv", "source_name": "etablissement_sante.csv", "path": "file:///data/source/csv/etablissement_sante.csv", "delimiter": ";", "output_table": "etablissement_sante", "pii_columns": ["email", "telephone", "telephone_2", "siret_site"], "sk_columns": ["finess_site"]},
        {"type": "csv", "source_name": "professionnel_sante.csv", "path": "file:///data/source/csv/professionnel_sante.csv", "delimiter": ";", "output_table": "professionnel_sante", "pii_columns": ["nom", "prenom"], "sk_columns": ["identifiant"]},
        {"type": "csv", "source_name": "Hospitalisations.csv", "path": "file:///data/source/csv/Hospitalisations.csv", "delimiter": ";", "output_table": "hospitalisations", "pii_columns": ["id_patient"], "sk_columns": ["num_hospitalisation"]},
        {"type": "csv", "source_name": "deces.csv", "path": "file:///data/source/csv/deces.csv", "delimiter": ",", "output_table": "deces", "pii_columns": ["nom", "prenom", "numero_acte_deces"], "sk_columns": ["nom", "prenom", "date_naissance"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_es.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_es.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_participant.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_participant.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_participant", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "DPA_SSR_recueil2014_donnee2013_table_lexique.csv", "path": "file:///data/source/csv/DPA_SSR_recueil2014_donnee2013_table_lexique.csv", "delimiter": ";", "output_table": "satisfaction_2013_dpa_ssr_lexique", "pii_columns": [], "sk_columns": ["NAME"]},
        {"type": "csv", "source_name": "RCP_MCO_recueil2014_donnee2013_table_es.csv", "path": "file:///data/source/csv/RCP_MCO_recueil2014_donnee2013_table_es.csv", "delimiter": ";", "output_table": "satisfaction_2013_rcp_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "RCP_MCO_recueil2014_donnee2013_table_participant.csv", "path": "file:///data/source/csv/RCP_MCO_recueil2014_donnee2013_table_participant.csv", "delimiter": ";", "output_table": "satisfaction_2013_rcp_mco_participant", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "hpp_mco_recueil2015_donnee2014_tables_es.csv", "path": "file:///data/source/csv/hpp_mco_recueil2015_donnee2014_tables_es.csv", "delimiter": ";", "output_table": "satisfaction_2014_hpp_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "idm_mco_recueil2015_donnee2014_tables_es.csv", "path": "file:///data/source/csv/idm_mco_recueil2015_donnee2014_tables_es.csv", "delimiter": ";", "output_table": "satisfaction_2014_idm_mco_es", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dan_mco_recueil2016_donnee2015_donnees.csv", "path": "file:///data/source/csv/dan_mco_recueil2016_donnee2015_donnees.csv", "delimiter": ",", "output_table": "satisfaction_2015_dan_mco", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dpa_had_recueil2016_donnee2015_donnees.csv", "path": "file:///data/source/csv/dpa_had_recueil2016_donnee2015_donnees.csv", "delimiter": ",", "output_table": "satisfaction_2015_dpa_had", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "dpa-ssr-recueil2018-donnee2017-donnees.csv", "path": "file:///data/source/csv/dpa-ssr-recueil2018-donnee2017-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_dpa_ssr", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "ete-ortho-ipaqss-2017-2018-donnees.csv", "path": "file:///data/source/csv/ete-ortho-ipaqss-2017-2018-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_2018_ete_ortho", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "rcp-mco-recueil2018-donnee2017-donnees.csv", "path": "file:///data/source/csv/rcp-mco-recueil2018-donnee2017-donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_rcp_mco", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "ESATIS48H_MCO_recueil2017_donnees.csv", "path": "file:///data/source/csv/ESATIS48H_MCO_recueil2017_donnees.csv", "delimiter": ";", "output_table": "satisfaction_2017_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-esatis48h-mco-open-data-2019.csv", "path": "file:///data/source/csv/resultats-esatis48h-mco-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-esatisca-mco-open-data-2019.csv", "path": "file:///data/source/csv/resultats-esatisca-mco-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_esatisca", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "csv", "source_name": "resultats-iqss-open-data-2019.csv", "path": "file:///data/source/csv/resultats-iqss-open-data-2019.csv", "delimiter": ";", "decimal": ",", "output_table": "satisfaction_2019_iqss", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "dpa_had_recueil2016_donnee2015_donnees.xlsx", "path": "file:///data/source/xlsx/dpa_had_recueil2016_donnee2015_donnees.xlsx", "sheet_name": "Feuil1", "output_table": "satisfaction_2015_dpa_had_excel", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-esatisca-mco-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-esatisca-mco-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_esatisca", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-esatis48h-mco-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-esatis48h-mco-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-iqss-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-iqss-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_iqss", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "postgres", "source_name": "public.patient", "path": "public.patient", "output_table": "patients", "pii_columns": ["nom", "prenom", "adresse", "ville", "email", "tel", "num_secu"], "sk_columns": ["id_patient"]},
        {"type": "postgres", "source_name": "public.consultation", "path": "public.consultation", "output_table": "consultations", "pii_columns": ["id_patient", "id_prof_sante"], "sk_columns": ["num_consultation"]},
        {"type": "postgres", "source_name": "public.diagnostic", "path": "public.diagnostic", "output_table": "diagnostics", "pii_columns": [], "sk_columns": ["code_diag"]},
        {"type": "postgres", "source_name": "public.medicaments", "path": "public.medicaments", "output_table": "medicaments", "pii_columns": [], "sk_columns": ["code_cis"]},
        {"type": "postgres", "source_name": "public.professionnel_de_sante", "path": "public.professionnel_de_sante", "output_table": "professionnels_sante_db", "pii_columns": ["nom", "prenom"], "sk_columns": ["identifiant"]}
    ]

    for config in source_configs:
        process_source(spark, config)

    spark.stop()