import os
import sys
import uuid as uuid_lib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, current_timestamp, lit, concat_ws, 
    trim, upper, lower, regexp_replace, when, 
    coalesce, monotonically_increasing_id, row_number,
    length, count as count_agg, md5, udf, sum as spark_sum
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window
import re

def get_spark_session():
    """Initialise et retourne une session Spark configurée pour MinIO."""
    try:
        spark = SparkSession.builder \
            .appName("Bronze Ingestion Pipeline - MinIO") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        # Configuration Hadoop pour MinIO
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        print("✅ Session Spark initialisée avec succès")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation de Spark: {e}")
        print("🔄 Utilisation de la configuration Spark par défaut...")
        # Fallback - session Spark sans configuration MinIO
        return SparkSession.builder \
            .appName("Bronze Ingestion Pipeline - Fallback") \
            .getOrCreate()

def clean_col_names(df):
    """Nettoie et standardise les noms de colonnes d'un DataFrame."""
    def clean_name(name):
        # Convertir en minuscules
        name = name.lower()
        # Remplacer les caractères spéciaux et espaces par _
        name = re.sub(r'[^a-z0-9]', '_', name)
        # Remplacer les multiples _ par un seul
        name = re.sub(r'_+', '_', name)
        # Supprimer les _ au début et à la fin
        name = name.strip('_')
        return name

    new_cols = [clean_name(c) for c in df.columns]
    return df.toDF(*new_cols)

def remove_duplicates(df, sk_columns):
    """
    Supprime les doublons en se basant sur les colonnes clés.
    Garde la ligne la plus récente en cas de doublon.
    """
    if not sk_columns or not all(c in df.columns for c in sk_columns):
        print(f"⚠️ Avertissement : Colonnes SK {sk_columns} non disponibles, dédoublonnage sur toutes les colonnes")
        return df.dropDuplicates()
    
    # Créer une fenêtre de partition pour garder la dernière occurrence
    window_spec = Window.partitionBy(*sk_columns).orderBy(col("_ingestion_date").desc())
    
    df_with_row_num = df.withColumn("_row_num", row_number().over(window_spec))
    df_deduplicated = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
    
    return df_deduplicated

def clean_data(df):
    """
    Nettoie les données :
    - Supprime les espaces en début/fin
    - Remplace les valeurs vides par NULL
    - Normalise les champs texte
    """
    for column in df.columns:
        if column.startswith("_"):  # Ne pas toucher aux colonnes techniques
            continue
            
        col_type = df.schema[column].dataType
        
        # Nettoyage des colonnes string
        if isinstance(col_type, StringType):
            df = df.withColumn(
                column,
                when(
                    (trim(col(column)) == "") | 
                    (trim(col(column)).isNull()) |
                    (upper(trim(col(column))).isin("NULL", "NA", "N/A", "NONE", "UNKNOWN", "-")),
                    lit(None)
                ).otherwise(trim(col(column)))
            )
    
    return df

def remove_empty_rows(df, threshold=0.5):
    """
    Supprime les lignes avec trop de valeurs nulles.
    threshold : pourcentage minimum de colonnes non-nulles requises (0.5 = 50%)
    """
    total_cols = len([c for c in df.columns if not c.startswith("_")])
    min_non_null = int(total_cols * threshold)
    
    # Compter les colonnes non-nulles par ligne
    non_null_cols = [
        when(col(c).isNotNull(), 1).otherwise(0) 
        for c in df.columns if not c.startswith("_")
    ]
    
    df = df.withColumn("_non_null_count", spark_sum(*non_null_cols))
    df = df.filter(col("_non_null_count") >= min_non_null)
    df = df.drop("_non_null_count")
    
    return df

def normalize_data(df):
    """
    Normalise les données selon le type :
    - Emails en minuscules
    - Codes postaux formatés
    - Téléphones formatés
    - Textes normalisés
    """
    for column in df.columns:
        if column.startswith("_"):
            continue
        
        col_lower = column.lower()
        
        # Normalisation des emails
        if "email" in col_lower or "mail" in col_lower:
            df = df.withColumn(column, lower(col(column)))
        
        # Normalisation des téléphones (suppression des espaces et caractères spéciaux)
        elif "tel" in col_lower or "phone" in col_lower or "telephone" in col_lower:
            df = df.withColumn(
                column,
                regexp_replace(col(column), r"[^0-9+]", "")
            )
        
        # Normalisation des codes postaux
        elif "code_postal" in col_lower or "cp" in col_lower or "postal" in col_lower:
            df = df.withColumn(
                column,
                regexp_replace(col(column), r"[^0-9]", "")
            )
        
        # Normalisation des noms propres (première lettre majuscule)
        elif "nom" in col_lower or "prenom" in col_lower or "ville" in col_lower:
            # Cette colonne sera hashée après, mais on la normalise d'abord
            df = df.withColumn(
                column,
                when(col(column).isNotNull(), 
                     regexp_replace(upper(col(column)), r"\s+", " "))
                .otherwise(None)
            )
    
    return df

def anonymize_pii(df, pii_columns):
    """
    Anonymise les données sensibles (PII) en utilisant SHA-256.
    """
    for pii_col in pii_columns:
        if pii_col in df.columns:
            df = df.withColumn(
                pii_col,
                when(col(pii_col).isNotNull(),
                     sha2(col(pii_col).cast("string"), 256))
                .otherwise(None)
            )
            print(f"  🔒 Colonne '{pii_col}' anonymisée")
    
    return df

def add_surrogate_key(df, sk_columns, output_table_name):
    """
    Ajoute une clé de substitution (SK) basée sur les colonnes métier.
    """
    if not sk_columns:
        print(f"  ⚠️ Aucune colonne SK définie pour {output_table_name}")
        return df
    
    # Vérifier que toutes les colonnes SK existent
    available_sk_cols = [c for c in sk_columns if c in df.columns]
    
    if not available_sk_cols:
        print(f"  ⚠️ Aucune colonne SK disponible pour {output_table_name}")
        return df
    
    # Créer la clé de substitution
    df = df.withColumn(
        "sk_id",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in available_sk_cols]), 256)
    )
    
    print(f"  🔑 Clé SK créée à partir de : {', '.join(available_sk_cols)}")
    
    return df

def add_technical_columns(df, source_name, output_table_name):
    """
    Ajoute les colonnes techniques à la table :
    - _ingestion_date : Date d'ingestion
    - _source : Source d'origine
    - _table_name : Nom de la table de destination
    - _record_uuid : UUID unique par enregistrement
    - _hash_record : Hash de l'enregistrement complet
    - _processing_timestamp : Timestamp de traitement
    """
    # UDF pour générer des UUID
    generate_uuid_udf = udf(lambda: str(uuid_lib.uuid4()), StringType())
    
    # Colonnes techniques
    df = df.withColumn("_ingestion_date", current_timestamp())
    df = df.withColumn("_source", lit(source_name))
    df = df.withColumn("_table_name", lit(output_table_name))
    df = df.withColumn("_record_uuid", generate_uuid_udf())
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    # Créer un hash de l'enregistrement complet (pour détecter les changements)
    non_tech_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "_hash_record",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in non_tech_cols]), 256)
    )
    
    return df

def get_data_quality_stats(df, output_table_name):
    """
    Calcule et affiche des statistiques de qualité des données.
    """
    total_rows = df.count()
    total_cols = len([c for c in df.columns if not c.startswith("_")])
    
    print(f"\n  📊 Statistiques de qualité pour {output_table_name}:")
    print(f"     - Nombre total de lignes : {total_rows}")
    print(f"     - Nombre de colonnes métier : {total_cols}")
    
    # Calculer le taux de remplissage par colonne
    for column in [c for c in df.columns if not c.startswith("_")]:
        null_count = df.filter(col(column).isNull()).count()
        fill_rate = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
        if fill_rate < 100:
            print(f"     - {column}: {fill_rate:.1f}% rempli ({null_count} nulls)")

def write_to_bronze(df, output_table_name):
    """
    Écrit le DataFrame dans la couche Bronze.
    Tente MinIO d'abord, puis fallback local.
    """
    final_count = df.count()
    
    # Essai d'écriture dans MinIO
    bronze_path = f"s3a://bronze/{output_table_name}"
    local_path = f"/tmp/bronze/{output_table_name}"
    
    try:
        print(f"   💾 Tentative d'écriture dans MinIO...")
        df.write.mode("overwrite").parquet(bronze_path)
        print(f"   ✅ {final_count} lignes écrites dans MinIO: {bronze_path}")
        return True, final_count
        
    except Exception as e:
        print(f"   ⚠️ Échec de l'écriture MinIO: {str(e).split(':')[0]}")
        print(f"   💾 Écriture de secours en local...")
        
        # Créer le répertoire local si nécessaire
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        df.write.mode("overwrite").parquet(local_path)
        print(f"   ✅ {final_count} lignes sauvegardées localement: {local_path}")
        return False, final_count

def process_source(spark, config):
    """
    Fonction générique pour lire une source, la nettoyer, la traiter 
    et l'écrire dans la couche Bronze.
    """
    source_type = config["type"]
    source_path = config["path"]
    output_table_name = config["output_table"]
    
    print(f"\n{'='*80}")
    print(f"🚀 Début du traitement : {output_table_name}")
    print(f"{'='*80}")
    print(f"Type: {source_type} | Source: {config['source_name']}")

    try:
        # ========================================
        # 1. LECTURE DE LA SOURCE
        # ========================================
        print(f"\n📥 Étape 1: Lecture de la source...")
        
        if source_type == "csv":
            if not source_path.startswith("file:///") or os.path.exists(source_path.replace("file://", "")):
                print(f"   ✓ Fichier trouvé: {source_path}")
            else:
                raise FileNotFoundError(f"Fichier non trouvé: {source_path}")

            reader = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("delimiter", config.get("delimiter", ",")) \
                .option("encoding", "UTF-8")
            
            if "decimal" in config:
                reader = reader.option("decimal", config["decimal"])
            
            df = reader.csv(source_path)

        elif source_type == "excel":
            print(f"   ⚠️ Lecture Excel désactivée (packages manquants)")
            print(f"   ℹ️ Utilisez la version CSV si disponible")
            return

        elif source_type == "postgres":
            print(f"   ⏳ Connexion à PostgreSQL...")
            df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://bigdata_postgres:5432/healthcare_data") \
                .option("dbtable", source_path) \
                .option("user", "admin") \
                .option("password", "admin123") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            print(f"   ✓ Données chargées depuis PostgreSQL")
        else:
            raise ValueError(f"Type de source non supporté : {source_type}")

        initial_count = df.count()
        print(f"   ✓ {initial_count} lignes lues")

        # ========================================
        # 2. NETTOYAGE DES NOMS DE COLONNES
        # ========================================
        print(f"\n🧹 Étape 2: Nettoyage des noms de colonnes...")
        df = clean_col_names(df)
        print(f"   ✓ Colonnes standardisées: {', '.join(df.columns[:5])}...")

        # ========================================
        # 3. NETTOYAGE DES DONNÉES
        # ========================================
        print(f"\n🧼 Étape 3: Nettoyage des données...")
        df = clean_data(df)
        df = remove_empty_rows(df, threshold=0.3)  # Garde les lignes avec au moins 30% de données
        after_clean_count = df.count()
        removed_empty = initial_count - after_clean_count
        if removed_empty > 0:
            print(f"   ✓ {removed_empty} lignes vides supprimées")

        # ========================================
        # 4. NORMALISATION
        # ========================================
        print(f"\n📐 Étape 4: Normalisation des données...")
        df = normalize_data(df)
        print(f"   ✓ Données normalisées")

        # ========================================
        # 5. AJOUT DES COLONNES TECHNIQUES (AVANT ANONYMISATION)
        # ========================================
        print(f"\n⚙️ Étape 5: Ajout des colonnes techniques...")
        df = add_technical_columns(df, config["source_name"], output_table_name)
        print(f"   ✓ Colonnes techniques ajoutées")

        # ========================================
        # 6. ANONYMISATION DES PII
        # ========================================
        print(f"\n🔐 Étape 6: Anonymisation des données sensibles...")
        pii_columns = config.get("pii_columns", [])
        if pii_columns:
            df = anonymize_pii(df, pii_columns)
        else:
            print(f"   ℹ️ Aucune colonne PII à anonymiser")

        # ========================================
        # 7. AJOUT DE LA CLÉ DE SUBSTITUTION
        # ========================================
        print(f"\n🔑 Étape 7: Création de la clé de substitution...")
        sk_columns = config.get("sk_columns", [])
        df = add_surrogate_key(df, sk_columns, output_table_name)

        # ========================================
        # 8. SUPPRESSION DES DOUBLONS
        # ========================================
        print(f"\n🗑️ Étape 8: Suppression des doublons...")
        before_dedup = df.count()
        df = remove_duplicates(df, sk_columns)
        after_dedup = df.count()
        duplicates_removed = before_dedup - after_dedup
        if duplicates_removed > 0:
            print(f"   ✓ {duplicates_removed} doublons supprimés")
        else:
            print(f"   ✓ Aucun doublon trouvé")

        # ========================================
        # 9. STATISTIQUES DE QUALITÉ
        # ========================================
        get_data_quality_stats(df, output_table_name)

        # ========================================
        # 10. ÉCRITURE DANS BRONZE
        # ========================================
        print(f"\n💾 Étape 10: Écriture dans la couche Bronze...")
        minio_success, final_count = write_to_bronze(df, output_table_name)
        
        # ========================================
        # 11. AFFICHAGE DU SCHÉMA FINAL
        # ========================================
        print(f"\n📋 Schéma final:")
        df.printSchema()
        
        # ========================================
        # RÉSUMÉ
        # ========================================
        print(f"\n✅ Traitement terminé avec succès!")
        print(f"   - Lignes initiales : {initial_count}")
        print(f"   - Lignes après nettoyage : {after_clean_count}")
        print(f"   - Lignes après dédoublonnage : {final_count}")
        print(f"   - Taux de rétention : {(final_count/initial_count*100):.1f}%")
        if not minio_success:
            print(f"   ⚠️  Données sauvegardées localement (MinIO non disponible)")

    except FileNotFoundError as e:
        print(f"\n❌ ERREUR : Fichier non trouvé")
        print(f"   {str(e)}")
    except Exception as e:
        print(f"\n❌ ERREUR inattendue lors du traitement")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*80}\n")

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  BRONZE INGESTION PIPELINE - MinIO                           ║
    ║  Pipeline de nettoyage, normalisation et anonymisation       ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        # Configuration des sources (CSV et PostgreSQL uniquement)
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
        {"type": "excel", "source_name": "dpa_had_recueil2016_donnee2015_donnees.xlsx", "path": "file:///data/source/xlsx/dpa_had_recueil2016_donnee2015_donnees.xlsx", "sheet_name": "dpa_had_recueil2016_donnee2015_", "output_table": "satisfaction_2015_dpa_had_excel", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-esatisca-mco-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-esatisca-mco-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_esatisca", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-esatis48h-mco-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-esatis48h-mco-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_esatis48h", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "excel", "source_name": "resultats-iqss-open-data-2020.xlsx", "path": "file:///data/source/xlsx/resultats-iqss-open-data-2020.xlsx", "sheet_name": "Resultats", "output_table": "satisfaction_2020_iqss", "pii_columns": [], "sk_columns": ["finess"]},
        {"type": "postgres", "source_name": "public.patient", "path": "public.patient", "output_table": "patients", "pii_columns": ["nom", "prenom", "adresse", "ville", "email", "tel", "num_secu"], "sk_columns": ["id_patient"]},
        {"type": "postgres", "source_name": "public.consultation", "path": "public.consultation", "output_table": "consultations", "pii_columns": ["id_patient", "id_prof_sante"], "sk_columns": ["num_consultation"]},
        {"type": "postgres", "source_name": "public.diagnostic", "path": "public.diagnostic", "output_table": "diagnostics", "pii_columns": [], "sk_columns": ["code_diag"]},
        {"type": "postgres", "source_name": "public.medicaments", "path": "public.medicaments", "output_table": "medicaments", "pii_columns": [], "sk_columns": ["code_cis"]},
        {"type": "postgres", "source_name": "public.professionnel_de_sante", "path": "public.professionnel_de_sante", "output_table": "professionnels_sante_db", "pii_columns": ["nom", "prenom"], "sk_columns": ["identifiant"]}
    ]

        # Traitement des sources
        successful_tables = []
        failed_tables = []
        
        for config in source_configs:
            try:
                process_source(spark, config)
                successful_tables.append(config["output_table"])
            except Exception as e:
                print(f"💥 Échec du traitement pour {config['output_table']}: {e}")
                failed_tables.append(config["output_table"])

        spark.stop()
        
        # Résumé final
        print("\n" + "="*80)
        print("🎉 RÉSUMÉ DU PIPELINE BRONZE")
        print("="*80)
        print(f"✅ Tables traitées avec succès: {len(successful_tables)}")
        for table in successful_tables:
            print(f"   - {table}")
        
        if failed_tables:
            print(f"❌ Tables en échec: {len(failed_tables)}")
            for table in failed_tables:
                print(f"   - {table}")
        
        print(f"\n📊 Total: {len(successful_tables) + len(failed_tables)} tables traitées")
        print("="*80)
        
    except Exception as e:
        print(f"💥 Erreur critique: {e}")
        import traceback
        traceback.print_exc()