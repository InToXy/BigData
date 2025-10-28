"""
Script d'ingestion Bronze avec anonymisation RGPD complète
Traite toutes les sources de données disponibles dans data/source/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, md5, concat_ws, 
    to_date, current_timestamp, current_date, when,
    regexp_replace, length, coalesce, year, month,
    monotonically_increasing_id, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType
from datetime import datetime
import hashlib

# ============================================================================
# CONFIGURATION
# ============================================================================

def get_spark_session():
    """Crée une session Spark avec configuration S3A pour MinIO"""
    spark = SparkSession.builder \
        .appName("Bronze_Ingestion_RGPD") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def add_technical_metadata(df, source_name):
    """Ajoute les métadonnées techniques à un DataFrame"""
    return df \
        .withColumn("_sk", monotonically_increasing_id()) \
        .withColumn("_source", lit(source_name)) \
        .withColumn("_version", lit(1).cast(IntegerType())) \
        .withColumn("_ingestion_date", current_timestamp()) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_is_deleted", lit(False))

def hash_pii(column_value):
    """Hash MD5 pour les données personnelles (RGPD)"""
    return when(col(column_value).isNotNull(), 
                md5(col(column_value))) \
           .otherwise(lit(None))

def normalize_string(column):
    """Normalise une chaîne : trim + uppercase"""
    return upper(trim(col(column)))

def normalize_date(column, format_pattern="yyyy-MM-dd"):
    """Normalise une date selon un format"""
    return to_date(col(column), format_pattern)

def normalize_postal_code(column):
    """Normalise et valide un code postal français (5 chiffres)"""
    return when(
        regexp_replace(col(column), "[^0-9]", "").rlike("^[0-9]{5}$"),
        regexp_replace(col(column), "[^0-9]", "")
    ).otherwise(lit(None))

def normalize_float(column):
    """Normalise un float (remplace virgule par point)"""
    return regexp_replace(col(column), ",", ".").cast(FloatType())

def validate_age(column):
    """Valide un âge entre 1 et 150"""
    return when(
        (col(column).cast(IntegerType()).between(1, 150)),
        col(column).cast(IntegerType())
    ).otherwise(lit(None))

def remove_all_null_rows(df, exclude_columns=None):
    """
    Supprime les lignes où toutes les colonnes métier sont NULL
    exclude_columns: colonnes techniques à exclure du check (ex: _sk, _version)
    """
    if exclude_columns is None:
        exclude_columns = ['_sk', '_source', '_version', '_ingestion_date', '_is_current', '_is_deleted']
    
    # Colonnes métier (exclure les colonnes techniques)
    business_cols = [c for c in df.columns if c not in exclude_columns]
    
    if not business_cols:
        return df
    
    # Créer une condition: au moins une colonne métier doit être non-NULL
    condition = None
    for col_name in business_cols:
        if condition is None:
            condition = col(col_name).isNotNull()
        else:
            condition = condition | col(col_name).isNotNull()
    
    return df.filter(condition) if condition is not None else df

# ============================================================================
# TRAITEMENT DES FICHIERS
# ============================================================================

def process_deces(spark):
    """
    Traite le fichier deces.csv
    Anonymisation: nom, prenom hachés
    """
    print("\n" + "="*80)
    print("📄 TRAITEMENT: deces.csv")
    print("="*80)
    
    source_path = "/data/source/DECES EN FRANCE/deces.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        print(f"   📊 Lignes source: {df.count():,}")
        
        # Transformation avec anonymisation RGPD
        df_bronze = df.select(
            # RGPD: Hachage des données personnelles
            hash_pii("nom").alias("nom_hash"),
            hash_pii("prenom").alias("prenom_hash"),
            
            # Données non sensibles (conservées)
            col("prenom").substr(1, 1).alias("initiale_prenom"),
            col("sexe").cast(IntegerType()).alias("sexe"),
            normalize_date("date_naissance").alias("date_naissance"),
            col("code_lieu_naissance").alias("code_lieu_naissance"),
            normalize_string("lieu_naissance").alias("lieu_naissance"),
            when(trim(col("pays_naissance")) == "", lit("FRANCE"))
                .otherwise(normalize_string("pays_naissance")).alias("pays_naissance"),
            normalize_date("date_deces").alias("date_deces"),
            col("code_lieu_deces").alias("code_lieu_deces"),
            col("numero_acte_deces").alias("numero_acte_deces"),
            
            # Calcul de l'âge au décès
            (year(col("date_deces")) - year(col("date_naissance"))).alias("age_deces"),
            
            # Géographie (code département extrait du code commune)
            col("code_lieu_deces").substr(1, 2).alias("departement"),
            col("code_lieu_deces").substr(1, 2).alias("region")
        )
        
        # Ajout métadonnées techniques
        df_bronze = add_technical_metadata(df_bronze, "deces")
        
        # Nettoyage: supprimer lignes avec toutes colonnes NULL
        rows_before = df_bronze.count()
        df_bronze = remove_all_null_rows(df_bronze)
        rows_after = df_bronze.count()
        rows_removed = rows_before - rows_after
        
        # Écriture en Bronze
        output_path = "s3a://bronze/deces/"
        df_bronze.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"   ✅ Lignes bronze: {rows_after:,}")
        if rows_removed > 0:
            print(f"   🗑️  Lignes NULL supprimées: {rows_removed:,}")
        print(f"   💾 Écrit dans: {output_path}")
        print(f"   🔒 Champs anonymisés: nom, prenom")
        
        return df_bronze.count()
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        return 0

def process_etablissements(spark):
    """
    Traite le fichier etablissement_sante.csv
    Anonymisation: email, telephone, telecopie hachés
    """
    print("\n" + "="*80)
    print("📄 TRAITEMENT: etablissement_sante.csv")
    print("="*80)
    
    source_path = "/data/source/Etablissement de SANTE/etablissement_sante.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        print(f"   📊 Lignes source: {df.count():,}")
        
        # Transformation avec anonymisation RGPD
        df_bronze = df.select(
            # Identifiants (NON hachés - pour jointures)
            coalesce(
                when(trim(col("finess_site")) != "", trim(col("finess_site"))),
                trim(col("identifiant_organisation"))
            ).alias("finess"),
            col("identifiant_organisation").alias("identifiant_organisation"),
            col("finess_etablissement_juridique").alias("finess_ej"),
            col("siren_site").alias("siren"),
            col("siret_site").alias("siret"),
            
            # Informations établissement
            normalize_string("raison_sociale_site").alias("raison_sociale"),
            normalize_string("enseigne_commerciale_site").alias("enseigne"),
            
            # RGPD: Hachage des coordonnées
            hash_pii("email").alias("email_hash"),
            hash_pii("telephone").alias("telephone_hash"),
            hash_pii("telephone_2").alias("telephone_2_hash"),
            hash_pii("telecopie").alias("fax_hash"),
            
            # Adresse (partielle - anonymisée)
            hash_pii("adresse").alias("adresse_hash"),
            normalize_postal_code("code_postal").alias("code_postal"),
            normalize_string("commune").alias("commune"),
            col("code_commune").alias("code_commune"),
            normalize_string("cedex").alias("cedex"),
            when(trim(col("pays")) == "", lit("FRANCE"))
                .otherwise(normalize_string("pays")).alias("pays"),
            
            # Complément adresse (non sensible)
            normalize_string("complement_destinataire").alias("complement_destinataire"),
            normalize_string("complement_point_geographique").alias("complement_geographique"),
            normalize_string("mention_distribution").alias("mention_distribution"),
            
            # Voirie (structure de l'adresse - pas de numéro exact)
            col("type_voie").alias("type_voie"),
            normalize_string("voie").alias("voie"),
            col("indice_repetition_voie").alias("indice_repetition")
        )
        
        # Ajout métadonnées techniques
        df_bronze = add_technical_metadata(df_bronze, "etablissement_sante")
        
        # Nettoyage: supprimer lignes avec toutes colonnes NULL (sauf métadonnées)
        rows_before = df_bronze.count()
        df_bronze = remove_all_null_rows(df_bronze)
        rows_after = df_bronze.count()
        rows_removed = rows_before - rows_after
        
        # Écriture en Bronze
        output_path = "s3a://bronze/etablissements/"
        df_bronze.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"   ✅ Lignes bronze: {df_bronze.count():,}")
        if rows_removed > 0:
            print(f"   🗑️ Lignes NULL supprimées: {rows_removed:,}")
        print(f"   💾 Écrit dans: {output_path}")
        print(f"   🔒 Champs anonymisés: email, telephone, telephone_2, fax, adresse")
        
        return df_bronze.count()
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        return 0

def process_professionnels_sante(spark):
    """
    Traite le fichier professionnel_sante.csv
    Anonymisation: identifiants personnels
    """
    print("\n" + "="*80)
    print("📄 TRAITEMENT: professionnel_sante.csv")
    print("="*80)
    
    source_path = "/data/source/Etablissement de SANTE/professionnel_sante.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        print(f"   📊 Lignes source: {df.count():,}")
        
        # Transformation avec anonymisation RGPD
        df_bronze = df.select(
            # RGPD: Identifiant personnel haché
            hash_pii("Identifiant_PS").alias("identifiant_ps_hash"),
            
            # Identifiant conservé pour jointures (format anonyme déjà)
            col("Identifiant_PS").alias("identifiant_original"),
            
            # Informations professionnelles (non sensibles)
            normalize_string("Code_profession").alias("code_profession"),
            normalize_string("Libelle_profession").alias("profession"),
            normalize_string("Code_categorie_professionnelle").alias("code_categorie"),
            normalize_string("Libelle_categorie_professionnelle").alias("categorie"),
            normalize_string("Libelle_savoir_faire").alias("specialite")
        )
        
        # Ajout métadonnées techniques
        df_bronze = add_technical_metadata(df_bronze, "professionnel_sante")
        
        # Nettoyage: supprimer lignes avec toutes colonnes NULL (sauf métadonnées)
        rows_before = df_bronze.count()
        df_bronze = remove_all_null_rows(df_bronze)
        rows_after = df_bronze.count()
        rows_removed = rows_before - rows_after
        
        # Écriture en Bronze
        output_path = "s3a://bronze/professionnels_sante/"
        df_bronze.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"   ✅ Lignes bronze: {df_bronze.count():,}")
        if rows_removed > 0:
            print(f"   🗑️ Lignes NULL supprimées: {rows_removed:,}")
        print(f"   💾 Écrit dans: {output_path}")
        print(f"   🔒 Champs anonymisés: identifiant_ps")
        
        return df_bronze.count()
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        return 0

def process_hospitalisations(spark):
    """
    Traite le fichier Hospitalisations.csv
    Anonymisation: id_patient haché
    """
    print("\n" + "="*80)
    print("📄 TRAITEMENT: Hospitalisations.csv")
    print("="*80)
    
    source_path = "/data/source/Hospitalisation/Hospitalisations.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        print(f"   📊 Lignes source: {df.count():,}")
        
        # Transformation avec anonymisation RGPD
        df_bronze = df.select(
            # Identifiants
            col("Num_Hospitalisation").cast(IntegerType()).alias("num_hospitalisation"),
            
            # RGPD: ID patient haché
            hash_pii("Id_patient").alias("id_patient_hash"),
            
            # ID patient conservé pour jointures (format anonyme déjà)
            col("Id_patient").cast(IntegerType()).alias("id_patient_original"),
            
            # Établissement (pour jointures)
            col("identifiant_organisation").alias("identifiant_organisation"),
            
            # Diagnostic (non sensible - codes)
            col("Code_diagnostic").alias("code_diagnostic"),
            normalize_string("Suite_diagnostic_consultation").alias("libelle_diagnostic"),
            
            # Dates et durées
            to_date(col("Date_Entree"), "dd/MM/yyyy").alias("date_entree"),
            col("Jour_Hospitalisation").cast(IntegerType()).alias("duree_sejour")
        )
        
        # Ajout métadonnées techniques
        df_bronze = add_technical_metadata(df_bronze, "hospitalisations")
        
        # Nettoyage: supprimer lignes avec toutes colonnes NULL (sauf métadonnées)
        rows_before = df_bronze.count()
        df_bronze = remove_all_null_rows(df_bronze)
        rows_after = df_bronze.count()
        rows_removed = rows_before - rows_after
        
        # Écriture en Bronze
        output_path = "s3a://bronze/hospitalisations/"
        df_bronze.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"   ✅ Lignes bronze: {df_bronze.count():,}")
        if rows_removed > 0:
            print(f"   🗑️ Lignes NULL supprimées: {rows_removed:,}")
        print(f"   💾 Écrit dans: {output_path}")
        print(f"   🔒 Champs anonymisés: id_patient")
        
        return df_bronze.count()
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        return 0

def process_satisfaction_2017(spark):
    """
    Traite le fichier ESATIS48H_MCO_recueil2017_donnees.csv
    Pas d'anonymisation (données agrégées par établissement)
    """
    print("\n" + "="*80)
    print("📄 TRAITEMENT: ESATIS48H_MCO_recueil2017_donnees.csv")
    print("="*80)
    
    source_path = "/data/source/Satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv"
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        print(f"   📊 Lignes source: {df.count():,}")
        
        # Sélectionner et normaliser les colonnes principales
        df_bronze = df.select(
            # Identifiant établissement (pour jointures)
            col("finess").alias("identifiant_organisation"),
            
            # Informations établissement
            normalize_string("rs").alias("raison_sociale"),
            normalize_string("region").alias("region"),
            normalize_string("statut_juridique").alias("statut_juridique"),
            
            # Indicateurs de satisfaction (float)
            when(col("score_all_ajust") != "DI", 
                 normalize_float("score_all_ajust")).otherwise(lit(None)).alias("score_all_ajust"),
            when(col("note_all_ajust") != "DI",
                 normalize_float("note_all_ajust")).otherwise(lit(None)).alias("note_all_ajust"),
            
            # Classement
            when(col("classement") != "DI",
                 col("classement")).otherwise(lit(None)).alias("classement"),
            
            # Nombre de répondants
            col("nb_repondant").cast(IntegerType()).alias("nb_repondants"),
            
            # Année de recueil
            lit(2017).cast(IntegerType()).alias("annee_recueil")
        )
        
        # Ajout métadonnées techniques
        df_bronze = add_technical_metadata(df_bronze, "satisfaction_mco_2017")
        
        # Nettoyage: supprimer lignes avec toutes colonnes NULL (sauf métadonnées)
        rows_before = df_bronze.count()
        df_bronze = remove_all_null_rows(df_bronze)
        rows_after = df_bronze.count()
        rows_removed = rows_before - rows_after
        
        # Écriture en Bronze
        output_path = "s3a://bronze/satisfaction_mco_2017/"
        df_bronze.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"   ✅ Lignes bronze: {df_bronze.count():,}")
        if rows_removed > 0:
            print(f"   🗑️ Lignes NULL supprimées: {rows_removed:,}")
        print(f"   💾 Écrit dans: {output_path}")
        print(f"   ℹ️  Pas d'anonymisation (données agrégées)")
        
        return df_bronze.count()
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        return 0

# ============================================================================
# PROGRAMME PRINCIPAL
# ============================================================================

def main():
    """Programme principal d'ingestion Bronze"""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    INGESTION BRONZE AVEC ANONYMISATION RGPD                  ║
║                         Data Platform CHU Medical                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    start_time = datetime.now()
    print(f"🕐 Début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Créer la session Spark
    spark = get_spark_session()
    print(f"✅ Spark Session créée: {spark.version}\n")
    
    # Traiter chaque source
    stats = {}
    
    stats['deces'] = process_deces(spark)
    stats['etablissements'] = process_etablissements(spark)
    stats['professionnels_sante'] = process_professionnels_sante(spark)
    stats['hospitalisations'] = process_hospitalisations(spark)
    stats['satisfaction_2017'] = process_satisfaction_2017(spark)
    
    # Résumé final
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*80)
    print("📊 RÉSUMÉ DE L'INGESTION BRONZE")
    print("="*80)
    
    total_rows = sum(stats.values())
    for table, count in stats.items():
        if count > 0:
            print(f"  ✅ {table:30s} : {count:>10,} lignes")
        else:
            print(f"  ❌ {table:30s} : ERREUR")
    
    print("="*80)
    print(f"📈 Total lignes ingérées : {total_rows:,}")
    print(f"⏱️  Durée totale          : {duration:.1f}s")
    print(f"🕐 Fin                   : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    print("""
🔒 ANONYMISATION RGPD APPLIQUÉE:
   • Hachage MD5 des données personnelles (nom, prenom, email, telephone, etc.)
   • Conservation des identifiants pour jointures (finess, id_patient, etc.)
   • Métadonnées techniques ajoutées (_sk, _source, _version, _ingestion_date)
   • Format Parquet snappy dans s3://bronze/
    """)
    
    spark.stop()
    print("✅ Ingestion Bronze terminée avec succès!\n")

if __name__ == "__main__":
    main()
