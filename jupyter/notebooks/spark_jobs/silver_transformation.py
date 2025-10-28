#!/usr/bin/env python3
"""
Pipeline Silver - Transformation Bronze vers modèle dimensionnel
Création de tables de dimensions et de faits pour la zone Gold
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
import sys

# Configuration
MINIO_ENDPOINT = "http://172.18.0.2:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

def get_spark_session():
    """Initialise Spark avec configuration S3A."""
    builder = SparkSession.builder \
        .appName("Silver Transformation") \
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

def add_silver_metadata(df, table_name, batch_id):
    """Ajoute les métadonnées Silver."""
    return df \
        .withColumn("_silver_load_date", F.current_timestamp()) \
        .withColumn("_silver_batch_id", F.lit(batch_id)) \
        .withColumn("_silver_table", F.lit(table_name))

def remove_all_null_rows_silver(df, key_columns):
    """
    Supprime les lignes où toutes les colonnes métier (hors clés et metadata) sont NULL
    key_columns: colonnes qui DOIVENT être non-NULL (ex: ['etablissement_id'])
    """
    # Toujours filtrer les clés NULL
    for key_col in key_columns:
        if key_col in df.columns:
            df = df.filter(F.col(key_col).isNotNull())
    
    # Colonnes à exclure du check NULL
    exclude_cols = key_columns + ['_silver_load_date', '_silver_batch_id', '_silver_table']
    
    # Colonnes métier
    business_cols = [c for c in df.columns if c not in exclude_cols]
    
    if not business_cols:
        return df
    
    # Au moins une colonne métier doit être non-NULL
    condition = None
    for col_name in business_cols:
        if condition is None:
            condition = F.col(col_name).isNotNull()
        else:
            condition = condition | F.col(col_name).isNotNull()
    
    return df.filter(condition) if condition is not None else df

def write_silver_table(df, table_name):
    """Écrit une table dans Silver."""
    output_path = f"s3a://{BUCKET_SILVER}/{table_name}/"
    print(f"   💾 Écriture: {output_path} ({df.count()} lignes)")
    df.write.mode("overwrite").parquet(output_path)
    print(f"   ✅ {table_name} terminé")
    return True

# ============================================
# DIMENSIONS
# ============================================

def create_dim_temps(spark, df_deces, batch_id):
    """Dimension Temps - basée sur les dates de décès."""
    print("\n🔷 DIM_TEMPS")
    
    # Extraire toutes les dates uniques
    dates = df_deces.select("date_deces").distinct()
    
    dim_temps = dates \
        .withColumn("date_id", F.monotonically_increasing_id()) \
        .withColumn("annee", F.year(F.col("date_deces"))) \
        .withColumn("mois", F.month(F.col("date_deces"))) \
        .withColumn("jour", F.dayofmonth(F.col("date_deces"))) \
        .withColumn("trimestre", F.quarter(F.col("date_deces"))) \
        .withColumn("jour_semaine", F.dayofweek(F.col("date_deces"))) \
        .withColumn("nom_mois", F.date_format(F.col("date_deces"), "MMMM")) \
        .withColumn("annee_mois", F.date_format(F.col("date_deces"), "yyyy-MM")) \
        .withColumn("est_weekend", F.when(F.col("jour_semaine").isin(1, 7), True).otherwise(False))
    
    dim_temps = add_silver_metadata(dim_temps, "dim_temps", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    dim_temps = remove_all_null_rows_silver(dim_temps, ['date_id'])
    
    write_silver_table(dim_temps, "dim_temps")
    return dim_temps

def create_dim_geographie(spark, df_deces, df_etablissements, batch_id):
    """Dimension Géographie - localisation géographique."""
    print("\n🔷 DIM_GEOGRAPHIE")
    
    # Extraire les lieux de décès
    geo_deces = df_deces.select(
        F.col("code_lieu_deces").alias("code_lieu"),
        F.lit("Décès").alias("type_lieu")
    ).filter(F.col("code_lieu_deces").isNotNull())
    
    # Extraire les lieux depuis établissements si disponibles
    geo_cols = []
    if "code_postal" in df_etablissements.columns:
        geo_etab = df_etablissements.select(
            F.col("code_postal").alias("code_lieu"),
            F.col("departement").alias("departement") if "departement" in df_etablissements.columns else F.lit(None).alias("departement"),
            F.col("region").alias("region") if "region" in df_etablissements.columns else F.lit(None).alias("region"),
            F.col("ville").alias("ville") if "ville" in df_etablissements.columns else F.lit(None).alias("ville"),
            F.lit("Etablissement").alias("type_lieu")
        ).filter(F.col("code_postal").isNotNull()).distinct()
        
        # Union avec les lieux de décès
        dim_geo = geo_deces.unionByName(
            geo_etab.select("code_lieu", "type_lieu"),
            allowMissingColumns=True
        ).distinct()
        
        # Enrichir avec les infos des établissements
        dim_geo = dim_geo.join(
            geo_etab.select("code_lieu", "departement", "region", "ville").distinct(),
            "code_lieu",
            "left"
        )
    else:
        dim_geo = geo_deces.distinct()
    
    # Ajouter un ID et extraire code département du code postal
    dim_geo = dim_geo \
        .withColumn("geo_id", F.monotonically_increasing_id()) \
        .withColumn("code_departement", 
                   F.when(F.length(F.col("code_lieu")) >= 2, F.substring(F.col("code_lieu"), 1, 2))
                   .otherwise(F.col("code_lieu")))
    
    dim_geo = add_silver_metadata(dim_geo, "dim_geographie", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    dim_geo = remove_all_null_rows_silver(dim_geo, ['geo_id'])
    
    write_silver_table(dim_geo, "dim_geographie")
    return dim_geo

def create_dim_etablissement(spark, df_etablissements, batch_id):
    """Dimension Établissement."""
    print("\n🔷 DIM_ETABLISSEMENT")
    
    # Sélectionner les colonnes clés
    cols_to_select = ["_sk"]
    optional_cols = {
        "finess_site": "finess",
        "identifiant_organisation": "identifiant",
        "raison_sociale_site": "raison_sociale_site",
        "commune": "commune",
        "region": "region",
        "departement": "departement",
        "ville": "ville",
        "code_postal": "code_postal",
        "statut_juridique": "statut_juridique",
        "categorie": "categorie"
    }
    
    for col_name, alias in optional_cols.items():
        if col_name in df_etablissements.columns:
            cols_to_select.append(col_name)
    
    dim_etab = df_etablissements.select(*[F.col(c) for c in cols_to_select if c in df_etablissements.columns])
    
    # Renommer _sk en etablissement_id
    dim_etab = dim_etab.withColumnRenamed("_sk", "etablissement_id")
    
    dim_etab = add_silver_metadata(dim_etab, "dim_etablissement", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    dim_etab = remove_all_null_rows_silver(dim_etab, ['etablissement_id'])
    
    write_silver_table(dim_etab, "dim_etablissement")
    return dim_etab

def create_dim_professionnel(spark, df_professionnels, batch_id):
    """Dimension Professionnel de santé."""
    print("\n🔷 DIM_PROFESSIONNEL")
    
    # Sélectionner les colonnes pertinentes (PII déjà anonymisées)
    cols_to_select = ["_sk"]
    optional_cols = {
        "civilite": "civilite",
        "profession": "profession",
        "specialite": "specialite",
        "categorie_professionnelle": "categorie_professionnelle",
        "identifiant": "identifiant"
    }
    
    for col_name in optional_cols.keys():
        if col_name in df_professionnels.columns:
            cols_to_select.append(col_name)
    
    dim_prof = df_professionnels.select(*[F.col(c) for c in cols_to_select if c in df_professionnels.columns])
    dim_prof = dim_prof.withColumnRenamed("_sk", "professionnel_id")
    
    dim_prof = add_silver_metadata(dim_prof, "dim_professionnel", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    dim_prof = remove_all_null_rows_silver(dim_prof, ['professionnel_id'])
    
    write_silver_table(dim_prof, "dim_professionnel")
    return dim_prof

# ============================================
# FAITS
# ============================================

def create_fait_deces(spark, df_deces, dim_temps, dim_geo, batch_id):
    """Table de faits Décès."""
    print("\n📊 FAIT_DECES")
    
    fait = df_deces.select(
        F.col("_sk").alias("deces_id"),
        F.col("date_deces"),
        F.col("sexe"),
        F.col("date_naissance"),
        F.col("code_lieu_deces"),
        F.col("annee_deces")
    )
    
    # Calculer l'âge au décès
    fait = fait.withColumn(
        "age_deces",
        F.when(F.col("date_naissance").isNotNull() & F.col("date_deces").isNotNull(),
             F.floor(F.datediff(F.col("date_deces"), F.col("date_naissance")) / 365.25))
        .otherwise(None)
    )
    
    # Catégorie d'âge
    fait = fait.withColumn(
        "categorie_age",
        F.when(F.col("age_deces") < 1, "< 1 an")
        .F.when(F.col("age_deces") < 18, "1-17 ans")
        .F.when(F.col("age_deces") < 30, "18-29 ans")
        .F.when(F.col("age_deces") < 45, "30-44 ans")
        .F.when(F.col("age_deces") < 60, "45-59 ans")
        .F.when(F.col("age_deces") < 75, "60-74 ans")
        .F.when(F.col("age_deces") < 90, "75-89 ans")
        .F.when(F.col("age_deces") >= 90, "90+ ans")
        .otherwise("Inconnu")
    )
    
    # Jointure avec dimension temps
    fait = fait.join(
        dim_temps.select("date_deces", "date_id"),
        "date_deces",
        "left"
    )
    
    # Jointure avec dimension géographie
    fait = fait.join(
        dim_geo.select(F.col("code_lieu").alias("code_lieu_deces"), "geo_id"),
        "code_lieu_deces",
        "left"
    )
    
    fait = add_silver_metadata(fait, "fait_deces", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    fait = remove_all_null_rows_silver(fait, ['deces_id'])
    
    write_silver_table(fait, "fait_deces")
    return fait

def create_fait_activite(spark, df_activite, batch_id):
    """Table de faits Activité professionnelle."""
    print("\n📊 FAIT_ACTIVITE")
    
    # Vérifier les colonnes disponibles
    available_cols = df_activite.columns
    
    select_cols = ["_sk"]
    optional_mapping = {
        "Id_prof_sante": "professionnel_id",
        "Date": "date_activite",
        "Code_specialite": "code_specialite",
        "Nombre_actes": "nombre_actes",
        "Montant": "montant"
    }
    
    for src_col, dest_col in optional_mapping.items():
        if src_col in available_cols:
            select_cols.append(src_col)
    
    fait = df_activite.select(*[F.col(c) for c in select_cols if c in available_cols])
    fait = fait.withColumnRenamed("_sk", "activite_id")
    
    # Ajouter année si date disponible
    if "Date" in available_cols:
        fait = fait.withColumn("annee", F.year(F.col("Date")))
    
    fait = add_silver_metadata(fait, "fait_activite", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    fait = remove_all_null_rows_silver(fait, ['activite_id'])
    
    write_silver_table(fait, "fait_activite")
    return fait

def create_fait_hospitalisation(spark, df_hospi, batch_id):
    """Table de faits Hospitalisation."""
    print("\n📊 FAIT_HOSPITALISATION")
    
    cols_to_keep = ["_sk"]
    available = df_hospi.columns
    
    optional = [
        "Date", "Duree_sejour", "Type_admission", "Mode_sortie",
        "Code_diagnostic", "Age", "Sexe"
    ]
    
    for col_name in optional:
        if col_name in available:
            cols_to_keep.append(col_name)
    
    fait = df_hospi.select(*[F.col(c) for c in cols_to_keep if c in available])
    fait = fait.withColumnRenamed("_sk", "hospitalisation_id")
    
    fait = add_silver_metadata(fait, "fait_hospitalisation", batch_id)
    
    # Nettoyage: supprimer lignes avec toutes colonnes NULL
    fait = remove_all_null_rows_silver(fait, ['hospitalisation_id'])
    
    write_silver_table(fait, "fait_hospitalisation")
    return fait

def create_fait_satisfaction(spark, satisfaction_tables, batch_id):
    """Consolide toutes les tables de satisfaction en un fait unifié."""
    print("\n📊 FAIT_SATISFACTION")
    
    all_satisfaction = []
    
    for table_name, df in satisfaction_tables:
        print(f"   📥 Traitement {table_name}")
        
        # Extraire l'année du nom de la table
        annee = None
        if "2014" in table_name:
            annee = 2014
        elif "2015" in table_name:
            annee = 2015
        elif "2016" in table_name:
            annee = 2016
        elif "2017" in table_name:
            annee = 2017
        elif "2018" in table_name:
            annee = 2018
        elif "2019" in table_name:
            annee = 2019
        
        # Déterminer le type de satisfaction
        type_sat = "Général"
        if "esatis48h" in table_name.lower():
            type_sat = "ESATIS 48H"
        elif "esatisca" in table_name.lower():
            type_sat = "ESATIS CA"
        elif "iqss" in table_name.lower():
            type_sat = "IQSS"
        elif "dpa" in table_name.lower():
            type_sat = "DPA"
        elif "rcp" in table_name.lower():
            type_sat = "RCP"
        elif "hpp" in table_name.lower():
            type_sat = "HPP"
        elif "idm" in table_name.lower():
            type_sat = "IDM"
        
        # Sélectionner colonnes communes
        df_subset = df.select("_sk") \
            .withColumn("annee_satisfaction", F.lit(annee)) \
            .withColumn("type_satisfaction", F.lit(type_sat)) \
            .withColumn("source_table", F.lit(table_name))
        
        # Ajouter colonnes métriques si disponibles
        for metric_col in ["Score", "Taux", "Note", "Indicateur", "Valeur"]:
            if metric_col in df.columns:
                df_subset = df_subset.withColumn("score", F.col(metric_col))
                break
        
        if "score" not in df_subset.columns:
            df_subset = df_subset.withColumn("score", F.lit(None).cast(DoubleType()))
        
        all_satisfaction.append(df_subset)
    
    # Union de toutes les tables
    if all_satisfaction:
        fait = all_satisfaction[0]
        for df_sat in all_satisfaction[1:]:
            fait = fait.unionByName(df_sat, allowMissingColumns=True)
        
        fait = fait.withColumnRenamed("_sk", "satisfaction_id")
        fait = add_silver_metadata(fait, "fait_satisfaction", batch_id)
        write_silver_table(fait, "fait_satisfaction")
        return fait
    else:
        print("   ⚠️  Aucune table de satisfaction disponible")
        return None

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════╗
    ║       PIPELINE SILVER - MODELISATION      ║
    ║     Dimensions + Faits pour KPIs Gold     ║
    ╚═══════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        batch_id = str(uuid.uuid4())
        print(f"📦 Batch ID: {batch_id}\n")
        
        # ===== CHARGEMENT BRONZE =====
        print("📥 CHARGEMENT BRONZE")
        
        df_deces = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/deces/")
        print(f"   ✅ deces: {df_deces.count()} lignes")
        
        df_etablissements = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/etablissements/")
        print(f"   ✅ etablissements: {df_etablissements.count()} lignes")
        
        df_professionnels = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/professionnels_sante/")
        print(f"   ✅ professionnels: {df_professionnels.count()} lignes")
        
        df_activite = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/activite_professionnels/")
        print(f"   ✅ activite: {df_activite.count()} lignes")
        
        df_hospi = spark.read.parquet(f"s3a://{BUCKET_BRONZE}/hospitalisations/")
        print(f"   ✅ hospitalisations: {df_hospi.count()} lignes")
        
        # Charger toutes les tables de satisfaction
        satisfaction_tables = []
        for year in ["2014", "2015", "2016", "2017", "2018", "2019"]:
            for table_type in ["esatis48h", "esatisca", "iqss", "dpa_ssr", "rcp_mco", "hpp_mco", "idm_mco", "dan_mco", "dpa_had", "ete_ortho"]:
                try:
                    path = f"s3a://{BUCKET_BRONZE}/satisfaction_{year}_{table_type}/"
                    df_sat = spark.read.parquet(path)
                    satisfaction_tables.append((f"satisfaction_{year}_{table_type}", df_sat))
                    print(f"   ✅ satisfaction_{year}_{table_type}: {df_sat.count()} lignes")
                except:
                    pass
        
        print(f"\n📊 Total satisfaction: {len(satisfaction_tables)} tables")
        
        # ===== CRÉATION DIMENSIONS =====
        print("\n" + "="*60)
        print("🔷 CRÉATION DES DIMENSIONS")
        print("="*60)
        
        dim_temps = create_dim_temps(spark, df_deces, batch_id)
        dim_geo = create_dim_geographie(spark, df_deces, df_etablissements, batch_id)
        dim_etab = create_dim_etablissement(spark, df_etablissements, batch_id)
        dim_prof = create_dim_professionnel(spark, df_professionnels, batch_id)
        
        # ===== CRÉATION FAITS =====
        print("\n" + "="*60)
        print("📊 CRÉATION DES FAITS")
        print("="*60)
        
        fait_deces = create_fait_deces(spark, df_deces, dim_temps, dim_geo, batch_id)
        fait_activite = create_fait_activite(spark, df_activite, batch_id)
        fait_hospi = create_fait_hospitalisation(spark, df_hospi, batch_id)
        fait_sat = create_fait_satisfaction(spark, satisfaction_tables, batch_id)
        
        # ===== RÉSUMÉ =====
        print("\n" + "="*60)
        print("🎉 PIPELINE SILVER TERMINÉ")
        print("="*60)
        
        print("\n✅ DIMENSIONS créées:")
        print(f"   🔷 dim_temps")
        print(f"   🔷 dim_geographie")
        print(f"   🔷 dim_etablissement")
        print(f"   🔷 dim_professionnel")
        
        print("\n✅ FAITS créés:")
        print(f"   📊 fait_deces")
        print(f"   📊 fait_activite")
        print(f"   📊 fait_hospitalisation")
        if fait_sat:
            print(f"   📊 fait_satisfaction")
        
        print("\n💾 Données disponibles dans s3a://silver/")
        
        spark.stop()
        
    except Exception as e:
        print(f"\n💥 ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
