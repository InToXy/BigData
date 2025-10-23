import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, to_date, count, sum, avg,
    when, lit, datediff, months_between, round, expr,
    countDistinct, dense_rank, row_number, desc, upper, trim,
    regexp_replace, coalesce, length, substring, concat_ws,
    current_timestamp, md5, sha2, mean, stddev, isnan, isnull,
    create_map, regexp_extract, lower, split, explode, size,
    array_contains, monotonically_increasing_id, quarter, 
    dayofweek, date_format, hour, minute, weekofyear
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType, BooleanType, FloatType
)
from pyspark.sql.window import Window
import os
import re

# Configuration MinIO
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin123",
    "bucket_silver": "silver",
    "bucket_bronze": "bronze"
}

# Configuration du partitionnement
PARTITIONING_CONFIG = {
    "fact_consultation": ["annee_consultation", "mois_consultation"],
    "fact_hospitalisation": ["annee_entree"],
    "fact_deces": ["annee_deces"],
    "fact_satisfaction": ["annee_enquete"],
    "mart_taux_consultation_etablissement": ["annee_consultation"],
    "mart_taux_consultation_diagnostic": ["annee_consultation"],
    "mart_taux_hospitalisation_global": ["annee_entree"],
    "mart_taux_hospitalisation_diagnostic": ["annee_entree"],
    "mart_taux_hospitalisation_demographie": ["annee_entree"],
    "mart_taux_consultation_professionnel": ["annee_consultation"],
    "mart_deces_localisation": ["annee_deces"],
    "mart_satisfaction_region": ["annee_enquete"]
}

def get_spark_session():
    """Session Spark optimisée pour Silver."""
    try:
        jars_dir = "/home/jovyan/jars"
        jar_files = [
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
            f"{jars_dir}/hadoop-common-3.3.4.jar"
        ]
        
        # Vérification des JARs
        for jar in jar_files:
            if not os.path.exists(jar):
                print(f"⚠️  JAR manquant: {jar} - continuation sans ce JAR")
                jar_files.remove(jar)
        
        if not jar_files:
            print("⚠️  Aucun JAR trouvé - utilisation de Spark sans S3 optimisé")
            jars_path = None
        else:
            jars_path = ",".join(jar_files)
            print(f"📚 JARs chargés: {len(jar_files)}")
        
        # Configuration Spark de base d'abord
        builder = SparkSession.builder \
            .appName("Silver_Pipeline_Gold_Ready") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skew.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "100") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")
        
        # Ajouter les JARs seulement s'ils existent
        if jars_path:
            builder = builder.config("spark.jars", jars_path)
            
        # Configuration S3A seulement si les JARs sont disponibles
        if jar_files:
            hadoop_conf = {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.endpoint": MINIO_CONFIG["endpoint"],
                "spark.hadoop.fs.s3a.access.key": MINIO_CONFIG["access_key"],
                "spark.hadoop.fs.s3a.secret.key": MINIO_CONFIG["secret_key"],
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.fast.upload": "true"
            }
            
            for key, value in hadoop_conf.items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # Configuration Hadoop explicite seulement si les JARs sont disponibles
        if jar_files:
            try:
                hadoop_conf = spark._jsc.hadoopConfiguration()
                hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
                hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
                hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
                hadoop_conf.set("fs.s3a.path.style.access", "true")
                hadoop_conf.set("fs.s3a.fast.upload", "true")
                print("✅ Configuration S3A appliquée")
            except Exception as hadoop_error:
                print(f"⚠️  Erreur configuration Hadoop: {hadoop_error}")
        
        print("✅ Spark Silver initialisé (prêt pour Gold)")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark Silver: {e}")
        # Essayer de créer une session Spark basique sans configuration avancée
        try:
            print("🔄 Tentative de création d'une session Spark basique...")
            spark = SparkSession.builder \
                .appName("Silver_Pipeline_Basic") \
                .getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            print("✅ Session Spark basique créée")
            return spark
        except Exception as fallback_error:
            print(f"❌ Échec de la création de session Spark: {fallback_error}")
            raise
            
def read_bronze_table(spark, table_name):
    """Lit une table depuis Bronze avec optimisation."""
    try:
        bronze_path = f"s3a://{MINIO_CONFIG['bucket_bronze']}/{table_name}"
        print(f"📂 Lecture {table_name} depuis Bronze...")
        
        df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(bronze_path)
        
        # Métadonnées pour Gold - utilisation de count() avec gestion d'erreur
        try:
            row_count = df.count()
            print(f"  ✅ {row_count:,} lignes | {len(df.columns)} colonnes")
        except Exception as count_error:
            print(f"  ⚠️  Impossible de compter les lignes: {count_error}")
            print(f"  📊 Schema: {len(df.columns)} colonnes")
            row_count = 0
        
        return df  # Retirer le cache() pour éviter les problèmes
        
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        raise

def create_conformed_dimensions(spark):
    """Crée les dimensions conformées pour Gold."""
    print("\n🏗️ CRÉATION DES DIMENSIONS POUR GOLD")
    
    dimensions = {}
    
    try:
        # Dimension Patient enrichie
        patients = read_bronze_table(spark, "patients")
        dim_patient = patients.select(
            col("_sk_patient").alias("patient_sk"),
            col("id_patient").alias("patient_nk"),
            col("nom"),
            col("prenom"),
            col("sexe"),
            to_date(col("date_naissance")).alias("date_naissance"),
            
            # Démographie enrichie
            year(current_timestamp()).alias("current_year"),
            (year(current_timestamp()) - year(to_date(col("date_naissance")))).alias("age"),
            
            # Tranches d'âge standardisées
            when((year(current_timestamp()) - year(to_date(col("date_naissance")))).isNull(), "Inconnu")
            .when((year(current_timestamp()) - year(to_date(col("date_naissance")))) < 18, "0-17")
            .when((year(current_timestamp()) - year(to_date(col("date_naissance")))) <= 35, "18-35")
            .when((year(current_timestamp()) - year(to_date(col("date_naissance")))) <= 55, "36-55")
            .when((year(current_timestamp()) - year(to_date(col("date_naissance")))) <= 75, "56-75")
            .otherwise("75+").alias("tranche_age"),
            
            # Géographie normalisée
            upper(trim(col("Ville"))).alias("ville_normalisee"),
            when(length(trim(col("code_postal"))) == 5, substring(trim(col("code_postal")), 1, 2))
              .otherwise("99").alias("departement_code"),
            
            # Metadata pour Gold
            current_timestamp().alias("silver_created_at"),
            lit("silver_layer").alias("source_layer"),
            lit(1).alias("is_active")
        ).distinct()
        
        patient_count = dim_patient.count()
        print(f"  ✅ Dim Patient: {patient_count:,} patients uniques")
        dimensions["dim_patient"] = dim_patient
        
        # Dimension Établissement
        etablissements = read_bronze_table(spark, "etablissements")
        
        # Vérification des colonnes disponibles pour debug
        available_columns = set(etablissements.columns)
        print(f"  🔍 Colonnes disponibles dans etablissements: {len(available_columns)}")
        
        # Construction dynamique basée sur les colonnes réelles
        select_exprs = [
            col("_sk_etablissement").alias("etablissement_sk"),
            col("identifiant_organisation").alias("etablissement_nk"),
            col("raison_sociale_site").alias("nom_etablissement"),
            
            # Catégorisation standardisée
            when(lower(col("raison_sociale_site")).contains("chu"), "CHU")
            .when(lower(col("raison_sociale_site")).contains("hopital"), "Hôpital")
            .when(lower(col("raison_sociale_site")).contains("clinique"), "Clinique")
            .when(lower(col("raison_sociale_site")).contains("centre hospitalier"), "Centre Hospitalier")
            .otherwise("Autre").alias("type_etablissement"),
        ]
        
        # Géographie basée sur les colonnes disponibles
        if 'commune' in available_columns:
            select_exprs.append(upper(trim(col("commune"))).alias("commune_normalisee"))
        else:
            select_exprs.append(lit("Commune inconnue").alias("commune_normalisee"))
        
        # Département déduit du code postal
        if 'code_postal' in available_columns:
            select_exprs.append(
                when(length(trim(col("code_postal"))) == 5, 
                     substring(trim(col("code_postal")), 1, 2))
                .otherwise("99").alias("departement_code")
            )
            select_exprs.append(
                when(length(trim(col("code_postal"))) == 5, 
                     concat_ws(" ", lit("Département"), substring(trim(col("code_postal")), 1, 2)))
                .otherwise("Département inconnu").alias("departement_normalise")
            )
        else:
            select_exprs.append(lit("99").alias("departement_code"))
            select_exprs.append(lit("Département inconnu").alias("departement_normalise"))
        
        # Région déduite du code postal (approximation France)
        if 'code_postal' in available_columns:
            select_exprs.append(
                when(substring(trim(col("code_postal")), 1, 2).isin("75", "77", "78", "91", "92", "93", "94", "95"), "Île-de-France")
                .when(substring(trim(col("code_postal")), 1, 2).isin("44", "49", "53", "72", "85"), "Pays de la Loire")
                .when(substring(trim(col("code_postal")), 1, 2).isin("35", "56", "22", "29"), "Bretagne")
                .when(substring(trim(col("code_postal")), 1, 2).isin("14", "27", "50", "61", "76"), "Normandie")
                .when(substring(trim(col("code_postal")), 1, 2).isin("02", "59", "60", "62", "80"), "Hauts-de-France")
                .when(substring(trim(col("code_postal")), 1, 2).isin("67", "68", "88"), "Grand Est")
                .when(substring(trim(col("code_postal")), 1, 2).isin("21", "25", "39", "58", "70", "71", "89", "90"), "Bourgogne-Franche-Comté")
                .when(substring(trim(col("code_postal")), 1, 2).isin("03", "15", "43", "63", "69", "73", "74"), "Auvergne-Rhône-Alpes")
                .when(substring(trim(col("code_postal")), 1, 2).isin("16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"), "Nouvelle-Aquitaine")
                .when(substring(trim(col("code_postal")), 1, 2).isin("09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"), "Occitanie")
                .when(substring(trim(col("code_postal")), 1, 2).isin("04", "05", "06", "13", "83", "84"), "Provence-Alpes-Côte d'Azur")
                .when(substring(trim(col("code_postal")), 1, 2).isin("20"), "Corse")
                .when(substring(trim(col("code_postal")), 1, 2).isin("97"), "Outre-Mer")
                .otherwise("Région inconnue").alias("region_normalisee")
            )
        else:
            select_exprs.append(lit("Région inconnue").alias("region_normalisee"))
        
        # Metadata
        select_exprs.extend([
            current_timestamp().alias("silver_created_at"),
            lit("silver_layer").alias("source_layer")
        ])
        
        dim_etablissement = etablissements.select(*select_exprs).distinct()
        
        etablissement_count = dim_etablissement.count()
        print(f"  ✅ Dim Établissement: {etablissement_count:,} établissements uniques")
        dimensions["dim_etablissement"] = dim_etablissement
        
        # Dimension Professionnel de Santé
        professionnels = read_bronze_table(spark, "professionnels")
        
        dim_professionnel = professionnels.select(
            col("_sk_professionnel").alias("professionnel_sk"),
            col("identifiant").alias("professionnel_nk"),
            col("nom"),
            col("prenom"),
            col("civilite"),
            col("categorie_professionnelle"),
            col("profession"),
            col("specialite"),
            upper(trim(col("commune"))).alias("commune_normalisee"),
            
            # Catégorisation des spécialités
            when(lower(col("specialite")).contains("generaliste"), "Médecin Généraliste")
            .when(lower(col("specialite")).contains("chirurg"), "Chirurgien")
            .when(lower(col("specialite")).contains("cardiologue"), "Cardiologue")
            .when(lower(col("specialite")).contains("pediatr"), "Pédiatre")
            .when(lower(col("specialite")).contains("gyneco"), "Gynécologue")
            .when(lower(col("specialite")).contains("psychiatr"), "Psychiatre")
            .when(lower(col("specialite")).contains("osteopathe"), "Ostéopathe")
            .otherwise("Autre spécialité").alias("categorie_specialite"),
            
            current_timestamp().alias("silver_created_at"),
            lit("silver_layer").alias("source_layer")
        ).distinct()
        
        professionnel_count = dim_professionnel.count()
        print(f"  ✅ Dim Professionnel: {professionnel_count:,} professionnels uniques")
        dimensions["dim_professionnel"] = dim_professionnel
        
        # Dimension Diagnostic
        diagnostics = read_bronze_table(spark, "diagnostics")
        
        dim_diagnostic = diagnostics.select(
            col("_sk_diagnostic").alias("diagnostic_sk"),
            col("code_diag").alias("diagnostic_nk"),
            col("Diagnostic").alias("libelle_diagnostic"),
            
            # Catégorisation des diagnostics
            when(col("code_diag").startswith("A"), "Maladies infectieuses")
            .when(col("code_diag").startswith("C"), "Tumeurs")
            .when(col("code_diag").startswith("I"), "Maladies cardiovasculaires")
            .when(col("code_diag").startswith("J"), "Maladies respiratoires")
            .when(col("code_diag").startswith("E"), "Maladies endocriniennes")
            .when(col("code_diag").startswith("F"), "Troubles mentaux")
            .when(col("code_diag").startswith("S") | col("code_diag").startswith("T"), "Traumatismes")
            .otherwise("Autres maladies").alias("categorie_diagnostic"),
            
            current_timestamp().alias("silver_created_at"),
            lit("silver_layer").alias("source_layer")
        ).distinct()
        
        diagnostic_count = dim_diagnostic.count()
        print(f"  ✅ Dim Diagnostic: {diagnostic_count:,} diagnostics uniques")
        dimensions["dim_diagnostic"] = dim_diagnostic
        
        # Dimension Temps (préparation pour Gold)
        print("  🕒 Préparation de la dimension Temps...")
        
        dates_df = spark.sql("""
            SELECT explode(sequence(to_date('2010-01-01'), to_date('2024-12-31'), interval 1 day)) as date_complete
        """)
        
        dim_temp = dates_df.select(
            col("date_complete").alias("date_complete"),
            year(col("date_complete")).alias("annee"),
            month(col("date_complete")).alias("mois"),
            quarter(col("date_complete")).alias("trimestre"),
            dayofmonth(col("date_complete")).alias("jour"),
            weekofyear(col("date_complete")).alias("semaine"),
            date_format(col("date_complete"), "EEEE").alias("jour_semaine"),
            dayofweek(col("date_complete")).alias("numero_jour_semaine"),
            when(dayofweek(col("date_complete")).isin(1, 7), "Weekend")
              .otherwise("Semaine").alias("type_jour")
        ).distinct()
        
        temp_count = dim_temp.count()
        print(f"  ✅ Dim Temps: {temp_count:,} dates préparées")
        dimensions["dim_temp"] = dim_temp
        
        return dimensions
        
    except Exception as e:
        print(f"❌ Erreur lors de la création des dimensions: {e}")
        # Nettoyer les DataFrames en cas d'erreur
        for df in dimensions.values():
            try:
                df.unpersist()
            except:
                pass
        raise

def create_gold_ready_facts(spark, dimensions):
    """Crée les faits préparés pour Gold."""
    print("\n📊 CRÉATION DES FAITS POUR GOLD")
    
    facts = {}
    
    try:
        dim_patient = dimensions["dim_patient"]
        dim_etablissement = dimensions["dim_etablissement"]
        dim_professionnel = dimensions["dim_professionnel"]
        dim_diagnostic = dimensions["dim_diagnostic"]
        
        # Fact Consultations avec jointure sur professionnel
        consultations = read_bronze_table(spark, "consultations")
        activites_pro = read_bronze_table(spark, "activites_professionnels")
        
        # Jointure étape par étape pour éviter les timeouts
        fact_consultation_base = consultations.alias("c") \
            .join(dim_patient.alias("p"), col("c.id_patient") == col("p.patient_nk"), "inner") \
            .join(dim_diagnostic.alias("d"), col("c._sk_diagnostic") == col("d.diagnostic_sk"), "left")
        
        # Jointure avec activités professionnelles
        fact_consultation_with_activites = fact_consultation_base \
            .join(activites_pro.alias("ap"), col("c.Id_prof_sante") == col("ap.identifiant"), "left")
        
        # Jointure finale avec établissement
        fact_consultation = fact_consultation_with_activites \
            .join(dim_etablissement.alias("e"), col("ap.identifiant_organisation") == col("e.etablissement_nk"), "left") \
            .select(
                # Clés conformées
                col("p.patient_sk"),
                col("e.etablissement_sk"),
                col("d.diagnostic_sk"),
                col("c._sk").alias("consultation_nk"),
                col("c.Id_prof_sante").alias("professionnel_nk"),
                
                # Dates - IMPORTANT: ces colonnes seront utilisées pour le partitionnement
                to_date(coalesce(col("c.Heure_debut"), current_timestamp())).alias("date_consultation"),
                year(coalesce(col("c.Heure_debut"), current_timestamp())).alias("annee_consultation"),
                month(coalesce(col("c.Heure_debut"), current_timestamp())).alias("mois_consultation"),
                quarter(coalesce(col("c.Heure_debut"), current_timestamp())).alias("trimestre_consultation"),
                
                # Mesures standardisées
                lit(1).alias("nb_consultations"),
                col("c.code_diag").alias("diagnostic_code"),
                col("c.Motif").alias("motif_consultation"),
                
                # Metadata pour Gold
                current_timestamp().alias("silver_created_at"),
                lit("consultation_bronze").alias("source_system")
            )
        
        consultation_count = fact_consultation.count()
        print(f"  ✅ Fact Consultation: {consultation_count:,} consultations")
        facts["fact_consultation"] = fact_consultation
        
        # Fact Hospitalisations - version simplifiée
        hospitalisations = read_bronze_table(spark, "hospitalisations")
        
        fact_hospitalisation = hospitalisations.alias("h") \
            .join(dim_patient.alias("p"), col("h.id_patient") == col("p.patient_nk"), "inner") \
            .join(dim_etablissement.alias("e"), col("h.identifiant_organisation") == col("e.etablissement_nk"), "inner") \
            .join(dim_diagnostic.alias("d"), col("h._sk_diagnostic") == col("d.diagnostic_sk"), "left") \
            .select(
                # Clés conformées
                col("p.patient_sk"),
                col("e.etablissement_sk"),
                col("d.diagnostic_sk"),
                col("h._sk").alias("hospitalisation_nk"),
                
                # Dates et durée - IMPORTANT: colonnes pour partitionnement
                to_date(col("h.Date_Entree")).alias("date_entree"),
                year(col("h.Date_Entree")).alias("annee_entree"),
                month(col("h.Date_Entree")).alias("mois_entree"),
                to_date(col("h.Date_Entree")).alias("date_sortie"),  # Approximation car pas de date_sortie
                col("h.Jour_Hospitalisation").alias("duree_sejour"),
                
                # Mesures
                lit(1).alias("nb_hospitalisations"),
                col("h.code_diag").alias("diagnostic_principal"),
                
                # Metadata
                current_timestamp().alias("silver_created_at"),
                lit("hospitalisation_bronze").alias("source_system")
            )
        
        hospitalisation_count = fact_hospitalisation.count()
        print(f"  ✅ Fact Hospitalisation: {hospitalisation_count:,} hospitalisations")
        facts["fact_hospitalisation"] = fact_hospitalisation
        
        # Fact Décès - version simplifiée
        deces = read_bronze_table(spark, "deces")
        
        fact_deces = deces.select(
            lit(None).cast(StringType()).alias("patient_sk"),
            lit(None).cast(StringType()).alias("etablissement_sk"),
            col("_sk").alias("deces_nk"),
            
            to_date(col("date_deces")).alias("date_deces"),
            year(col("date_deces")).alias("annee_deces"),  # Pour partitionnement
            month(col("date_deces")).alias("mois_deces"),
            
            (year(to_date(col("date_deces"))) - year(to_date(col("date_naissance")))).alias("age_deces"),
            
            lit(1).alias("nb_deces"),
            col("sexe"),
            
            # Géographie pour analyse régionale
            when(col("code_lieu_deces").isNotNull(), 
                 substring(col("code_lieu_deces"), 1, 2)).alias("departement_deces"),
            
            current_timestamp().alias("silver_created_at")
        )
        
        deces_count = fact_deces.count()
        print(f"  ✅ Fact Décès: {deces_count:,} décès")
        facts["fact_deces"] = fact_deces
        
        # Fact Satisfaction (optionnel - seulement si les données existent)
        try:
            satisfaction_2019 = read_bronze_table(spark, "satisfaction_48h_2019")
            
            fact_satisfaction = satisfaction_2019.select(
                col("_sk_etablissement").alias("etablissement_sk"),
                col("identifiant_organisation").alias("etablissement_nk"),
                col("region"),
                lit(2019).alias("annee_enquete"),
                col("participation"),
                col("nb_rep_score_all_rea_ajust").alias("nb_repondants"),
                regexp_replace(col("score_all_rea_ajust"), ",", ".").cast("double").alias("score_satisfaction_global"),
                regexp_replace(col("score_accueil_rea_ajust"), ",", ".").cast("double").alias("score_accueil"),
                regexp_replace(col("score_PECinf_rea_ajust"), ",", ".").cast("double").alias("score_soins_infirmiers"),
                regexp_replace(col("score_PECmed_rea_ajust"), ",", ".").cast("double").alias("score_soins_medicaux"),
                current_timestamp().alias("silver_created_at")
            )
            
            satisfaction_count = fact_satisfaction.count()
            print(f"  ✅ Fact Satisfaction: {satisfaction_count:,} enregistrements")
            facts["fact_satisfaction"] = fact_satisfaction
            
        except Exception as sat_error:
            print(f"  ⚠️  Impossible de créer le fait Satisfaction: {sat_error}")
            # Créer un fait satisfaction vide
            fact_satisfaction = spark.createDataFrame([], StructType([
                StructField("etablissement_sk", StringType(), True),
                StructField("etablissement_nk", StringType(), True),
                StructField("region", StringType(), True),
                StructField("annee_enquete", IntegerType(), True),
                StructField("participation", StringType(), True),
                StructField("nb_repondants", IntegerType(), True),
                StructField("score_satisfaction_global", DoubleType(), True),
                StructField("score_accueil", DoubleType(), True),
                StructField("score_soins_infirmiers", DoubleType(), True),
                StructField("score_soins_medicaux", DoubleType(), True),
                StructField("silver_created_at", TimestampType(), True)
            ]))
            facts["fact_satisfaction"] = fact_satisfaction
        
        return facts
        
    except Exception as e:
        print(f"❌ Erreur lors de la création des faits: {e}")
        # Nettoyer les DataFrames en cas d'erreur
        for df in facts.values():
            try:
                df.unpersist()
            except:
                pass
        raise

def create_business_marts(spark, dimensions, facts):
    """Crée les marts business pour répondre aux besoins Gold."""
    print("\n📈 CRÉATION DES MARTS BUSINESS")
    
    marts = {}
    
    try:
        fact_consultation = facts["fact_consultation"]
        fact_hospitalisation = facts["fact_hospitalisation"]
        fact_deces = facts["fact_deces"]
        fact_satisfaction = facts["fact_satisfaction"]
        dim_patient = dimensions["dim_patient"]
        dim_etablissement = dimensions["dim_etablissement"]
        dim_diagnostic = dimensions["dim_diagnostic"]
        dim_professionnel = dimensions["dim_professionnel"]
        
        # Mart 1: Taux de consultation des patients dans un établissement X sur une période de temps Y
        print("  🏥 Création du mart consultation établissement...")
        mart_taux_consultation_etablissement = fact_consultation.alias("fc") \
            .join(dim_etablissement.alias("e"), "etablissement_sk") \
            .groupBy("e.etablissement_nk", "e.nom_etablissement", "e.region_normalisee", 
                    "fc.annee_consultation", "fc.mois_consultation") \
            .agg(
                count("fc.consultation_nk").alias("nb_consultations"),
                countDistinct("fc.patient_sk").alias("nb_patients_uniques"),
                (count("fc.consultation_nk") / countDistinct("fc.patient_sk")).alias("taux_consultation_patient")
            )
        
        marts["mart_taux_consultation_etablissement"] = mart_taux_consultation_etablissement
        print("  ✅ Mart Taux Consultation Établissement créé")
        
        # Mart 2: Taux de consultation des patients par rapport à un diagnostic X sur une période de temps Y
        print("  🩺 Création du mart consultation diagnostic...")
        mart_taux_consultation_diagnostic = fact_consultation.alias("fc") \
            .join(dim_diagnostic.alias("d"), "diagnostic_sk") \
            .groupBy("d.diagnostic_nk", "d.libelle_diagnostic", "d.categorie_diagnostic",
                    "fc.annee_consultation", "fc.mois_consultation") \
            .agg(
                count("fc.consultation_nk").alias("nb_consultations"),
                countDistinct("fc.patient_sk").alias("nb_patients_uniques"),
                (count("fc.consultation_nk") / countDistinct("fc.patient_sk")).alias("taux_consultation_diagnostic")
            )
        
        marts["mart_taux_consultation_diagnostic"] = mart_taux_consultation_diagnostic
        print("  ✅ Mart Taux Consultation Diagnostic créé")
        
        # Mart 3: Taux global d'hospitalisation des patients dans une période donnée Y
        print("  🏨 Création du mart hospitalisation global...")
        mart_taux_hospitalisation_global = fact_hospitalisation.alias("fh") \
            .groupBy("fh.annee_entree", "fh.mois_entree") \
            .agg(
                count("fh.hospitalisation_nk").alias("nb_hospitalisations"),
                countDistinct("fh.patient_sk").alias("nb_patients_uniques"),
                (count("fh.hospitalisation_nk") / countDistinct("fh.patient_sk")).alias("taux_hospitalisation_global")
            )
        
        marts["mart_taux_hospitalisation_global"] = mart_taux_hospitalisation_global
        print("  ✅ Mart Taux Hospitalisation Global créé")
        
        # Mart 4: Taux d'hospitalisation des patients par rapport à des diagnostics sur une période donnée
        print("  💊 Création du mart hospitalisation diagnostic...")
        mart_taux_hospitalisation_diagnostic = fact_hospitalisation.alias("fh") \
            .join(dim_diagnostic.alias("d"), "diagnostic_sk") \
            .groupBy("d.diagnostic_nk", "d.libelle_diagnostic", "d.categorie_diagnostic",
                    "fh.annee_entree", "fh.mois_entree") \
            .agg(
                count("fh.hospitalisation_nk").alias("nb_hospitalisations"),
                countDistinct("fh.patient_sk").alias("nb_patients_uniques"),
                avg("fh.duree_sejour").alias("duree_sejour_moyenne"),
                (count("fh.hospitalisation_nk") / countDistinct("fh.patient_sk")).alias("taux_hospitalisation_diagnostic")
            )
        
        marts["mart_taux_hospitalisation_diagnostic"] = mart_taux_hospitalisation_diagnostic
        print("  ✅ Mart Taux Hospitalisation Diagnostic créé")
        
        # Mart 5: Taux d'hospitalisation par sexe, par âge
        print("  👥 Création du mart hospitalisation démographie...")
        mart_taux_hospitalisation_demographie = fact_hospitalisation.alias("fh") \
            .join(dim_patient.alias("p"), "patient_sk") \
            .groupBy("p.sexe", "p.tranche_age", "fh.annee_entree") \
            .agg(
                count("fh.hospitalisation_nk").alias("nb_hospitalisations"),
                countDistinct("fh.patient_sk").alias("nb_patients_uniques"),
                avg("fh.duree_sejour").alias("duree_sejour_moyenne"),
                (count("fh.hospitalisation_nk") / countDistinct("fh.patient_sk")).alias("taux_hospitalisation_demographique")
            )
        
        marts["mart_taux_hospitalisation_demographie"] = mart_taux_hospitalisation_demographie
        print("  ✅ Mart Taux Hospitalisation Démographie créé")
        
        # Mart 6: Taux de consultation par professionnel
        print("  👨‍⚕️ Création du mart consultation professionnel...")
        mart_taux_consultation_professionnel = fact_consultation.alias("fc") \
            .join(dim_professionnel.alias("p"), col("fc.professionnel_nk") == col("p.professionnel_nk"), "left") \
            .groupBy("p.professionnel_nk", "p.nom", "p.prenom", "p.profession", "p.categorie_specialite",
                    "fc.annee_consultation", "fc.mois_consultation") \
            .agg(
                count("fc.consultation_nk").alias("nb_consultations"),
                countDistinct("fc.patient_sk").alias("nb_patients_uniques"),
                (count("fc.consultation_nk") / countDistinct("fc.patient_sk")).alias("taux_consultation_professionnel")
            )
        
        marts["mart_taux_consultation_professionnel"] = mart_taux_consultation_professionnel
        print("  ✅ Mart Taux Consultation Professionnel créé")
        
        # Mart 7: Nombre de décès par localisation (région) et sur l'année 2019
        print("  📊 Création du mart décès localisation...")
        mart_deces_localisation = fact_deces.alias("fd") \
            .filter(col("annee_deces") == 2019) \
            .groupBy("fd.departement_deces", "fd.annee_deces") \
            .agg(
                count("fd.deces_nk").alias("nb_deces"),
                countDistinct("fd.sexe").alias("nb_sexes_distincts"),
                avg("fd.age_deces").alias("age_moyen_deces")
            ).join(
                dim_etablissement.select("departement_code", "region_normalisee").distinct(),
                col("fd.departement_deces") == col("departement_code"),
                "left"
            ).groupBy("region_normalisee", "fd.annee_deces") \
            .agg(
                sum("nb_deces").alias("nb_deces_region"),
                avg("age_moyen_deces").alias("age_moyen_deces_region")
            )
        
        marts["mart_deces_localisation"] = mart_deces_localisation
        print("  ✅ Mart Décès Localisation créé")
        
        # Mart 8: Taux global de satisfaction par région sur l'année 2020
        print("  ⭐ Création du mart satisfaction région...")
        # Note: Utilisation des données 2019 comme proxy pour 2020
        mart_satisfaction_region = fact_satisfaction.alias("fs") \
            .filter(col("annee_enquete") == 2019) \
            .groupBy("fs.region", "fs.annee_enquete") \
            .agg(
                avg("fs.score_satisfaction_global").alias("score_satisfaction_moyen_region"),
                avg("fs.score_accueil").alias("score_accueil_moyen_region"),
                avg("fs.score_soins_infirmiers").alias("score_soins_infirmiers_moyen_region"),
                sum("fs.nb_repondants").alias("total_repondants_region"),
                count("fs.etablissement_sk").alias("nb_etablissements_enquetes")
            )
        
        marts["mart_satisfaction_region"] = mart_satisfaction_region
        print("  ✅ Mart Satisfaction Région créé")
        
        return marts
        
    except Exception as e:
        print(f"❌ Erreur lors de la création des marts: {e}")
        # Nettoyer les DataFrames en cas d'erreur
        for df in marts.values():
            try:
                df.unpersist()
            except:
                pass
        raise

def write_silver_for_gold(spark, tables):
    """Écrit les tables Silver optimisées pour Gold avec partitionnement."""
    print("\n💾 ÉCRITURE DES DONNÉES SILVER POUR GOLD AVEC PARTITIONNEMENT")
    
    for table_name, df in tables.items():
        try:
            silver_path = f"s3a://{MINIO_CONFIG['bucket_silver']}/{table_name}"
            
            # Vérifier si cette table doit être partitionnée
            partition_columns = PARTITIONING_CONFIG.get(table_name)
            
            if partition_columns:
                # Vérifier que les colonnes de partitionnement existent
                missing_columns = [col for col in partition_columns if col not in df.columns]
                if missing_columns:
                    print(f"  ⚠️  Colonnes de partitionnement manquantes pour {table_name}: {missing_columns}")
                    print(f"     Écriture sans partitionnement")
                    # Écriture sans partitionnement
                    df.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(silver_path)
                else:
                    # ÉCRITURE AVEC PARTITIONNEMENT
                    print(f"  🗂️  Écriture partitionnée de {table_name} par {partition_columns}")
                    df.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .partitionBy(*partition_columns) \
                        .parquet(silver_path)
            else:
                # Écriture sans partitionnement pour les tables non configurées
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(silver_path)
            
            # Compter les lignes avec gestion d'erreur
            try:
                row_count = df.count()
                print(f"  ✅ {table_name}: {row_count:,} lignes écrites")
                print(f"     📊 Schema: {len(df.columns)} colonnes")
            except Exception as count_error:
                print(f"  ✅ {table_name}: écriture terminée (comptage échoué: {count_error})")
                print(f"     📊 Schema: {len(df.columns)} colonnes")
            
        except Exception as e:
            print(f"  ❌ Erreur écriture {table_name}: {e}")

def generate_gold_readiness_report(tables, marts):
    """Génère un rapport de préparation pour Gold."""
    print("\n" + "="*80)
    print("📋 RAPPORT DE PRÉPARATION POUR GOLD LAYER")
    print("="*80)
    
    total_tables = len(tables)
    total_marts = len(marts)
    
    print(f"""
🎯 ÉTAT DE PRÉPARATION SILVER → GOLD:

📊 VOLUME DE DONNÉES:
├── Tables Silver créées: {total_tables} (Dimensions + Faits)
├── Marts Business créés: {total_marts}
└── Structure prête pour l'agrégation Gold

🏗️  STRUCTURE POUR GOLD:
✅ Dimensions conformées (Patient, Établissement, Professionnel, Diagnostic, Temps)
✅ Faits normalisés avec clés naturelles
✅ Marts business pré-calculés pour les KPI
✅ Schémas optimisés pour l'agrégation
✅ Métadonnées de traçabilité
✅ PARTITIONNEMENT POUR PERFORMANCE

📈 MARTS BUSINESS CRÉÉS POUR LES KPI GOLD:

1. 🏥 Taux de consultation par établissement et période
2. 🩺 Taux de consultation par diagnostic et période  
3. 🏨 Taux global d'hospitalisation par période
4. 💊 Taux d'hospitalisation par diagnostic et période
5. 👥 Taux d'hospitalisation par sexe et âge
6. 👨‍⚕️ Taux de consultation par professionnel
7. 📊 Décès par localisation (2019)
8. ⭐ Satisfaction par région (2019)

🔜 PRÊT POUR LES REQUÊTES GOLD:

Les données Silver sont maintenant optimisées pour:
• Requêtes analytiques performantes
• Agrégations complexes
• Analyses temporelles
• Segmentation multi-dimensionnelle

Prochaine étape: Exécuter le pipeline Gold pour générer les KPI avancés.
    """)

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                SILVER → GOLD READY PIPELINE                 ║
    ║     Préparation optimale des données pour la couche Gold    ║
    ║           AVEC MARTS BUSINESS ET PARTITIONNEMENT            ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    spark = None
    try:
        spark = get_spark_session()
        
        # 1. Création des dimensions conformées
        dimensions = create_conformed_dimensions(spark)
        
        # 2. Création des faits pour Gold
        facts = create_gold_ready_facts(spark, dimensions)
        
        # 3. Création des marts business
        marts = create_business_marts(spark, dimensions, facts)
        
        # 4. Regroupement de toutes les tables
        all_tables = {**dimensions, **facts, **marts}
        
        # 5. Écriture optimisée AVEC PARTITIONNEMENT
        write_silver_for_gold(spark, all_tables)
        
        # 6. Rapport de préparation
        generate_gold_readiness_report({**dimensions, **facts}, marts)
        
        print("\n✅ Pipeline Silver terminé - Prêt pour Gold!")
        print("📊 Marts business créés pour répondre aux KPI demandés")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # S'assurer que Spark est bien arrêté à la fin
        if spark:
            try:
                spark.stop()
                print("🔴 Spark session arrêtée")
            except:
                pass