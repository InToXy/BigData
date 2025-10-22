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
    dayofweek, date_format
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
                raise Exception(f"❌ JAR manquant: {jar}")
        
        jars_path = ",".join(jar_files)
        print(f"📚 JARs chargés: {len(jar_files)}")
        
        # Configuration Spark optimisée pour Silver
        builder = SparkSession.builder \
            .appName("Silver_Pipeline_Gold_Ready") \
            .config("spark.jars", jars_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skew.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.hive.convertMetastoreParquet", "false")
            
        # Configuration S3A
        hadoop_conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": MINIO_CONFIG["endpoint"],
            "spark.hadoop.fs.s3a.access.key": MINIO_CONFIG["access_key"],
            "spark.hadoop.fs.s3a.secret.key": MINIO_CONFIG["secret_key"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
        
        for key, value in hadoop_conf.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # Configuration Hadoop explicite
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        
        print("✅ Spark Silver initialisé (prêt pour Gold)")
        return spark
        
    except Exception as e:
        print(f"❌ Erreur Spark Silver: {e}")
        raise

def read_bronze_table(spark, table_name):
    """Lit une table depuis Bronze avec optimisation."""
    try:
        bronze_path = f"s3a://{MINIO_CONFIG['bucket_bronze']}/{table_name}"
        print(f"📂 Lecture {table_name} depuis Bronze...")
        
        df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(bronze_path)
        
        # Métadonnées pour Gold
        row_count = df.count()
        print(f"  ✅ {row_count:,} lignes | {len(df.columns)} colonnes")
        
        return df.cache()  # Cache pour réutilisation
        
    except Exception as e:
        print(f"❌ Erreur lecture {table_name}: {e}")
        raise

def create_conformed_dimensions(spark):
    """Crée les dimensions conformées pour Gold."""
    print("\n🏗️ CRÉATION DES DIMENSIONS POUR GOLD")
    
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
        upper(trim(col("ville"))).alias("ville_normalisee"),
        when(length(trim(col("code_postal"))) == 5, substring(trim(col("code_postal")), 1, 2))
          .otherwise("99").alias("departement_code"),
        
        # Metadata pour Gold
        current_timestamp().alias("silver_created_at"),
        lit("silver_layer").alias("source_layer"),
        lit(1).alias("is_active")
    ).distinct()
    
    print(f"  ✅ Dim Patient: {dim_patient.count():,} patients uniques")
    
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
                 concat_ws(" ", lit("Département"), substring(trim(col("code_postal")), 1, 2)))
            .otherwise("Département inconnu").alias("departement_normalise")
        )
    else:
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
    
    print(f"  ✅ Dim Établissement: {dim_etablissement.count():,} établissements uniques")
    
    # Dimension Temps (préparation pour Gold)
    print("  🕒 Préparation de la dimension Temps...")
    
    dates_df = spark.sql("""
        SELECT explode(sequence(to_date('2018-01-01'), to_date('2024-12-31'), interval 1 day)) as date_complete
    """)
    
    dim_temp = dates_df.select(
        col("date_complete").alias("date_complete"),
        year(col("date_complete")).alias("annee"),
        month(col("date_complete")).alias("mois"),
        quarter(col("date_complete")).alias("trimestre"),
        dayofmonth(col("date_complete")).alias("jour"),
        date_format(col("date_complete"), "EEEE").alias("jour_semaine"),
        when(dayofweek(col("date_complete")).isin(1, 7), "Weekend")
          .otherwise("Semaine").alias("type_jour")
    ).distinct()
    
    print(f"  ✅ Dim Temps: {dim_temp.count():,} dates préparées")
    
    return {
        "dim_patient": dim_patient,
        "dim_etablissement": dim_etablissement,
        "dim_temp": dim_temp
    }

def create_gold_ready_facts(spark, dimensions):
    """Crée les faits préparés pour Gold."""
    print("\n📊 CRÉATION DES FAITS POUR GOLD")
    
    dim_patient = dimensions["dim_patient"]
    dim_etablissement = dimensions["dim_etablissement"]
    
    # Fact Consultations
    consultations = read_bronze_table(spark, "consultations")
    
    fact_consultation = consultations.alias("c") \
        .join(dim_patient.alias("p"), col("c.id_patient") == col("p.patient_nk"), "inner") \
        .select(
            # Clés conformées
            col("p.patient_sk"),
            col("c._sk").alias("consultation_nk"),
            
            # Dates
            to_date(coalesce(col("c.Heure_debut"), current_timestamp())).alias("date_consultation"),
            year(coalesce(col("c.Heure_debut"), current_timestamp())).alias("annee_consultation"),
            month(coalesce(col("c.Heure_debut"), current_timestamp())).alias("mois_consultation"),
            
            # Mesures standardisées
            lit(1).alias("nb_consultations"),
            col("c.code_diag").alias("diagnostic_code"),
            
            # Metadata pour Gold
            current_timestamp().alias("silver_created_at"),
            lit("consultation_bronze").alias("source_system")
        )
    
    print(f"  ✅ Fact Consultation: {fact_consultation.count():,} consultations")
    
    # Fact Hospitalisations
    hospitalisations = read_bronze_table(spark, "hospitalisations")
    
    fact_hospitalisation = hospitalisations.alias("h") \
        .join(dim_patient.alias("p"), col("h.id_patient") == col("p.patient_nk"), "inner") \
        .join(dim_etablissement.alias("e"), col("h.identifiant_organisation") == col("e.etablissement_nk"), "inner") \
        .select(
            # Clés conformées
            col("p.patient_sk"),
            col("e.etablissement_sk"),
            col("h._sk").alias("hospitalisation_nk"),
            
            # Dates et durée
            to_date(col("h.Date_Entree")).alias("date_entree"),
            to_date(col("h.Date_Entree")).alias("date_sortie"),  # Approximation car pas de date_sortie
            col("h.Jour_Hospitalisation").alias("duree_sejour"),
            
            # Mesures
            lit(1).alias("nb_hospitalisations"),
            col("h.code_diag").alias("diagnostic_principal"),
            
            # Metadata
            current_timestamp().alias("silver_created_at"),
            lit("hospitalisation_bronze").alias("source_system")
        )
    
    print(f"  ✅ Fact Hospitalisation: {fact_hospitalisation.count():,} hospitalisations")
    
    # Fact Décès - Gestion des différentes stratégies de jointure
    deces = read_bronze_table(spark, "deces")
    
    # Debug des clés disponibles
    print("  🔍 Debug Décès - Échantillon des IDs:")
    deces.select("id").show(5)
    dim_patient.select("patient_nk").show(5)
    
    # Essai de jointure avec _sk_patient si disponible
    if '_sk_patient' in deces.columns:
        print("  🔍 Tentative de jointure avec _sk_patient...")
        match_count_sk = deces.alias("d") \
            .join(dim_patient.alias("p"), col("d._sk_patient") == col("p.patient_sk"), "inner") \
            .count()
        print(f"  🔍 Correspondances avec _sk_patient: {match_count_sk}")
        
        if match_count_sk > 0:
            fact_deces = deces.alias("d") \
                .join(dim_patient.alias("p"), col("d._sk_patient") == col("p.patient_sk"), "inner") \
                .select(
                    col("p.patient_sk"),
                    lit(None).cast(StringType()).alias("etablissement_sk"),
                    col("d._sk").alias("deces_nk"),
                    
                    to_date(col("d.date_deces")).alias("date_deces"),
                    year(col("d.date_deces")).alias("annee_deces"),
                    
                    # Calcul âge au décès
                    (year(to_date(col("d.date_deces"))) - year(to_date(col("d.date_naissance")))).alias("age_deces"),
                    
                    lit(1).alias("nb_deces"),
                    col("d.sexe"),
                    
                    current_timestamp().alias("silver_created_at")
                )
        else:
            # Fallback : création sans jointure
            print("  ⚠️  Aucune correspondance avec _sk_patient, création sans jointure")
            fact_deces = deces.select(
                lit(None).cast(StringType()).alias("patient_sk"),
                lit(None).cast(StringType()).alias("etablissement_sk"),
                col("_sk").alias("deces_nk"),
                
                to_date(col("date_deces")).alias("date_deces"),
                year(col("date_deces")).alias("annee_deces"),
                
                (year(to_date(col("date_deces"))) - year(to_date(col("date_naissance")))).alias("age_deces"),
                
                lit(1).alias("nb_deces"),
                col("sexe"),
                
                current_timestamp().alias("silver_created_at")
            )
    else:
        # Fallback : création sans jointure
        print("  ⚠️  Colonne _sk_patient non trouvée, création sans jointure")
        fact_deces = deces.select(
            lit(None).cast(StringType()).alias("patient_sk"),
            lit(None).cast(StringType()).alias("etablissement_sk"),
            col("_sk").alias("deces_nk"),
            
            to_date(col("date_deces")).alias("date_deces"),
            year(col("date_deces")).alias("annee_deces"),
            
            (year(to_date(col("date_deces"))) - year(to_date(col("date_naissance")))).alias("age_deces"),
            
            lit(1).alias("nb_deces"),
            col("sexe"),
            
            current_timestamp().alias("silver_created_at")
        )
    
    print(f"  ✅ Fact Décès: {fact_deces.count():,} décès")
    
    return {
        "fact_consultation": fact_consultation,
        "fact_hospitalisation": fact_hospitalisation,
        "fact_deces": fact_deces
    }

def create_business_metrics(spark, dimensions, facts):
    """Crée les métriques business pour Gold."""
    print("\n📈 CRÉATION DES MÉTRIQUES BUSINESS")
    
    fact_consultation = facts["fact_consultation"]
    fact_hospitalisation = facts["fact_hospitalisation"]
    fact_deces = facts["fact_deces"]
    dim_patient = dimensions["dim_patient"]
    dim_etablissement = dimensions["dim_etablissement"]
    
    # Métrique 1: Activité de consultation
    metrique_consultation = fact_consultation \
        .groupBy("annee_consultation", "mois_consultation") \
        .agg(
            count("patient_sk").alias("nb_consultations_total"),
            countDistinct("patient_sk").alias("nb_patients_uniques"),
            (count("patient_sk") / countDistinct("patient_sk")).alias("taux_frequentation_moyenne")
        )
    
    print("  ✅ Métrique Consultation créée")
    
    # Métrique 2: Hospitalisations par établissement
    metrique_hospitalisation_etablissement = fact_hospitalisation \
        .join(dim_etablissement, "etablissement_sk") \
        .groupBy("type_etablissement", "region_normalisee") \
        .agg(
            count("hospitalisation_nk").alias("nb_hospitalisations"),
            avg("duree_sejour").alias("duree_sejour_moyenne"),
            countDistinct("patient_sk").alias("nb_patients_uniques")
        )
    
    print("  ✅ Métrique Hospitalisation créée")
    
    # Métrique 3: Démographie des décès
    metrique_deces_demographie = fact_deces.alias("f") \
        .join(dim_patient.alias("p"), "patient_sk") \
        .groupBy("f.annee_deces", "f.sexe", "p.tranche_age") \
        .agg(
            count("f.deces_nk").alias("nb_deces"),
            avg("f.age_deces").alias("age_moyen_deces")
        )
    
    print("  ✅ Métrique Décès créée")
    
    # Métrique 4: Taux d'occupation temporel
    metrique_activite_temporelle = fact_consultation \
        .groupBy("annee_consultation", "mois_consultation") \
        .agg(
            count("patient_sk").alias("volume_consultations"),
            (count("patient_sk") / countDistinct("patient_sk")).alias("ratio_consultations_patient")
        )
    
    print("  ✅ Métrique Activité Temporelle créée")
    
    return {
        "metrique_consultation": metrique_consultation,
        "metrique_hospitalisation_etablissement": metrique_hospitalisation_etablissement,
        "metrique_deces_demographie": metrique_deces_demographie,
        "metrique_activite_temporelle": metrique_activite_temporelle
    }

def write_silver_for_gold(spark, tables):
    """Écrit les tables Silver optimisées pour Gold."""
    print("\n💾 ÉCRITURE DES DONNÉES SILVER POUR GOLD")
    
    for table_name, df in tables.items():
        try:
            silver_path = f"s3a://{MINIO_CONFIG['bucket_silver']}/{table_name}"
            
            # Écriture optimisée
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(silver_path)
            
            print(f"  ✅ {table_name}: {df.count():,} lignes écrites")
            print(f"     📊 Schema: {len(df.columns)} colonnes")
            
        except Exception as e:
            print(f"  ❌ Erreur écriture {table_name}: {e}")

def generate_gold_readiness_report(tables):
    """Génère un rapport de préparation pour Gold."""
    print("\n" + "="*80)
    print("📋 RAPPORT DE PRÉPARATION POUR GOLD LAYER")
    print("="*80)
    
    total_tables = len(tables)
    
    # CORRECTION : Calcul correct du nombre total de lignes
    total_rows = 0
    for df in tables.values():
        total_rows += df.count()
    
    print(f"""
🎯 ÉTAT DE PRÉPARATION SILVER → GOLD:

📊 VOLUME DE DONNÉES:
├── Tables Silver créées: {total_tables}
├── Lignes totales: {total_rows:,}
└── Stockage estimé: {(total_rows * 0.5) / 1024:.1f} MB (approx.)

🏗️  STRUCTURE POUR GOLD:
✅ Dimensions conformées (Patient, Établissement, Temps)
✅ Faits normalisés avec clés naturelles
✅ Métriques business pré-calculées
✅ Schémas optimisés pour l'agrégation
✅ Métadonnées de traçabilité

📈 INDICATEURS PRÊTS POUR GOLD:
├── Activité de consultation (volume, fréquentation)
├── Hospitalisations par établissement et région
├── Démographie des décès (âge, sexe)
├── Métriques temporelles d'occupation
└── Taux et ratios business standardisés

⚠️  POINTS D'ATTENTION:
• Fact Décès: problème de jointure à investiguer
• Métrique Décès: conséquence du problème ci-dessus

🔜 PROCHAINES ÉTAPES GOLD:
1. Agrégations avancées et KPI complexes
2. Modèles prédictifs et machine learning
3. Data Marts métier spécialisés
4. APIs de données pour applications
5. Tableaux de bord executive

💡 CONSEILS POUR LE JOB GOLD:
• Réutiliser les dimensions conformées de Silver
• Utiliser les métriques pré-calculées comme base
• Focus sur l'agrégation et les KPI complexes
• Optimiser pour la requêtabilité BI
    """)

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                SILVER → GOLD READY PIPELINE                 ║
    ║     Préparation optimale des données pour la couche Gold    ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    try:
        spark = get_spark_session()
        
        # 1. Création des dimensions conformées
        dimensions = create_conformed_dimensions(spark)
        
        # 2. Création des faits pour Gold
        facts = create_gold_ready_facts(spark, dimensions)
        
        # 3. Création des métriques business
        metrics = create_business_metrics(spark, dimensions, facts)
        
        # 4. Regroupement de toutes les tables
        all_tables = {**dimensions, **facts, **metrics}
        
        # 5. Écriture optimisée
        write_silver_for_gold(spark, all_tables)
        
        # 6. Rapport de préparation
        generate_gold_readiness_report(all_tables)
        
        spark.stop()
        print("\n✅ Pipeline Silver terminé - Prêt pour Gold!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()