# ============================================================
  # ÉTAPE 1: Créer les schémas Bronze, Silver, Gold
  # ============================================================
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.bronze WITH (location =
  's3a://bronze/')"
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.silver WITH (location =
  's3a://silver/')"
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.gold WITH (location = 's3a://gold/')"

  # Vérifier que les schémas sont créés
  docker exec chu_trino trino --execute "SHOW SCHEMAS FROM parquet"

  # ============================================================
  # ÉTAPE 2: Copier et exécuter le script SQL
  # ============================================================
  cd ../tmp
  docker cp create_all_gold_tables.sql chu_trino:/tmp/
  docker exec chu_trino bash -c "trino < /tmp/create_all_gold_tables.sql"

  # ============================================================
  # ÉTAPE 3: Recréer les tables partitionnées correctement
  # ============================================================

  # 3.1 - fact_consultation (partitionnée par année et mois)
  docker exec chu_trino trino --execute "DROP TABLE IF EXISTS parquet.gold.fact_consultation"

  docker exec chu_trino trino --execute "CREATE TABLE parquet.gold.fact_consultation (
      date_consultation_fk INTEGER,
      patient_fk VARCHAR,
      diagnostic_fk VARCHAR,
      professionnel_fk VARCHAR,
      etablissement_fk VARCHAR,
      date_consultation DATE,
      region VARCHAR,
      sexe VARCHAR,
      categorie_age VARCHAR,
      saison_consultation VARCHAR,
      periode_journee VARCHAR,
      duree_heures DOUBLE,
      est_consultation_longue INTEGER,
      nb_consultations INTEGER,
      consultation_annee INTEGER,
      consultation_mois INTEGER
  )
  WITH (
      external_location = 's3a://gold/fact_consultation',
      format = 'PARQUET',
      partitioned_by = ARRAY['consultation_annee', 'consultation_mois']
  )"

  # Synchroniser les partitions
  docker exec chu_trino trino --execute "CALL parquet.system.sync_partition_metadata('gold', 'fact_consultation',
  'FULL')"

  # 3.2 - mart_diagnostic_epidemio (partitionnée par année)
  docker exec chu_trino trino --execute "DROP TABLE IF EXISTS parquet.gold.mart_diagnostic_epidemio"

  docker exec chu_trino trino --execute "CREATE TABLE parquet.gold.mart_diagnostic_epidemio (
      trimestre INTEGER,
      code_diag VARCHAR,
      diagnostic VARCHAR,
      type_pathologie VARCHAR,
      gravite_pathologie VARCHAR,
      periode_annee VARCHAR,
      nb_consultations BIGINT,
      nb_patients_consultes BIGINT,
      nb_hospitalisations BIGINT,
      nb_patients_hospitalises BIGINT,
      duree_moyenne_sejour_diag DOUBLE,
      total_consultations_periode BIGINT,
      total_hospitalisations_periode BIGINT,
      taux_consultation_diagnostic_pct DOUBLE,
      taux_hospitalisation_diagnostic_pct DOUBLE,
      annee INTEGER
  )
  WITH (
      external_location = 's3a://gold/mart_diagnostic_epidemio',
      format = 'PARQUET',
      partitioned_by = ARRAY['annee']
  )"

  # Synchroniser les partitions
  docker exec chu_trino trino --execute "CALL parquet.system.sync_partition_metadata('gold',
  'mart_diagnostic_epidemio', 'FULL')"

  # ============================================================
  # ÉTAPE 4: Vérification
  # ============================================================

  # Lister toutes les tables
  docker exec chu_trino trino --execute "SHOW TABLES FROM parquet.gold"

  # Vérifier les comptages
  docker exec chu_trino trino --execute "
  SELECT table_name, CAST(row_count AS VARCHAR) || ' lignes' as rows
  FROM (
      SELECT 'dim_diagnostic' as table_name, COUNT(*) as row_count FROM parquet.gold.dim_diagnostic
      UNION ALL SELECT 'dim_etablissement', COUNT(*) FROM parquet.gold.dim_etablissement
      UNION ALL SELECT 'dim_localisation', COUNT(*) FROM parquet.gold.dim_localisation
      UNION ALL SELECT 'dim_patient', COUNT(*) FROM parquet.gold.dim_patient
      UNION ALL SELECT 'dim_professionnel', COUNT(*) FROM parquet.gold.dim_professionnel
      UNION ALL SELECT 'fact_consultation', COUNT(*) FROM parquet.gold.fact_consultation
      UNION ALL SELECT 'fact_deces', COUNT(*) FROM parquet.gold.fact_deces
      UNION ALL SELECT 'mart_deces_localisation_2019', COUNT(*) FROM parquet.gold.mart_deces_localisation_2019
      UNION ALL SELECT 'mart_demographie', COUNT(*) FROM parquet.gold.mart_demographie
      UNION ALL SELECT 'mart_diagnostic_epidemio', COUNT(*) FROM parquet.gold.mart_diagnostic_epidemio
      UNION ALL SELECT 'mart_professionnel', COUNT(*) FROM parquet.gold.mart_professionnel
      UNION ALL SELECT 'mart_satisfaction_region_2020', COUNT(*) FROM parquet.gold.mart_satisfaction_region_2020
  )
  ORDER BY table_name
  "

  📝 Version script bash (à sauvegarder dans setup_trino_tables.sh) :

  #!/bin/bash
  # Script de création des tables Trino Gold

  set -e

  echo "🔧 Création des schémas Trino..."
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.bronze WITH (location =
  's3a://bronze/')"
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.silver WITH (location =
  's3a://silver/')"
  docker exec chu_trino trino --execute "CREATE SCHEMA IF NOT EXISTS parquet.gold WITH (location = 's3a://gold/')"

  echo "✅ Schémas créés"
  echo ""

  echo "📊 Exécution du script SQL..."
  docker cp spark_jobs/tmp/create_all_gold_tables.sql chu_trino:/tmp/
  docker exec chu_trino bash -c "trino < /tmp/create_all_gold_tables.sql" 2>&1 | grep -E "CREATE TABLE|failed"

  echo "✅ Tables créées"
  echo ""

  echo "🔄 Correction des tables partitionnées..."

  # fact_consultation
  echo "  - fact_consultation..."
  docker exec chu_trino trino --execute "DROP TABLE IF EXISTS parquet.gold.fact_consultation" > /dev/null 2>&1
  docker exec chu_trino trino --execute "CREATE TABLE parquet.gold.fact_consultation (
      date_consultation_fk INTEGER,
      patient_fk VARCHAR,
      diagnostic_fk VARCHAR,
      professionnel_fk VARCHAR,
      etablissement_fk VARCHAR,
      date_consultation DATE,
      region VARCHAR,
      sexe VARCHAR,
      categorie_age VARCHAR,
      saison_consultation VARCHAR,
      periode_journee VARCHAR,
      duree_heures DOUBLE,
      est_consultation_longue INTEGER,
      nb_consultations INTEGER,
      consultation_annee INTEGER,
      consultation_mois INTEGER
  )
  WITH (
      external_location = 's3a://gold/fact_consultation',
      format = 'PARQUET',
      partitioned_by = ARRAY['consultation_annee', 'consultation_mois']
  )" > /dev/null 2>&1

  docker exec chu_trino trino --execute "CALL parquet.system.sync_partition_metadata('gold', 'fact_consultation',
  'FULL')" > /dev/null 2>&1

  # mart_diagnostic_epidemio
  echo "  - mart_diagnostic_epidemio..."
  docker exec chu_trino trino --execute "DROP TABLE IF EXISTS parquet.gold.mart_diagnostic_epidemio" > /dev/null
  2>&1
  docker exec chu_trino trino --execute "CREATE TABLE parquet.gold.mart_diagnostic_epidemio (
      trimestre INTEGER,
      code_diag VARCHAR,
      diagnostic VARCHAR,
      type_pathologie VARCHAR,
      gravite_pathologie VARCHAR,
      periode_annee VARCHAR,
      nb_consultations BIGINT,
      nb_patients_consultes BIGINT,
      nb_hospitalisations BIGINT,
      nb_patients_hospitalises BIGINT,
      duree_moyenne_sejour_diag DOUBLE,
      total_consultations_periode BIGINT,
      total_hospitalisations_periode BIGINT,
      taux_consultation_diagnostic_pct DOUBLE,
      taux_hospitalisation_diagnostic_pct DOUBLE,
      annee INTEGER
  )
  WITH (
      external_location = 's3a://gold/mart_diagnostic_epidemio',
      format = 'PARQUET',
      partitioned_by = ARRAY['annee']
  )" > /dev/null 2>&1

  docker exec chu_trino trino --execute "CALL parquet.system.sync_partition_metadata('gold',
  'mart_diagnostic_epidemio', 'FULL')" > /dev/null 2>&1

  echo "✅ Tables partitionnées corrigées"
  echo ""

  echo "📊 Vérification finale..."
  docker exec chu_trino trino --execute "
  SELECT table_name, CAST(row_count AS VARCHAR) || ' lignes' as rows
  FROM (
      SELECT 'dim_patient' as table_name, COUNT(*) as row_count FROM parquet.gold.dim_patient
      UNION ALL SELECT 'fact_consultation', COUNT(*) FROM parquet.gold.fact_consultation
      UNION ALL SELECT 'fact_deces', COUNT(*) FROM parquet.gold.fact_deces
      UNION ALL SELECT 'mart_diagnostic_epidemio', COUNT(*) FROM parquet.gold.mart_diagnostic_epidemio
  )
  ORDER BY table_name
  " 2>&1 | grep -v WARNING

  echo ""
  echo "✅ Configuration Trino terminée !"
  echo ""
  echo "🔗 URI Superset: trino://admin@trino:8080/parquet/gold"