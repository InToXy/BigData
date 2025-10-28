-- ============================================================
-- setup_trino_gold.sql
-- Création du schéma et des tables Trino pour accéder au Gold
-- ============================================================

-- 1. Créer le schéma pour le Gold
CREATE SCHEMA IF NOT EXISTS hive.chu_gold
WITH (location = 's3a://gold/');

-- 2. Utiliser le schéma
USE hive.chu_gold;

-- ============================================================
-- TABLES GOLD - KPIs
-- ============================================================

-- KPI 1: Taux de consultation
CREATE TABLE IF NOT EXISTS kpi_consultation_rate (
    annee INTEGER,
    mois INTEGER,
    nb_consultations BIGINT,
    nb_patients_uniques BIGINT,
    montant_total DOUBLE,
    montant_moyen DOUBLE,
    duree_moyenne_minutes DOUBLE,
    taux_consultation_patient DOUBLE,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_consultation_rate/',
    format = 'PARQUET'
);

-- KPI 2: Métriques hospitalisation
CREATE TABLE IF NOT EXISTS kpi_hospitalisation_metrics (
    annee INTEGER,
    nb_hospitalisations BIGINT,
    nb_patients_hospitalises BIGINT,
    duree_moyenne_sejour DOUBLE,
    duree_min_sejour INTEGER,
    duree_max_sejour INTEGER,
    taux_hospit_patient DOUBLE,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_hospitalisation_metrics/',
    format = 'PARQUET'
);

-- KPI 3: Décès par région
CREATE TABLE IF NOT EXISTS kpi_deces_by_region (
    annee INTEGER,
    lieu_deces VARCHAR,
    sexe VARCHAR,
    nb_deces BIGINT,
    age_moyen_deces DOUBLE,
    age_min_deces INTEGER,
    age_max_deces INTEGER,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_deces_by_region/',
    format = 'PARQUET'
);

-- KPI 4: Satisfaction globale
CREATE TABLE IF NOT EXISTS kpi_satisfaction_global (
    source_enquete VARCHAR,
    nb_reponses_enquete BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_satisfaction_global/',
    format = 'PARQUET'
);

-- KPI 5: Activité mensuelle
CREATE TABLE IF NOT EXISTS kpi_activite_mensuelle (
    annee INTEGER,
    mois INTEGER,
    nb_consultations BIGINT,
    nb_hospitalisations BIGINT,
    activite_totale BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_activite_mensuelle/',
    format = 'PARQUET'
);

-- KPI 6: Démographie patients
CREATE TABLE IF NOT EXISTS kpi_patient_demographics (
    tranche_age VARCHAR,
    sexe VARCHAR,
    nb_patients BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_patient_demographics/',
    format = 'PARQUET'
);

-- KPI 7: Performance établissements
CREATE TABLE IF NOT EXISTS kpi_etablissement_performance (
    region VARCHAR,
    type_etablissement VARCHAR,
    nb_etablissements BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_etablissement_performance/',
    format = 'PARQUET'
);

-- KPI 8: Tendances temporelles
CREATE TABLE IF NOT EXISTS kpi_temporal_trends (
    annee INTEGER,
    trimestre INTEGER,
    type_activite VARCHAR,
    volume BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_temporal_trends/',
    format = 'PARQUET'
);

-- ============================================================
-- VUES UTILES
-- ============================================================

-- Vue: Consultations récentes (12 derniers mois)
CREATE OR REPLACE VIEW v_consultations_recentes AS
SELECT *
FROM kpi_consultation_rate
WHERE annee >= YEAR(CURRENT_DATE) - 1
ORDER BY annee DESC, mois DESC;

-- Vue: Activité totale par année
CREATE OR REPLACE VIEW v_activite_annuelle AS
SELECT 
    annee,
    SUM(nb_consultations) as total_consultations,
    SUM(nb_hospitalisations) as total_hospitalisations,
    SUM(activite_totale) as activite_globale
FROM kpi_activite_mensuelle
GROUP BY annee
ORDER BY annee DESC;

-- Vue: Dashboard exécutif (top KPIs)
CREATE OR REPLACE VIEW v_dashboard_executif AS
SELECT
    'Consultations Année Courante' as indicateur,
    CAST(SUM(nb_consultations) AS VARCHAR) as valeur,
    'consultations' as unite
FROM kpi_consultation_rate
WHERE annee = YEAR(CURRENT_DATE)
UNION ALL
SELECT
    'Hospitalisations Année Courante',
    CAST(SUM(nb_hospitalisations) AS VARCHAR),
    'hospitalisations'
FROM kpi_hospitalisation_metrics
WHERE annee = YEAR(CURRENT_DATE)
UNION ALL
SELECT
    'Patients Uniques',
    CAST(COUNT(DISTINCT nb_patients_uniques) AS VARCHAR),
    'patients'
FROM kpi_patient_demographics;

-- ============================================================
-- REQUÊTES UTILES POUR VALIDATION
-- ============================================================

-- Afficher toutes les tables créées
SHOW TABLES;

-- Compter les lignes dans chaque KPI
SELECT 'kpi_consultation_rate' as table_name, COUNT(*) as row_count FROM kpi_consultation_rate
UNION ALL
SELECT 'kpi_hospitalisation_metrics', COUNT(*) FROM kpi_hospitalisation_metrics
UNION ALL
SELECT 'kpi_deces_by_region', COUNT(*) FROM kpi_deces_by_region
UNION ALL
SELECT 'kpi_satisfaction_global', COUNT(*) FROM kpi_satisfaction_global
UNION ALL
SELECT 'kpi_activite_mensuelle', COUNT(*) FROM kpi_activite_mensuelle
UNION ALL
SELECT 'kpi_patient_demographics', COUNT(*) FROM kpi_patient_demographics
UNION ALL
SELECT 'kpi_etablissement_performance', COUNT(*) FROM kpi_etablissement_performance
UNION ALL
SELECT 'kpi_temporal_trends', COUNT(*) FROM kpi_temporal_trends;
