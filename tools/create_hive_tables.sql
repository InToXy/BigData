-- Création des schémas et tables Hive pour accès via Trino
-- Les tables pointent vers les données Parquet dans MinIO

-- Créer les schémas
CREATE SCHEMA IF NOT EXISTS hive.bronze;
CREATE SCHEMA IF NOT EXISTS hive.silver;
CREATE SCHEMA IF NOT EXISTS hive.gold;

-- ============================================================
-- TABLES GOLD (pour Superset)
-- ============================================================

-- KPI: Décès par année/sexe/âge
CREATE TABLE IF NOT EXISTS hive.gold.kpi_deces_par_annee (
    annee_deces INT,
    sexe VARCHAR,
    categorie_age VARCHAR,
    nombre_deces BIGINT,
    age_moyen DOUBLE,
    age_min DOUBLE,
    age_max DOUBLE,
    age_ecart_type DOUBLE,
    pourcentage_annee DOUBLE,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_deces_par_annee/'
);

-- KPI: Décès par région
CREATE TABLE IF NOT EXISTS hive.gold.kpi_deces_par_region (
    annee_deces INT,
    code_dept VARCHAR,
    nombre_deces BIGINT,
    age_moyen DOUBLE,
    nombre_deces_uniques BIGINT,
    rang_departement INT,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_deces_par_region/'
);

-- KPI: Statistiques démographiques
CREATE TABLE IF NOT EXISTS hive.gold.kpi_demographic_summary (
    annee_deces INT,
    sexe VARCHAR,
    total_deces BIGINT,
    age_moyen DOUBLE,
    age_median DOUBLE,
    age_min DOUBLE,
    age_max DOUBLE,
    age_ecart_type DOUBLE,
    age_q1 DOUBLE,
    age_q3 DOUBLE,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_demographic_summary/'
);

-- KPI: Tendances temporelles
CREATE TABLE IF NOT EXISTS hive.gold.kpi_temporal_trends (
    annee INT,
    mois INT,
    trimestre INT,
    nombre_deces BIGINT,
    age_moyen DOUBLE,
    deces_uniques BIGINT,
    annee_mois VARCHAR,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_temporal_trends/'
);

-- KPI: Top départements
CREATE TABLE IF NOT EXISTS hive.gold.kpi_top_departements (
    annee_deces INT,
    code_dept VARCHAR,
    nombre_deces BIGINT,
    age_moyen DOUBLE,
    rang_departement INT
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_top_departements/'
);

-- KPI: Distribution par âge
CREATE TABLE IF NOT EXISTS hive.gold.kpi_distribution_age (
    annee_deces INT,
    categorie_age VARCHAR,
    nombre_deces BIGINT,
    pourcentage DOUBLE,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_distribution_age/'
);

-- KPI: Synthèse globale
CREATE TABLE IF NOT EXISTS hive.gold.kpi_synthese_globale (
    annee_deces INT,
    total_deces BIGINT,
    age_moyen_global DOUBLE,
    age_median_global DOUBLE,
    nombre_lieux_deces BIGINT,
    total_hommes BIGINT,
    total_femmes BIGINT,
    ratio_hommes_femmes DOUBLE,
    _gold_batch_id VARCHAR,
    _gold_load_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://gold/kpi_synthese_globale/'
);

-- ============================================================
-- TABLES SILVER (dimensions et faits)
-- ============================================================

-- Dimension Temps
CREATE TABLE IF NOT EXISTS hive.silver.dim_temps (
    date_id BIGINT,
    date_deces DATE,
    annee INT,
    mois INT,
    jour INT,
    trimestre INT
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://silver/dim_temps/'
);

-- Dimension Géographie
CREATE TABLE IF NOT EXISTS hive.silver.dim_geographie (
    geo_id BIGINT,
    code_lieu VARCHAR,
    code_dept VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://silver/dim_geographie/'
);

-- Fait Décès
CREATE TABLE IF NOT EXISTS hive.silver.fait_deces (
    deces_id BIGINT,
    date_deces DATE,
    sexe VARCHAR,
    date_naissance DATE,
    code_lieu_deces VARCHAR,
    annee_deces INT,
    age_deces BIGINT,
    categorie_age VARCHAR,
    date_id BIGINT,
    geo_id BIGINT
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://silver/fait_deces/'
);
