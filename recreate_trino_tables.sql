-- ============================================================
-- Recréation des tables Trino Gold avec les VRAIS schémas
-- ============================================================

-- KPI 3: Décès par région (basé sur gold_aggregation_clean.py ligne 166-188)
CREATE TABLE minio.default.kpi_deces_by_region (
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

-- KPI 4: Satisfaction globale (ligne 189-207)
CREATE TABLE minio.default.kpi_satisfaction_global (
    type_enquete VARCHAR,
    score_moyen DOUBLE,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_satisfaction_global/',
    format = 'PARQUET'
);

-- KPI 6: Démographie patients (ligne 265-288)
CREATE TABLE minio.default.kpi_patient_demographics (
    tranche_age VARCHAR,
    sexe VARCHAR,
    nb_patients BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_patient_demographics/',
    format = 'PARQUET'
);

-- KPI 7: Performance établissements (ligne 289-311)
CREATE TABLE minio.default.kpi_etablissement_performance (
    region VARCHAR,
    type_etablissement VARCHAR,
    nb_etablissements BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_etablissement_performance/',
    format = 'PARQUET'
);

-- KPI 8: Tendances temporelles (ligne 312-360)
CREATE TABLE minio.default.kpi_temporal_trends (
    annee INTEGER,
    trimestre INTEGER,
    nb_consultations BIGINT,
    nb_hospitalisations BIGINT,
    nb_deces BIGINT,
    activite_totale BIGINT,
    calcul_date TIMESTAMP
)
WITH (
    external_location = 's3a://gold/kpi_temporal_trends/',
    format = 'PARQUET'
);
