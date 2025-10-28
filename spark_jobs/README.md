# Spark Jobs - Pipeline de Production

Ce dossier contient les jobs Spark nécessaires pour le pipeline de données complet.

## 📋 Jobs de Production

### Bronze Layer
- **`bronze_ingestion_rgpd_complete.py`** (19KB)
  - Job principal d'ingestion Bronze
  - Charge les 21 tables sources (CSV)
  - Applique l'anonymisation RGPD (SHA-256)
  - Filtre sur les décès 2019
  - Sortie: s3a://bronze/ (210MB, 50 fichiers Parquet)
  - **Utilisation**: `spark-submit bronze_ingestion_rgpd_complete.py`

### Silver Layer
- **`silver_transformation.py`** (18KB)
  - Transformation Bronze → Silver
  - Crée le modèle dimensionnel (4 dimensions + 3 faits)
  - Dimensions: temps, géographie, établissement, professionnel
  - Faits: décès, activité, hospitalisation
  - Sortie: s3a://silver/ (20MB, 18 fichiers Parquet)
  - **Utilisation**: `spark-submit silver_transformation.py`

### Gold Layer - KPIs de Base
- **`gold_aggregation.py`** (8.4KB)
  - Crée 7 KPIs agrégés pour analyse exploratoire
  - KPIs: décès par année/région, démographie, tendances, distribution âge
  - Sortie: s3a://gold/ (23KB, 14 fichiers Parquet)
  - **Utilisation**: `spark-submit gold_aggregation.py`

### Gold Layer - KPIs Métier
- **`gold_kpis_metier.py`** (12KB)
  - Crée 7 KPIs métier pour analyse business
  - KPIs: consultations (établissement, professionnel), hospitalisation (global, sexe/âge), décès région 2019, satisfaction, synthèse
  - Sortie: s3a://gold/kpi_* (6.1MB)
  - **Utilisation**: `docker exec chu_jupyter spark-submit gold_metier.py`

### PostgreSQL Integration
- **`gold_to_postgres.py`** (3.1KB)
  - Charge les KPIs Gold dans PostgreSQL pour Superset
  - Charge 14 tables (7 base + 7 métier)
  - Base: healthcare_data, User: admin
  - **Utilisation**: `spark-submit --jars postgresql-42.6.0.jar gold_to_postgres.py`

## 🔄 Ordre d'Exécution

```bash
# 1. Bronze (ingestion des CSV sources)
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/jovyan/bronze_ingestion_rgpd_complete.py

# 2. Silver (modèle dimensionnel)
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/jovyan/silver_transformation.py

# 3. Gold - KPIs de base
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/jovyan/gold_aggregation.py

# 4. Gold - KPIs métier
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/jovyan/gold_metier.py

# 5. Chargement PostgreSQL
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --jars /usr/local/spark/jars/postgresql-42.6.0.jar \
    /home/jovyan/gold_kpis_to_postgres.py
```

## 📊 Résultats

### Stockage MinIO
- **Bronze**: 210MB, 21 tables (50 fichiers)
- **Silver**: 20MB, 7 tables (18 fichiers)
- **Gold**: 6.1MB, 14 KPIs (28 fichiers)

### PostgreSQL
- **Database**: healthcare_data
- **Tables**: 14 KPIs (accessible par Superset)

### Superset
- **Datasets**: 14 datasets exposés
- **URL**: http://localhost:8088

## ⚠️ Notes Importantes

- Tous les jobs utilisent Hadoop-AWS 3.3.4 pour MinIO S3A
- L'anonymisation RGPD est appliquée sur les PII (SHA-256)
- Les données sont filtrées sur 2019 uniquement
- Les KPIs incluent des métadonnées: _gold_batch_id, _gold_load_date
- PostgreSQL JDBC driver requis: postgresql-42.6.0.jar

## 📝 Fichiers Supprimés (Cleanup Oct 2025)

Les fichiers suivants ont été supprimés car obsolètes ou de test:
- bronze_all_sources.py
- bronze_csv_only.py
- bronze_deces_only.py
- bronze_ingestion_2019.py
- bronze_ingestion_rgpd.py
- bronze_test_sample.py
- check_data_quality.py
- display_bronze_tables.py
- fix_bronze_quality.py
- silver_simple.py
- view_bronze.py
- view_bronze_tables.py
