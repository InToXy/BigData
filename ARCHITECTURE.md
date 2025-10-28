# 🏗️ Architecture Data Platform - CHU Medical

## 📋 Vue d'ensemble

Architecture complète de traitement de données médicales avec 5 couches:
- **Stockage**: MinIO (S3-compatible)
- **Traitement**: Jupyter + Spark 3.5.0  
- **Requêtage**: Trino SQL Engine
- **Visualisation**: Apache Superset
- **Données source**: PostgreSQL

---

## 🐳 Conteneurs déployés

| Service | Conteneur | Port(s) | Status | Fonction |
|---------|-----------|---------|--------|----------|
| **MinIO** | chu_minio | 9000, 9001 | ✅ | Data Lake S3 (bronze/silver/gold) |
| **PostgreSQL** | chu_postgres | 5432 | ✅ | Base de données source |
| **Metastore DB** | chu_metastore_db | 5432 (interne) | ✅ | Métadonnées Hive (réservé) |
| **Jupyter+Spark** | chu_jupyter | 8888, 4040 | ✅ | Notebooks + Traitement Spark |
| **Trino** | chu_trino | 8080 | ✅ | Moteur SQL distribué |
| **Superset DB** | chu_superset_db | 5432 (interne) | ✅ | Base Superset |
| **Superset** | chu_superset | 8088 | ✅ | Dashboard & BI |

---

## 🔐 Credentials

### MinIO S3
- **Console Web**: http://localhost:9001
- **API S3**: http://localhost:9000
- **Access Key**: minioadmin
- **Secret Key**: minioadmin123
- **Buckets**: bronze, silver, gold

### Jupyter Lab
- **URL**: http://localhost:8888
- **Token**: admin123
- **Spark UI**: http://localhost:4040 (quand job actif)

### Trino
- **URL**: http://localhost:8080
- **CLI**: `docker exec -it chu_trino trino`
- **Catalogs**: hive (MinIO), postgresql

### Superset
- **URL**: http://localhost:8088
- **Username**: admin
- **Password**: admin123

### PostgreSQL Source
- **Host**: localhost:5432
- **Database**: healthcare_data
- **Username**: admin
- **Password**: admin123

---

## 📊 Architecture des données

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│  ┌──────────────┐          ┌──────────────┐                │
│  │  PostgreSQL  │          │  CSV Files   │                │
│  │ healthcare_  │          │   /data/     │                │
│  │     data     │          │              │                │
│  └──────┬───────┘          └──────┬───────┘                │
└─────────┼──────────────────────────┼────────────────────────┘
          │                          │
          └──────────┬───────────────┘
                     ▼
          ┌─────────────────────┐
          │   SPARK PROCESSING   │
          │  (chu_jupyter)       │
          │  - ETL               │
          │  - Transformations   │
          │  - Aggregations      │
          └──────────┬───────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   ┌────────┐  ┌────────┐  ┌────────┐
   │ BRONZE │  │ SILVER │  │  GOLD  │
   │  Raw   │  │ Clean  │  │  KPIs  │
   │  Data  │  │  Data  │  │  Agg   │
   └────┬───┘  └────┬───┘  └────┬───┘
        │           │           │
        └───────────┼───────────┘
                    │
            ┌───────▼────────┐
            │   MINIO S3     │
            │ (chu_minio)    │
            │  Data Lake     │
            └───────┬────────┘
                    │
        ┌───────────┼───────────┐
        ▼                       ▼
   ┌─────────┐          ┌─────────────┐
   │  TRINO  │          │   SUPERSET  │
   │  SQL    │◄─────────┤  Dashboard  │
   │ Engine  │          │     BI      │
   └─────────┘          └─────────────┘
```

---

## 🚀 Démarrage rapide

### 1. Vérifier l'état de la stack
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### 2. Accéder aux services
- MinIO Console: http://localhost:9001
- Jupyter Lab: http://localhost:8888 (token: admin123)
- Trino UI: http://localhost:8080
- Superset: http://localhost:8088 (admin/admin123)

### 3. Tester MinIO
```bash
docker exec chu_minio mc ls myminio/
# Devrait afficher: bronze/, silver/, gold/
```

### 4. Tester Trino
```bash
docker exec chu_trino trino --execute "SHOW CATALOGS;"
# Devrait afficher: hive, postgresql, system
```

---

## 📝 Workflow typique

### 1. Ingestion BRONZE (Données brutes)
- Chargement depuis PostgreSQL ou CSV
- Format: Parquet
- Localisation: s3://bronze/table_name/
- Script: `spark_jobs/bronze_ingestion.py`

### 2. Transformation SILVER (Données nettoyées)
- Nettoyage, typage, dédoublonnage
- Modèle dimensionnel (Dims + Facts)
- Localisation: s3://silver/dim_*/fact_*/
- Script: `spark_jobs/silver_transformation.py`

### 3. Agrégation GOLD (KPIs)
- Indicateurs métier
- Agrégations pré-calculées
- Localisation: s3://gold/kpi_*/
- Script: `spark_jobs/gold_aggregation.py`

### 4. Requêtage Trino
- SQL sur toutes les zones (bronze/silver/gold)
- Jointures cross-catalog (hive + postgresql)
- Connexion depuis Superset

### 5. Visualisation Superset
- Création de datasets (connexion Trino)
- Charts & Dashboards
- Partage & export

---

## 🛠️ Commandes utiles

### MinIO
```bash
# Lister buckets
docker exec chu_minio mc ls myminio/

# Lister tables bronze
docker exec chu_minio mc ls myminio/bronze/

# Supprimer une table
docker exec chu_minio mc rm --recursive myminio/bronze/table_name/
```

### Spark (depuis Jupyter)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CHU_ETL") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Lire depuis Bronze
df = spark.read.parquet("s3a://bronze/table_name/")
df.show()

# Écrire vers Silver
df.write.mode("overwrite").parquet("s3a://silver/dim_table/")
```

### Trino
```bash
# Shell interactif
docker exec -it chu_trino trino

# Requête simple
docker exec chu_trino trino --execute "SELECT * FROM hive.bronze.table_name LIMIT 10;"

# Lister tables
docker exec chu_trino trino --execute "SHOW TABLES FROM hive.bronze;"
```

### PostgreSQL
```bash
# Se connecter
docker exec -it chu_postgres psql -U admin -d healthcare_data

# Depuis l'hôte
psql -h localhost -U admin -d healthcare_data
```

---

## 🔧 Troubleshooting

### Conteneur en restart
```bash
# Voir les logs
docker logs chu_<service_name>

# Redémarrer un service
docker restart chu_<service_name>
```

### Problème de connexion MinIO/Spark
- Vérifier les credentials dans le code Spark
- Vérifier que MinIO est accessible: `curl http://localhost:9000`

### Trino ne voit pas les tables
- Les tables doivent être créées via Spark avec le format Hive
- Utiliser `CREATE EXTERNAL TABLE` dans Trino ou Spark SQL

### Superset ne se connecte pas à Trino
- URI: `trino://chu_trino:8080/hive`
- Driver installé: `pip install trino` (déjà fait au démarrage)

---

## 📦 Volumes persistants

| Volume | Contenu |
|--------|---------|
| bigdata_minio_data | Données S3 (bronze/silver/gold) |
| bigdata_postgres_data | Base PostgreSQL source |
| bigdata_metastore_db_data | Métadonnées Hive (réservé) |
| bigdata_superset_db_data | Configuration Superset |

---

## 🧹 Nettoyage

### Supprimer toute la stack
```bash
docker stop $(docker ps -aq --filter "name=chu_")
docker rm $(docker ps -aq --filter "name=chu_")
```

### Supprimer les volumes (⚠️ PERTE DE DONNÉES)
```bash
docker volume rm $(docker volume ls -q --filter "name=bigdata")
```

### Redémarrer from scratch
```bash
# Supprimer conteneurs + volumes
docker stop $(docker ps -aq --filter "name=chu_") && \
docker rm $(docker ps -aq --filter "name=chu_") && \
docker volume rm $(docker volume ls -q --filter "name=bigdata")

# Relancer avec le docker-compose.yml ou les commandes docker run
```

---

## 📚 Documentation

- **MinIO**: https://min.io/docs
- **Spark**: https://spark.apache.org/docs/latest/
- **Trino**: https://trino.io/docs/current/
- **Superset**: https://superset.apache.org/docs/intro
- **Hive**: https://hive.apache.org/

---

**Architecture validée le**: 2025-10-28  
**Version**: 1.0.0 - Stack vierge opérationnelle
