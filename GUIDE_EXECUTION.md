# 🚀 Guide d'Exécution des Pipelines Data

## Problème Résolu

### ❌ Erreur Initiale
```
ModuleNotFoundError: No module named 'delta'
ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found
PATH_NOT_FOUND: file:/data/source/csv/...
```

### ✅ Solutions Appliquées

1. **Installation de Delta Lake** :
   ```bash
   docker exec chu_jupyter pip install delta-spark==3.0.0
   ```

2. **Redémarrage du conteneur Jupyter** (pour monter les volumes) :
   ```bash
   docker restart chu_jupyter
   ```

3. **Création du bucket gold-delta** :
   ```bash
   docker exec chu_minio mc mb myminio/gold-delta
   ```

---

## 📋 Commandes d'Exécution

### 1️⃣ Pipeline Bronze (CSV + PostgreSQL → MinIO Bronze)

**Exécution complète** :
```bash
cd /home/alban/BigData/BigData
./run_bronze_ingestion.sh
```

**Exécution manuelle** :
```bash
docker exec chu_jupyter bash -c "
cd /home/jovyan/work && \
spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar,/home/jovyan/jars/postgresql-42.6.0.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
  /home/jovyan/jobs/main_jobs/bronze_ingestion.py
"
```

**Données traitées** :
- 10 tables PostgreSQL (patients, consultations, décès, etc.)
- 17 fichiers CSV (établissements, qualité, satisfaction)
- **Total estimé** : ~7.4M lignes → MinIO bucket `bronze`

---

### 2️⃣ Pipeline Silver (Bronze → MinIO Silver)

**Exécution** :
```bash
docker exec chu_jupyter bash -c "
cd /home/jovyan/jobs/main_jobs && \
spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  silver_transformation.py
"
```

**Transformation** :
- Création des tables de dimension (dim_patient, dim_etablissement, dim_temp)
- Création des tables de faits (fact_consultation, fact_deces, fact_hospitalisation)
- Création des métriques agrégées
- **Total estimé** : ~2.2M lignes → MinIO bucket `silver`

---

### 3️⃣ Pipeline Gold Delta (Silver → MinIO Gold-Delta)

**Exécution complète** :
```bash
cd /home/alban/BigData/BigData
./run_gold_delta.sh
```

**Exécution manuelle** :
```bash
docker exec chu_jupyter bash -c "
cd /home/jovyan/jobs/main_jobs && \
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  gold_aggregation_delta.py
"
```

**KPIs générés** (format Delta Lake) :
1. `kpi_taux_consultation_periode` - Taux de consultation global
2. `kpi_consultation_par_diagnostic` - Consultations par diagnostic
3. `kpi_taux_hospitalisation_global` - Taux d'hospitalisation
4. `kpi_hospitalisation_par_diagnostic` - Hospitalisations par diagnostic
5. `kpi_hospitalisation_sexe_age` - Hospitalisations par démographie
6. `kpi_consultation_par_professionnel` - Consultations par professionnel
7. `kpi_deces_par_region_2019` - Décès par région (2019)
8. `kpi_satisfaction_par_region_2020` - Satisfaction par région (2020)

---

## 🔍 Validation et Audit

### Audit des zones Bronze et Silver
```bash
docker cp /home/alban/BigData/BigData/spark_jobs/audit_zones.py chu_jupyter:/home/jovyan/work/
docker exec chu_jupyter bash -c "
cd /home/jovyan/work && \
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  audit_zones.py
"
```

### Audit de la zone Gold
```bash
docker cp /home/alban/BigData/BigData/spark_jobs/audit_gold.py chu_jupyter:/home/jovyan/work/
docker exec chu_jupyter bash -c "
cd /home/jovyan/work && \
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  audit_gold.py
"
```

---

## 📊 Accès aux Données

### MinIO Console Web
- **URL** : http://localhost:9001
- **Login** : minioadmin
- **Password** : minioadmin123
- **Buckets** : 
  - `bronze` : Données brutes (Parquet)
  - `silver` : Données transformées (Parquet)
  - `gold-delta` : KPIs (Delta Lake)

### Lecture des données Delta Lake

**Depuis un notebook Jupyter** :
```python
from pyspark.sql import SparkSession
from delta import *

# Initialiser Spark avec Delta
builder = SparkSession.builder \
    .appName("ReadGoldDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Lire une table Delta
df = spark.read.format("delta").load("s3a://gold-delta/kpi_taux_consultation_periode")
df.show()

# Time Travel (version précédente)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("s3a://gold-delta/kpi_taux_consultation_periode")

# Historique des versions
from delta import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3a://gold-delta/kpi_taux_consultation_periode")
delta_table.history().show()
```

---

## ⚠️ Résolution de Problèmes

### Erreur: "PATH_NOT_FOUND: file:/data/source/csv/..."
**Solution** : Redémarrer le conteneur Jupyter
```bash
docker restart chu_jupyter
```

### Erreur: "ClassNotFoundException: S3AFileSystem"
**Solution** : Utiliser `spark-submit` avec les options `--jars` et `--conf`
(voir commandes ci-dessus)

### Erreur: "ModuleNotFoundError: No module named 'delta'"
**Solution** : Installer delta-spark
```bash
docker exec chu_jupyter pip install delta-spark==3.0.0
```

### Bucket n'existe pas
**Solution** : Créer le bucket manquant
```bash
docker exec chu_minio mc mb myminio/<bucket-name>
```

---

## 📈 Architecture du Data Lake

```
Sources de Données
├── PostgreSQL (healthcare_data)
│   ├── Patient (100K lignes)
│   ├── Consultation (1M+ lignes)
│   ├── Décès (620K lignes 2019)
│   └── ... (10 tables)
│
└── CSV Files (/data/source/csv/)
    ├── etablissement_sante.csv (416K lignes)
    ├── professionnel_sante.csv (1M+ lignes)
    ├── activite_professionnel_sante.csv (1.8M+ lignes)
    └── ... (17 fichiers)

           ↓ bronze_ingestion.py

Bronze Zone (MinIO: s3a://bronze/)
├── 28 tables Parquet
├── 7.4M lignes totales
├── ~709 MB
├── PII anonymisées (SHA256)
├── Clés de substitution (_sk_*)
└── Métadonnées techniques (_ingestion_date, _hash_record)

           ↓ silver_transformation.py

Silver Zone (MinIO: s3a://silver/)
├── Dimensions
│   ├── dim_patient (100K)
│   ├── dim_etablissement (416K)
│   └── dim_temp (2.5K)
├── Faits
│   ├── fact_consultation (1M+)
│   ├── fact_deces (620K)
│   └── fact_hospitalisation (2.5K)
├── Métriques
│   └── 4 tables agrégées
├── 2.2M lignes totales
└── ~207 MB (70% compression)

           ↓ gold_aggregation_delta.py

Gold Zone (MinIO: s3a://gold-delta/)
├── 8 KPIs (Delta Lake format)
├── ACID transactions
├── Time travel activé
├── Schema evolution
└── Auto-compaction

           ↓ Visualisation

Dashboard / Analytics
└── PowerBI, Superset, Tableau
```

---

## 🎯 Prochaines Étapes

1. ✅ Pipeline Bronze fonctionnel
2. ✅ Pipeline Silver fonctionnel  
3. ✅ Pipeline Gold Delta fonctionnel
4. ⏳ Orchestration Airflow (DAGs prêts mais Airflow commenté)
5. ⏳ Dashboards Superset (conteneur prêt)
6. ⏳ Tests unitaires et CI/CD
7. ⏳ Monitoring et alerting

---

**Date de mise à jour** : 24 octobre 2025
**Auteur** : BigData Pipeline Team
