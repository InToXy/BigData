# 🏥 Plateforme BigData Healthcare - CHU

Plateforme de Data Lake et d'analyse de données de santé basée sur l'architecture Médaillon (Bronze, Silver, Gold) avec Apache Spark, Trino et Apache Superset.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│                   PostgreSQL (Healthcare Data)                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw Data)                     │
│                  MinIO S3 - Parquet Files                        │
└────────────────────┬────────────────────────────────────────────┘
                     │ Apache Spark ETL
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned Data)                    │
│              MinIO S3 - Parquet Files (Partitioned)              │
└────────────────────┬────────────────────────────────────────────┘
                     │ Apache Spark Aggregations
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              GOLD LAYER (Business Analytics)                     │
│         MinIO S3 - Parquet Files (Dimensions & Facts)            │
│                    • 5 Dimensions                                │
│                    • 2 Facts Tables                              │
│                    • 5 Data Marts                                │
└────────────────────┬────────────────────────────────────────────┘
                     │
         ┌───────────┴──────────────┐
         ▼                          ▼
    ┌─────────┐              ┌──────────────┐
    │  TRINO  │              │   SUPERSET   │
    │  Query  │              │ Dashboards   │
    │ Engine  │              │ & Analytics  │
    └─────────┘              └──────────────┘
```

## 📊 Couches de Données

### Bronze Layer
Données brutes ingérées depuis PostgreSQL :
- `consultations` - Consultations médicales
- `deces` - Données de décès
- `diagnostics` - Codes diagnostics
- `etablissements` - Établissements de santé
- `hospitalisations` - Séjours hospitaliers
- `patients` - Données patients
- `professionnels_sante` - Professionnels de santé
- `satisfaction_mco_2020` - Satisfaction patients 2020

### Silver Layer
Données nettoyées et transformées avec qualité de données améliorée

### Gold Layer
**Tables de Dimension (5):**
- `dim_diagnostic` - 15,490 lignes
- `dim_etablissement` - 416,665 lignes
- `dim_localisation` - 101 lignes
- `dim_patient` - 100,000 lignes
- `dim_professionnel` - 1,048,575 lignes

**Tables de Faits (2):**
- `fact_consultation` - 1,027,157 lignes (partitionnée)
- `fact_deces` - 620,608 lignes

**Data Marts (5):**
- `mart_deces_localisation_2019` - Analyse décès par région
- `mart_demographie` - Analyse démographique
- `mart_diagnostic_epidemio` - Épidémiologie
- `mart_professionnel` - Performance professionnels
- `mart_satisfaction_region_2020` - Satisfaction régionale

**Total: 3,430,885 lignes de données**

## 🛠️ Stack Technique

| Composant | Technologie | Port | Description |
|-----------|-------------|------|-------------|
| **Data Lake** | MinIO | 9000, 9001 | Stockage objet S3-compatible |
| **Source DB** | PostgreSQL 15 | 5432 | Base de données source |
| **Processing** | Apache Spark (via Jupyter) | 8888 | Traitement distribué |
| **Query Engine** | Trino | 8090 | Moteur de requêtes SQL |
| **Visualization** | Apache Superset | 8088 | Dashboards et analytics |
| **Metastore** | Superset PostgreSQL | - | Métadonnées Superset |

## 🚀 Démarrage Rapide

### Prérequis
- Docker & Docker Compose
- 16 GB RAM minimum
- 50 GB espace disque

### Installation

1. **Cloner le projet**
```bash
git clone <votre-repo>
cd BigData
```

2. **Configurer l'environnement**
```bash
# Créer le fichier .env
cp .env.example .env
# Adapter les chemins dans .env
```

3. **Télécharger les JARs Spark** (nécessaires pour S3/MinIO)
```bash
# Option 1: Script automatique
./download_jars.sh

# Option 2: Manuellement
mkdir -p jars
cd jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar
cd ..
```

**⚠️ Important**: Les JARs (~280MB) sont nécessaires pour que Spark puisse se connecter à MinIO/S3.

4. **Lancer l'infrastructure**
```bash
docker compose up -d
```

5. **Vérifier le démarrage**
```bash
docker compose ps
```

## 📝 Accès aux Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Jupyter Lab** | http://localhost:8888 | Token: admin123 |
| **Trino UI** | http://localhost:8090 | - |
| **Superset** | http://localhost:8088 | admin / admin123 |

## 🔗 Connexion Superset à Trino

1. Ouvrir Superset : http://localhost:8088
2. Login : `admin` / `admin123`
3. **Settings** > **Database Connections** > **+ Database**
4. Sélectionner **Trino**
5. **SQLAlchemy URI** :
   ```
   trino://admin@trino:8080/parquet/gold
   ```
6. **Test Connection** > **Connect**

## 🔍 Requêtes d'Exemple

### Trino CLI
```bash
# Se connecter à Trino
docker exec -it chu_trino trino

# Lister les catalogues
SHOW CATALOGS;

# Lister les tables Gold
SHOW TABLES FROM parquet.gold;

# Requête simple
SELECT COUNT(*) FROM parquet.gold.dim_patient;

# Analyse par région
SELECT region, COUNT(*) as nb_patients
FROM parquet.gold.dim_patient
GROUP BY region
ORDER BY nb_patients DESC;
```

### Depuis Python (Jupyter)
```python
from pyspark.sql import SparkSession

# Lire les données Gold
df = spark.read.parquet("s3a://gold/dim_patient")
df.show()

# Analyse
df.groupBy("region").count().show()
```

## 📦 Structure du Projet

```
BigData/
├── docker-compose.yml          # Configuration des services
├── .env                        # Variables d'environnement
├── .gitignore                  # Fichiers à ignorer
├── README.md                   # Cette documentation
│
├── spark_jobs/                 # Scripts Spark ETL
│   ├── main_jobs/              # Jobs principaux (Bronze→Silver→Gold)
│   ├── script_claude/          # Scripts utilitaires
│   └── visu/                   # Scripts de visualisation
│
├── trino/                      # Configuration Trino
│   └── catalog/
│       └── parquet.properties  # Catalogue Parquet/Hive
│
├── superset/                   # Configuration Superset
│   └── superset_config.py      # Configuration personnalisée
│
└── jupyter/                    # Notebooks Jupyter
    └── notebooks/
```

## 🔧 Scripts ETL Principaux

### Exécution depuis Jupyter Lab

1. Ouvrir Jupyter Lab: http://localhost:8888 (token: `admin123`)
2. Naviguer vers `jobs/main_jobs/`
3. Ouvrir un Terminal Jupyter (`File` > `New` > `Terminal`)
4. Exécuter les scripts:

```bash
# Bronze Layer (ingestion depuis PostgreSQL)
cd /home/jovyan/jobs/main_jobs
python bronze_ingestion.py

# Silver Layer (nettoyage et transformations)
python silver_transformation.py

# Gold Layer (modèle dimensionnel)
python gold_star_schema.py
```

**Note**: Les scripts nécessitent les JARs Spark pour S3/MinIO. Ils sont automatiquement chargés depuis `/home/jovyan/jars/`

### Alternative: Exécuter dans un notebook

Créer un nouveau notebook et copier le code du script, puis exécuter les cellules.

## 🎯 Cas d'Usage

### 1. Analyse Épidémiologique
```sql
SELECT
    diagnostic,
    type_pathologie,
    nb_consultations,
    nb_hospitalisations
FROM parquet.gold.mart_diagnostic_epidemio
WHERE annee = 2023
ORDER BY nb_consultations DESC
LIMIT 10;
```

### 2. Performance des Établissements
```sql
SELECT
    region,
    nb_etablissements_evalues,
    score_satisfaction_moyen,
    taux_recommandation_moyen
FROM parquet.gold.mart_satisfaction_region_2020
ORDER BY score_satisfaction_moyen DESC;
```

### 3. Analyse Démographique
```sql
SELECT
    sexe,
    categorie_age,
    nb_hospitalisations,
    duree_moyenne_sejour
FROM parquet.gold.mart_demographie
WHERE annee = 2023
ORDER BY nb_hospitalisations DESC;
```

## 🔐 Sécurité

**⚠️ IMPORTANT - Pour la production:**

1. **Changer tous les mots de passe** dans `.env`
2. **Activer HTTPS** pour tous les services web
3. **Configurer un reverse proxy** (Nginx/Traefik)
4. **Activer l'authentification** Trino
5. **Restreindre l'accès réseau** aux services
6. **Sauvegarder régulièrement** les volumes Docker
7. **Ne JAMAIS commit** le fichier `.env` avec des vrais credentials

## 📊 Monitoring

### Vérifier l'état des services
```bash
docker compose ps
docker compose logs -f <service>
```

### Métriques de stockage
```bash
# Se connecter à MinIO Console
http://localhost:9001

# Vérifier les buckets
docker exec chu_minio mc ls /data/
```

## 🐛 Troubleshooting

**📖 Guide complet**: Voir [TROUBLESHOOTING.md](TROUBLESHOOTING.md) pour tous les problèmes courants et leurs solutions.

### Problèmes fréquents

**❌ ClassNotFoundException: S3AFileSystem**
- Cause: JARs Spark non téléchargés
- Solution: `./download_jars.sh` puis redémarrer Jupyter

**❌ Tables vides dans Trino**
```bash
# Re-synchroniser les partitions
docker exec chu_trino trino --execute "CALL parquet.system.sync_partition_metadata('gold', 'fact_consultation', 'FULL')"
```

**❌ Superset: Erreur CSRF**
- Vérifier `superset_config.py`: `WTF_CSRF_ENABLED = False`
- Redémarrer: `docker compose restart superset`

**❌ MinIO inaccessible**
```bash
docker compose ps minio
curl http://localhost:9000
```

## 📚 Documentation Complémentaire

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Trino Documentation](https://trino.io/docs/current/)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## 🤝 Contribution

Pour contribuer au projet :
1. Fork le repository
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📄 License

Ce projet est sous licence MIT.

## ✨ Auteurs

**Projet BigData CHU**
- Architecture Médaillon
- Pipeline ETL Spark
- Analytics avec Trino & Superset

---

**Version**: 1.0.0
**Date**: 2025-10-27
**Status**: Production Ready ✅
