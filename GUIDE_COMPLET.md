# 🏥 CHU Data Warehouse - Guide Complet de Déploiement

## 📋 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    CHU DATA WAREHOUSE                            │
│                 Architecture Medallion complète                  │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   SOURCES    │     │   BRONZE     │     │   SILVER     │
│              │────>│              │────>│              │
│ PostgreSQL   │     │ Données      │     │ Schéma en    │
│ CSV Files    │     │ brutes       │     │ étoile       │
└──────────────┘     │ normalisées  │     │ (dims/facts) │
                     └──────────────┘     └──────────────┘
                            │                     │
                            │                     │
                            v                     v
                     ┌──────────────┐     ┌──────────────┐
                     │     GOLD     │     │    TRINO     │
                     │              │────>│              │
                     │ 8 KPIs       │     │ Query Engine │
                     │ agrégés      │     │              │
                     └──────────────┘     └──────────────┘
                                                  │
                                                  v
                                          ┌──────────────┐
                                          │   SUPERSET   │
                                          │              │
                                          │ Dashboards   │
                                          └──────────────┘
```

## 🎯 Tables Créées

### 🟤 BRONZE (15 tables - 5.4M lignes)
- `consultations` - 1.0M consultations
- `patients` - 1.0M patients
- `deces_2019` - 620K décès (filtrés 2019)
- `etablissements` - 417K établissements de santé
- `prescriptions` - 1.0M prescriptions
- `professionnels` - 1.0M professionnels de santé
- `hospitalisations` - 2.5K hospitalisations
- `satisfaction_*` - Enquêtes de satisfaction
- Et plus...

### 🔵 SILVER (7 tables - Star Schema)
**Dimensions:**
- `dim_patient` - Patients avec démographie enrichie
- `dim_etablissement` - Établissements avec géographie
- `dim_temps` - Calendrier 2018-2025

**Faits:**
- `fact_consultation` - Consultations avec métriques
- `fact_hospitalisation` - Hospitalisations et durées
- `fact_deces` - Décès avec démographie

**Métriques:**
- `metrique_satisfaction` - Satisfaction patients

### 🏆 GOLD (8 KPIs)
1. `kpi_consultation_rate` - Taux de consultation par période
2. `kpi_hospitalisation_metrics` - Métriques hospitalisation
3. `kpi_deces_by_region` - Décès par région
4. `kpi_satisfaction_global` - Satisfaction agrégée
5. `kpi_activite_mensuelle` - Activité mensuelle
6. `kpi_patient_demographics` - Démographie patients
7. `kpi_etablissement_performance` - Performance établissements
8. `kpi_temporal_trends` - Tendances temporelles

## 🚀 Déploiement Complet

### Prérequis
```bash
# Docker et Docker Compose installés
docker --version
docker-compose --version

# Mémoire recommandée: 8GB RAM minimum
# Espace disque: 50GB minimum
```

### Étape 1: Démarrer les conteneurs
```bash
cd /home/alban/BigData/BigData

# Lancer tous les services
docker-compose up -d

# Vérifier que tout est lancé
docker-compose ps

# Services attendus:
# - chu_minio (MinIO Data Lake)
# - chu_postgres_data (PostgreSQL source)
# - chu_jupyter (Spark/Jupyter)
# - chu_trino (Query Engine)
# - chu_hive_metastore (Metadata)
# - chu_superset (BI Tool)
```

### Étape 2: Exécuter le pipeline complet
```bash
# Option A: Pipeline complet automatique
chmod +x run_all_pipeline.sh
./run_all_pipeline.sh

# Option B: Étape par étape
chmod +x run_bronze.sh run_silver.sh run_gold.sh

# 1. Bronze (ingestion)
./run_bronze.sh

# 2. Silver (transformation)
./run_silver.sh

# 3. Gold (agrégation)
./run_gold.sh
```

### Étape 3: Configurer Trino
```bash
# Créer les tables Trino pour accéder au Gold
chmod +x setup_trino.sh
./setup_trino.sh

# Tester Trino CLI
docker exec -it chu_trino trino

# Dans Trino:
USE hive.chu_gold;
SHOW TABLES;
SELECT * FROM kpi_consultation_rate LIMIT 10;
```

### Étape 4: Configurer Superset
```bash
# 1. Accéder à Superset
#    URL: http://localhost:8088
#    Login: admin / admin123

# 2. Ajouter la connexion Trino
#    - Aller dans: Data > Databases > + Database
#    - Sélectionner: Trino
#    - SQLAlchemy URI: trino://trino@chu_trino:8080/hive/chu_gold
#    - Tester la connexion

# 3. Créer des datasets
#    - Data > Datasets > + Dataset
#    - Sélectionner: Database Trino, Schema chu_gold
#    - Ajouter chaque KPI comme dataset

# 4. Créer des dashboards
#    - Dashboards > + Dashboard
#    - Ajouter des charts à partir des datasets
```

## 📊 Exemples de Requêtes Trino

### Consultations par mois (derniers 12 mois)
```sql
USE hive.chu_gold;

SELECT 
    annee,
    mois,
    nb_consultations,
    nb_patients_uniques,
    ROUND(montant_moyen, 2) as montant_moyen_euros
FROM kpi_consultation_rate
WHERE annee >= YEAR(CURRENT_DATE) - 1
ORDER BY annee DESC, mois DESC;
```

### Hospitalisations - Durée moyenne par année
```sql
SELECT 
    annee,
    nb_hospitalisations,
    nb_patients_hospitalises,
    ROUND(duree_moyenne_sejour, 1) as duree_moyenne_jours,
    ROUND(taux_hospit_patient, 2) as taux_readmission
FROM kpi_hospitalisation_metrics
ORDER BY annee DESC;
```

### Décès par région (top 5)
```sql
SELECT 
    lieu_deces,
    sexe,
    SUM(nb_deces) as total_deces,
    ROUND(AVG(age_moyen_deces), 1) as age_moyen
FROM kpi_deces_by_region
WHERE annee = 2019
GROUP BY lieu_deces, sexe
ORDER BY total_deces DESC
LIMIT 5;
```

### Activité mensuelle - Tendance
```sql
SELECT 
    annee,
    mois,
    nb_consultations,
    nb_hospitalisations,
    activite_totale,
    ROUND(100.0 * nb_consultations / NULLIF(activite_totale, 0), 1) as pct_consultations
FROM kpi_activite_mensuelle
WHERE annee >= 2019
ORDER BY annee, mois;
```

### Démographie patients - Pyramide des âges
```sql
SELECT 
    tranche_age,
    sexe,
    nb_patients,
    ROUND(100.0 * nb_patients / SUM(nb_patients) OVER (), 2) as pourcentage
FROM kpi_patient_demographics
ORDER BY 
    CASE tranche_age
        WHEN '0-17' THEN 1
        WHEN '18-35' THEN 2
        WHEN '36-55' THEN 3
        WHEN '56-75' THEN 4
        WHEN '75+' THEN 5
        ELSE 6
    END,
    sexe;
```

## 🔧 Maintenance et Monitoring

### Vérifier l'état des buckets MinIO
```bash
# Via interface Web
# URL: http://localhost:9001
# Login: minioadmin / minioadmin123

# Ou via CLI
docker exec chu_minio mc ls myminio/bronze/
docker exec chu_minio mc ls myminio/silver/
docker exec chu_minio mc ls myminio/gold/
```

### Logs des jobs Spark
```bash
# Bronze
docker logs chu_jupyter | grep "BRONZE"

# Silver
docker logs chu_jupyter | grep "SILVER"

# Gold
docker logs chu_jupyter | grep "GOLD"
```

### Redémarrer un service
```bash
# Redémarrer Trino
docker-compose restart trino

# Redémarrer Superset
docker-compose restart superset

# Redémarrer tous les services
docker-compose restart
```

### Réinitialiser les données
```bash
# ATTENTION: Supprime toutes les données!

# Supprimer les buckets MinIO
docker exec chu_minio mc rb --force myminio/bronze/
docker exec chu_minio mc rb --force myminio/silver/
docker exec chu_minio mc rb --force myminio/gold/

# Recréer les buckets
docker exec chu_minio mc mb myminio/bronze/
docker exec chu_minio mc mb myminio/silver/
docker exec chu_minio mc mb myminio/gold/

# Relancer le pipeline
./run_all_pipeline.sh
```

## 🎨 Exemples de Dashboards Superset

### Dashboard 1: Vue Exécutive
**KPIs clés:**
- Nombre total de consultations (année en cours)
- Nombre total d'hospitalisations
- Durée moyenne de séjour
- Taux de satisfaction global

**Charts:**
- Big Numbers pour les totaux
- Line Chart pour les tendances mensuelles
- Bar Chart pour la répartition par type d'établissement

### Dashboard 2: Analyse Géographique
**Données:**
- `kpi_deces_by_region`
- `kpi_etablissement_performance`

**Charts:**
- Map Chart (si coordonnées GPS disponibles)
- Table avec décès par région
- Bar Chart horizontal - Établissements par région

### Dashboard 3: Activité Opérationnelle
**Données:**
- `kpi_activite_mensuelle`
- `kpi_temporal_trends`

**Charts:**
- Area Chart - Activité mensuelle empilée
- Line Chart - Tendances par trimestre
- Heatmap - Activité par jour de la semaine

### Dashboard 4: Démographie & Santé Publique
**Données:**
- `kpi_patient_demographics`
- `kpi_deces_by_region`

**Charts:**
- Pyramid Chart - Pyramide des âges par sexe
- Pie Chart - Répartition par tranche d'âge
- Bar Chart - Âge moyen de décès par région

## 🔗 URLs des Services

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 | Data Lake S3 |
| **Jupyter Lab** | http://localhost:8888 | Token: admin123 | Notebooks Spark |
| **Trino Web UI** | http://localhost:8090 | Aucun | Query Engine |
| **Superset** | http://localhost:8088 | admin / admin123 | Business Intelligence |
| **PostgreSQL** | localhost:5432 | admin / admin123 | DB: healthcare_data |

## 🐛 Troubleshooting

### Problème: "S3AFileSystem not found"
**Solution:**
Les JARs Hadoop ne sont pas dans le classpath. Utiliser les scripts fournis qui incluent automatiquement les JARs.

### Problème: "Table not found in Bronze"
**Solution:**
```bash
# Vérifier que Bronze a été exécuté
docker exec chu_jupyter ls /home/jovyan/work/

# Relancer Bronze
./run_bronze.sh
```

### Problème: Trino ne se connecte pas à MinIO
**Solution:**
```bash
# Vérifier Hive Metastore
docker logs chu_hive_metastore

# Redémarrer Trino
docker-compose restart trino

# Attendre 1 minute puis retester
```

### Problème: Superset ne trouve pas les tables
**Solution:**
1. Vérifier que Trino est accessible: http://localhost:8090
2. Tester la connexion Trino dans Superset
3. Rafraîchir les métadonnées: Data > Databases > Trino > Edit > Refresh Schemas

### Problème: Manque de mémoire
**Solution:**
```bash
# Réduire la mémoire Spark dans les scripts
# Éditer run_bronze.sh, run_silver.sh, run_gold.sh
# Changer: --driver-memory 2g --executor-memory 2g
# En: --driver-memory 1g --executor-memory 1g

# Ou augmenter la mémoire Docker
# Docker Desktop > Settings > Resources > Memory: 8GB
```

## 📚 Documentation Supplémentaire

- **Architecture Bronze**: `BRONZE_ARCHITECTURE.md`
- **Quick Start Bronze**: `QUICKSTART_BRONZE.md`
- **Besoins CHU**: `REPONSE_BESOINS_CHU.md`
- **Trino SQL**: `trino/setup_trino_gold.sql`

## 🎯 Prochaines Améliorations

1. **Orchestration Airflow** - Automatiser le pipeline avec DAGs
2. **Delta Lake** - Versioning et ACID sur les tables
3. **Monitoring** - Grafana + Prometheus pour métriques
4. **ML Pipeline** - Prédictions sur les données patient
5. **API REST** - Exposer les KPIs via FastAPI
6. **Alertes** - Notifications sur anomalies détectées

## 📞 Support

Pour toute question ou problème:
1. Vérifier les logs: `docker-compose logs <service>`
2. Consulter la documentation Trino: https://trino.io/docs/
3. Documentation Superset: https://superset.apache.org/docs/

---

**Version:** 1.0.0  
**Date:** Octobre 2025  
**Projet:** CHU Data Warehouse - Architecture Medallion
