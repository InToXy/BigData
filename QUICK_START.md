# 🚀 Quick Start - Pipeline Data Lake

## Problème Actuel

Votre script `gold_aggregation.py` affiche "fact_consultation introuvable" car **les tables Silver n'existent pas encore**.

## Solution : Exécuter les Pipelines dans l'Ordre

Le Data Lake nécessite 3 étapes dans l'ordre :

```
1. Bronze  (CSV/PostgreSQL → MinIO)
2. Silver  (Bronze → Transformation)  ← VOUS ÊTES ICI
3. Gold    (Silver → KPIs)
```

---

## ⚡ Démarrage Rapide

### Option 1 : Pipeline Complet Automatique (Recommandé)

```bash
cd /home/alban/BigData/BigData
./run_full_pipeline.sh
```

Ce script exécute automatiquement :
- ✅ Bronze (ingestion ~28 tables)
- ✅ Silver (transformation ~10 tables)
- ✅ Gold (agrégation 8 KPIs)

**Durée estimée** : 10-20 minutes selon votre machine

---

### Option 2 : Étape par Étape

#### 1️⃣ Pipeline Bronze (CSV + PostgreSQL → MinIO)
```bash
./run_bronze_ingestion.sh
```
**Résultat** : 28 tables dans bucket `bronze` (~709 MB, 7.4M lignes)

#### 2️⃣ Pipeline Silver (Bronze → Transformation)
```bash
./run_silver_transformation.sh
```
**Résultat** : 10 tables dans bucket `silver` (~207 MB, 2.2M lignes)
- 3 dimensions (patient, etablissement, temp)
- 3 faits (consultation, deces, hospitalisation)
- 4 métriques

#### 3️⃣ Pipeline Gold (Silver → KPIs)
```bash
./run_gold_delta.sh
```
**Résultat** : 8 KPIs dans bucket `gold-delta` (format Delta Lake)

---

## 📋 Vérification Rapide

### Vérifier les données dans MinIO

**Interface Web** :
- URL : http://localhost:9001
- Login : `minioadmin` / `minioadmin123`

**Ligne de commande** :
```bash
# Bronze
docker exec chu_minio mc ls myminio/bronze/

# Silver
docker exec chu_minio mc ls myminio/silver/

# Gold
docker exec chu_minio mc ls myminio/gold-delta/
```

---

## ❓ Pourquoi `gold_aggregation.py` ne fonctionne pas ?

```
fact_consultation introuvable  ← Table Silver inexistante
```

**Cause** : Vous avez sauté l'étape Silver

**Solution** : Exécuter d'abord :
```bash
./run_silver_transformation.sh
```

Puis re-lancer :
```bash
# Version Delta Lake (recommandée)
./run_gold_delta.sh

# OU version Parquet standard
docker exec chu_jupyter bash -c "cd /home/jovyan/jobs/main_jobs && \
  spark-submit --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  gold_aggregation.py"
```

---

## 🔧 Dépannage

### Erreur : "PATH_NOT_FOUND: file:/data/source/csv/"
```bash
docker restart chu_jupyter
```

### Erreur : "ClassNotFoundException: S3AFileSystem"
➡️ **Utiliser les scripts shell fournis** (ils configurent automatiquement les JARs)

### Bucket vide après exécution
```bash
# Vérifier les logs
tail -100 /home/alban/BigData/BigData/bronze_run.log
tail -100 /home/alban/BigData/BigData/gold_delta_run.log
```

### Processus bloqué
```bash
# Voir les jobs Spark en cours
docker exec chu_jupyter ps aux | grep spark

# Tuer un job si nécessaire
docker exec chu_jupyter pkill -f "nom_du_script"
```

---

## 📚 Documentation Complète

Voir `GUIDE_EXECUTION.md` pour :
- Commandes détaillées
- Architecture du Data Lake
- Utilisation de Delta Lake (Time Travel)
- Lecture des données avec Spark/Python

---

## 🎯 Commande Recommandée pour Vous

Puisque vos buckets sont vides, lancez directement :

```bash
cd /home/alban/BigData/BigData
./run_full_pipeline.sh
```

Puis allez prendre un café ☕ pendant 15 minutes !

---

**Dernière mise à jour** : 24 octobre 2025
