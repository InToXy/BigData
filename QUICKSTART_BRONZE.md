# 🏥 Guide de Démarrage Rapide - Pipeline Bronze CHU

## ✅ Ce qui a été fait

### 1. Résolution des conflits de merge
- ✅ Fichier `bronze_ingestion.py` nettoyé
- ✅ Fusion des imports PySpark
- ✅ Fonction `normalize_dates_advanced()` unifiée

### 2. Scripts d'exécution créés
- ✅ `run_bronze.sh` - Script simplifié d'exécution
- ✅ `verify_bronze.py` - Vérification du contenu Bronze

### 3. Documentation créée
- ✅ `BRONZE_ARCHITECTURE.md` - Architecture complète du pipeline

---

## 🚀 Lancer l'Ingestion Bronze

### Option 1 : Script automatisé (recommandé)

```bash
cd /home/alban/BigData/BigData
./run_bronze.sh
```

### Option 2 : Commande directe

```bash
docker exec chu_jupyter spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/jobs/main_jobs/bronze_ingestion.py
```

---

## 📊 Vérifier les Résultats

### Lister les tables Bronze

```bash
docker exec chu_jupyter spark-submit \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/work/verify_bronze.py
```

### Via l'interface MinIO

```
http://localhost:9001
Login: minioadmin / minioadmin123
Bucket: bronze
```

---

## 📋 Tables Attendues dans Bronze

| Table | Source | Volume Estimé |
|-------|--------|---------------|
| `etablissement_sante` | CSV | ~417K lignes |
| `professionnel_sante` | CSV | Variable |
| `activite_professionnel_sante` | CSV | Variable |
| `hospitalisation` | CSV | ~2.5K lignes |
| `deces_2019` | CSV (filtré) | ~620K lignes |
| `satisfaction_esatis48h_*` | CSV | Variable |
| `satisfaction_dpa_*` | CSV | Variable |
| `patients` | PostgreSQL | ⚠️ Si base peuplée |
| `consultations` | PostgreSQL | ⚠️ Si base peuplée |

---

## ⚠️ Points d'Attention

### 1. Driver PostgreSQL

Si vous voyez l'erreur :
```
ClassNotFoundException: org.postgresql.Driver
```

**Solution** : Ajouter le JAR PostgreSQL au spark-submit :
```bash
--jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar,/home/jovyan/jars/postgresql-42.6.0.jar
```

### 2. Base PostgreSQL vide

Si votre base PostgreSQL n'est pas encore peuplée, c'est normal. Le pipeline Bronze traitera uniquement les fichiers CSV disponibles.

**Pour peupler PostgreSQL** :
```bash
# Vérifier si la base contient des données
docker exec chu_postgres_data psql -U admin -d healthcare_data -c "\dt"

# Insérer des données de test si nécessaire
# (voir scripts dans /tools/)
```

### 3. Filtrage des Décès (2019 uniquement)

Le fichier `deces.csv` contient **25 millions de lignes**. Pour optimiser :
- Le pipeline filtre automatiquement sur l'année 2019
- Volume réduit à ~620K lignes
- Pour changer : modifier `read_postgres_table_safe()` dans `bronze_ingestion.py`

---

## 🎯 Prochaines Étapes

Une fois Bronze créé :

### 1. Transformation Silver

```bash
cd /home/alban/BigData/BigData
./run_silver_transformation.sh  # À créer ou utiliser existant
```

### 2. Agrégation Gold (KPIs)

```bash
cd /home/alban/BigData/BigData
./run_gold_aggregation.sh  # Déjà existant
```

### 3. Visualisation

- **Superset** : http://localhost:8088
- **Trino** : http://localhost:8090 (pour Power BI)

---

## 📂 Structure du Projet

```
/home/alban/BigData/BigData/
├── spark_jobs/
│   └── main_jobs/
│       ├── bronze_ingestion.py     ← Pipeline Bronze
│       ├── silver_transformation.py
│       └── gold_aggregation.py
├── run_bronze.sh                    ← Script d'exécution
├── verify_bronze.py                 ← Vérification
├── docs/
│   └── BRONZE_ARCHITECTURE.md      ← Documentation complète
└── data/
    └── source/
        ├── BDD PostgreSQL/
        ├── DECES EN FRANCE/
        ├── Etablissement de SANTE/
        ├── Hospitalisation/
        └── Satisfaction/
```

---

## 🔧 Dépannage

### Le job ne démarre pas

```bash
# Vérifier que les containers sont actifs
sudo docker-compose ps

# Redémarrer si nécessaire
sudo docker-compose restart chu_jupyter

# Vérifier les logs
docker logs chu_jupyter
```

### Erreur "No space left on device"

```bash
# Nettoyer les données temporaires
docker exec chu_jupyter bash -c "rm -rf /tmp/spark-*"

# Vérifier l'espace disque
df -h
```

### Performance lente

Le mode `LOW_RESOURCE_MODE=True` est activé par défaut (2GB RAM).

Pour augmenter (si vous avez assez de mémoire) :
```python
# Dans bronze_ingestion.py, ligne 27
LOW_RESOURCE_MODE = False  # Passera à 6GB driver, 8GB executor
```

---

## 📞 Support

**Fichiers de logs :**
- Spark UI : http://localhost:4040 (pendant l'exécution)
- Logs container : `docker logs chu_jupyter`

**Documentation :**
- Architecture Bronze : `docs/BRONZE_ARCHITECTURE.md`
- README principal : `README.md`

**Scripts utiles :**
- Vérification tables : `tools/check_table.py`
- Extraction CSV : `tools/extract_csv.py`
- Création table décès : `tools/create_deces_table.py`

---

## ✅ Checklist de Validation

Avant de passer à Silver, vérifiez :

- [ ] Le job Bronze s'exécute sans erreur
- [ ] Au moins 5 tables créées dans `s3a://bronze/`
- [ ] Les tables contiennent des données (> 0 lignes)
- [ ] Les dates sont correctement formatées
- [ ] Les données sensibles sont anonymisées (colonnes `*_anonymized`)
- [ ] Pas d'erreurs dans les logs Spark

---

**🎉 Votre zone Bronze est prête pour l'entrepôt de données CHU !**
