# 🧪 Tests d'Ingestion - Bucket Bronze

## 📋 Description

Ce dossier contient des scripts de test pour vérifier l'ingestion des données dans le bucket Bronze de MinIO.

## 📊 Scripts disponibles

### `test_consultation_bronze.py`

Script de test complet pour la table **consultations** du bucket Bronze.

**Fonctionnalités** :
- ✅ Vérification de la connexion MinIO
- ✅ Vérification de l'existence de la table
- ✅ Lecture et affichage des données Parquet
- ✅ Statistiques descriptives complètes
- ✅ Analyse de qualité des données
- ✅ Détection des doublons
- ✅ Identification des valeurs manquantes
- ✅ Distribution des valeurs catégorielles
- ✅ Informations sur les fichiers Parquet

**Affichage** :
1. Informations générales (nombre de lignes, colonnes, taille)
2. Schéma de la table (types de données)
3. Statistiques descriptives
4. Échantillon des 10 premières lignes
5. Échantillon des 10 dernières lignes
6. Vérifications de qualité
7. Informations sur les fichiers Parquet

### `run_test.sh`

Script bash pour lancer facilement le test avec vérification préalable de MinIO.

## 🚀 Utilisation

### Méthode 1 : Script bash (recommandé)

```bash
cd /home/alban/BigData/BigData/graphes/tests_ingestion
chmod +x run_test.sh
./run_test.sh
```

### Méthode 2 : Python direct

```bash
cd /home/alban/BigData/BigData/graphes/tests_ingestion
python3 test_consultation_bronze.py
```

## 📦 Prérequis

### Packages Python

```bash
pip install boto3 pyarrow pandas
```

### Services

- **MinIO** : Doit être accessible sur `http://127.0.0.1:9000`
- **Bucket Bronze** : Doit contenir la table `consultations`

## 🔍 Vérifications préalables

```bash
# Vérifier que MinIO est démarré
docker ps | grep minio

# Vérifier le bucket Bronze
docker exec chu_minio mc ls local/bronze/

# Vérifier la table consultations
docker exec chu_minio mc ls local/bronze/consultations/
```

## 📊 Exemple de sortie

```
================================================================================
🔍 TEST D'INGESTION - Table consultation (Bucket Bronze)
================================================================================
📅 Date : 23/10/2025 17:55:30

1️⃣  Connexion à MinIO...
   ✅ Connexion établie au bucket 'bronze'

2️⃣  Vérification de la table...
   ✅ Table 'consultations' trouvée

3️⃣  Lecture des données Parquet...
   ✅ Données chargées avec succès

================================================================================
📊 INFORMATIONS SUR LA TABLE
================================================================================
Nombre de lignes    : 1,027,157
Nombre de colonnes  : 12
Taille en mémoire   : 422.71 MB

================================================================================
📋 SCHÉMA DE LA TABLE
================================================================================
Colonne                        Type                 Nulls     
--------------------------------------------------------------------------------
consultation_id                int64                0         
patient_id                     int64                0         
professionnel_id               int64                0         
date_consultation              object               0         
...
```

## 🐛 Troubleshooting

### Erreur : "Connexion MinIO échouée"

```bash
# Démarrer MinIO
cd /home/alban/BigData/BigData
docker-compose up -d chu_minio
```

### Erreur : "Table 'consultations' n'existe pas"

```bash
# Lancer l'ingestion Bronze
docker exec chu_spark spark-submit /spark_jobs/main_jobs/bronze_ingestion.py
```

### Erreur : "Module not found: boto3"

```bash
# Installer les dépendances
pip install boto3 pyarrow pandas
```

## 📝 Créer un nouveau test

Pour tester une autre table, dupliquez et modifiez le script :

```python
# Changer la variable TABLE
TABLE = "patients"  # Au lieu de "consultations"
```

Ou créez un nouveau script :

```bash
cp test_consultation_bronze.py test_patients_bronze.py
# Puis éditez test_patients_bronze.py
```

## 🎯 Utilisation dans un pipeline

```bash
# Dans un script Airflow ou autre
cd /home/alban/BigData/BigData/graphes/tests_ingestion
./run_test.sh

# Récupérer le code de sortie
if [ $? -eq 0 ]; then
    echo "✅ Validation réussie"
else
    echo "❌ Validation échouée"
    exit 1
fi
```

## 📊 Métriques validées

Le script vérifie automatiquement :
- ✅ Présence de la table
- ✅ Nombre de lignes > 0
- ✅ Schéma des colonnes
- ✅ Types de données
- ✅ Valeurs manquantes
- ✅ Doublons
- ✅ Distribution des valeurs
- ✅ Plages de valeurs numériques
- ✅ Fichiers Parquet créés

## 🏥 Projet

**CHU - Big Data Healthcare Analytics**  
**Layer** : Bronze (Données brutes)  
**Format** : Apache Parquet avec compression Snappy  
**Storage** : MinIO (S3-compatible)
