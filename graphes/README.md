# 📊 Analyses de Performance MinIO - Data Lake CHU# 📊 Analyse de Performance - Data Lake MinIO



## 🏗️ Structure du projet## 🎯 Vue d'ensemble



```Système complet d'analyse de performance pour le Data Lake MinIO (couche Bronze).

graphes/Génère automatiquement **9 graphiques** et un **rapport HTML interactif**.

├── bucket_bronze/          # Analyses du bucket Bronze (données brutes)

│   ├── performance_minio.py## ⚡ Quick Start

│   ├── generer_rapport.py

│   ├── generer_tout.sh```bash

│   ├── README.md# Générer tous les graphiques et le rapport

│   ├── GUIDE_COMPLET.md./generer_tout.sh

│   ├── INDEX.md

│   ├── README_GRAPHIQUES.md# Ou manuellement :

│   └── [graphiques générés]python3 performance_minio.py    # Génère 9 graphiques

│python3 generer_rapport.py      # Crée le rapport HTML

├── bucket_silver/          # Analyses du bucket Silver (données transformées)```

│   ├── performance_minio.py

│   ├── generer_rapport.py## 📊 Graphiques Fournis

│   ├── generer_tout.sh

│   ├── README.md| # | Type | Fichier | Description |

│   ├── GUIDE_COMPLET.md|---|------|---------|-------------|

│   ├── INDEX.md| 1 | **Barres** | `1_temps_reponse_barres.png` | Temps de réponse par dataset |

│   ├── README_GRAPHIQUES.md| 2 | **Courbes** ⭐ | `2_evolution_temporelle_courbes.png` | Cache chaud/froid |

│   └── [graphiques générés]| 3 | **Histogramme** ⭐ | `3_distribution_histogramme.png` | Distribution des temps |

│| 4 | **Boxplot** ⭐ | `4_dispersion_boxplot.png` | Dispersion par type |

└── README.md              # Ce fichier| 5 | **Boxplot** | `4b_dispersion_boxplot_datasets.png` | Dispersion par dataset |

```| 6 | **Scatter** ⭐ | `5_correlation_scatter.png` | Corrélation volume/temps |

| 7 | **Heatmap** ⭐ | `6_heatmap_latence.png` | Carte thermique |

## 🎯 Architecture du Data Lake| 8 | **Barres** | `7_performance_debit.png` | Débit de lecture |

| 9 | **Dashboard** ⭐⭐⭐ | `8_dashboard_complet.png` | Vue d'ensemble 6-en-1 |

### Couche Bronze (Données Brutes)

- **Bucket** : `bronze`✅ **Tous les types demandés sont fournis** : Courbes, Histogramme, Boxplot, Scatter, Heatmap

- **Source** : PostgreSQL + CSV (données médicales brutes)

- **Format** : Parquet avec compression Snappy## 📈 Résultats (Dernière Exécution)

- **Transformation** : Aucune (données brutes)

- **Datasets** : ~28 tables (découverte automatique)```

Datasets analysés : 10

### Couche Silver (Données Transformées)Total de lignes   : 5.15M

- **Bucket** : `silver`Taille totale     : 2.09 GB

- **Source** : Bucket BronzeDébit moyen       : 911,632 lignes/s ⚡

- **Format** : Parquet avec compression SnappyTemps moyen       : 0.59s par requête

- **Transformation** : Nettoyage, validation, enrichissement, jointures```

- **Datasets** : ~10 tables (dimensions + faits + métriques)

## 📚 Documentation

## 🚀 Utilisation rapide

- **[INDEX.md](./INDEX.md)** - Vue d'ensemble et index des fichiers

### Analyser le bucket Bronze- **[GUIDE_COMPLET.md](./GUIDE_COMPLET.md)** - Guide utilisateur complet

- **[README_GRAPHIQUES.md](./README_GRAPHIQUES.md)** - Documentation technique

```bash

cd bucket_bronze## 🔧 Prérequis

./generer_tout.sh

``````bash

# Installer les dépendances

Génère :pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy

- 9 graphiques PNG de performance

- 1 rapport HTML interactif# Vérifier que MinIO est démarré

- Statistiques complètes (temps, débit, cache)docker ps | grep minio

```

### Analyser le bucket Silver

## 🌐 Rapport HTML

```bash

cd bucket_silverUn rapport HTML interactif est généré automatiquement :

./generer_tout.sh

``````bash

# Générer le rapport

Génère :python3 generer_rapport.py

- 9 graphiques PNG de performance

- 1 rapport HTML interactif# Ouvrir dans le navigateur (WSL)

- Métriques de transformation ETLexplorer.exe rapport_performance.html

```

## 📊 Graphiques générés (pour chaque bucket)

## 📁 Structure

| # | Nom | Description |

|---|-----|-------------|```

| 1 | `1_temps_reponse_barres.png` | Temps de lecture par dataset |graphes/

| 2 | `2_evolution_temporelle_courbes.png` | Analyse du cache (cold/warm/hot) |├── generer_tout.sh              🚀 Script de génération complète

| 3 | `3_distribution_histogramme.png` | Distribution statistique |├── performance_minio.py         ⭐ Script principal

| 4 | `4_dispersion_boxplot.png` | Dispersion par type de requête |├── generer_rapport.py           📄 Générateur HTML

| 5 | `4b_dispersion_boxplot_datasets.png` | Dispersion par dataset (top 15) |├── rapport_performance.html     🌐 Rapport interactif

| 6 | `5_correlation_scatter.png` | Corrélation volume/temps |├── INDEX.md                     📍 Index

| 7 | `6_heatmap_latence.png` | Carte thermique des latences |├── GUIDE_COMPLET.md             📖 Guide complet

| 8 | `7_performance_debit.png` | Débit (lignes/seconde) |├── README.md                    📘 Ce fichier

| 9 | `8_dashboard_complet.png` | Dashboard récapitulatif |└── [9 fichiers PNG]             📊 Graphiques

```

## 🔧 Installation

## 🎯 Cas d'Usage

### Prérequis

### Pour une présentation

```bash→ Utiliser `8_dashboard_complet.png` ou ouvrir `rapport_performance.html`

# Python 3.10+

python3 --version### Pour un rapport écrit

→ Insérer les PNG individuels dans Word/PowerPoint

# Packages requis

pip install boto3 pyarrow pandas matplotlib seaborn numpy### Pour une analyse détaillée

```→ Lire `GUIDE_COMPLET.md` et examiner chaque graphique



### Vérifier MinIO## 💡 Conseils



```bash- **Exécuter régulièrement** : Quotidien en prod, hebdomadaire en dev

# MinIO doit être accessible- **Comparer les résultats** : Suivre l'évolution dans le temps

curl http://127.0.0.1:9000/minio/health/live- **Optimiser les lents** : Focus sur top 3 datasets les plus lents



# Vérifier les buckets## ⚠️ Troubleshooting

docker exec chu_minio mc ls local/

```**MinIO non accessible** :

```bash

## 📈 Métriques analyséesdocker restart chu_minio

```

### Performances de lecture

- **Temps de réponse** : Latence de lecture (secondes)**Dépendances manquantes** :

- **Débit** : Lignes lues par seconde```bash

- **Throughput** : Mégaoctets par secondepip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy

- **Taille** : Volume de données en mémoire```



### Analyse du cache**Cache négatif** :

- **Requête Froide** : Première lecture (cache vide)- Fermer applications lourdes

- **Requête Tiède** : Deuxième lecture (cache partiel)- Exécuter à un moment moins chargé

- **Requête Chaude** : Troisième lecture (cache plein)- Augmenter mémoire WSL

- **Amélioration** : Gain de performance du cache

## 📅 Informations

### Statistiques

- **Moyenne** : Temps moyen de lecture- **Projet** : CHU Big Data Healthcare Analytics

- **Médiane** : Temps médian (robuste aux outliers)- **Composant** : Analyse Performance Data Lake

- **Écart-type** : Variabilité des performances- **Version** : 2.0 (Octobre 2025)

- **Coefficient de variation** : Stabilité (%)- **Auteur** : Système d'analyse automatisé

- **Quartiles** : Q1, Q2 (médiane), Q3

---

## 🎓 Documentation détaillée

🚀 **Pour commencer** : `./generer_tout.sh`

### Pour le bucket Bronze

```bash
cd bucket_bronze

# Démarrage rapide
cat README.md

# Guide complet
cat GUIDE_COMPLET.md

# Documentation technique des graphiques
cat README_GRAPHIQUES.md

# Index des fichiers
cat INDEX.md
```

### Pour le bucket Silver

```bash
cd bucket_silver

# Démarrage rapide
cat README.md

# Guide complet
cat GUIDE_COMPLET.md

# Documentation technique des graphiques
cat README_GRAPHIQUES.md

# Index des fichiers
cat INDEX.md
```

## ⚡ Commandes rapides

### Générer tout (Bronze + Silver)

```bash
# Depuis le dossier graphes/
cd bucket_bronze && ./generer_tout.sh && cd ../bucket_silver && ./generer_tout.sh
```

### Générer uniquement les graphiques (sans rapport HTML)

```bash
# Bronze
cd bucket_bronze && python3 performance_minio.py

# Silver
cd bucket_silver && python3 performance_minio.py
```

### Générer uniquement le rapport HTML

```bash
# Bronze
cd bucket_bronze && python3 generer_rapport.py

# Silver
cd bucket_silver && python3 generer_rapport.py
```

## 🔍 Découverte automatique

Les scripts détectent **automatiquement** tous les datasets dans chaque bucket via l'API S3 de MinIO :

```python
# Aucune configuration manuelle nécessaire !
response = s3_client.list_objects_v2(Bucket=BUCKET, Delimiter='/')
datasets = [prefix['Prefix'].rstrip('/') for prefix in response['CommonPrefixes']]
```

**Avantages** :
- ✅ Pas de liste codée en dur
- ✅ S'adapte automatiquement aux nouveaux datasets
- ✅ Production-ready
- ✅ Maintenance simplifiée

## 📊 Résultats attendus

### Bronze Layer
```
✅ 28 dataset(s) détecté(s)
📊 Total de lignes: 7,435,042
💾 Taille totale: 2910.70 MB
⚡ Débit moyen: 1,280,073 lignes/seconde
```

### Silver Layer
```
✅ 10 dataset(s) détecté(s)
📊 Total de lignes: ~1,771,000
💾 Taille totale: ~700 MB
⚡ Débit moyen: ~800,000 lignes/seconde
```

## 🐛 Troubleshooting

### Erreur : "Aucun dataset trouvé"

**Bronze** :
```bash
# Vérifier l'ingestion
cd /home/alban/BigData
docker exec chu_spark spark-submit /spark_jobs/main_jobs/bronze_ingestion.py
```

**Silver** :
```bash
# Vérifier la transformation
cd /home/alban/BigData
docker exec chu_spark spark-submit /spark_jobs/main_jobs/silver_transformation.py
```

### Erreur : "Connexion MinIO échouée"

```bash
# Vérifier les conteneurs
docker ps | grep minio

# Démarrer MinIO si nécessaire
cd /home/alban/BigData/BigData
docker-compose up -d chu_minio
```

### Erreur : "Module not found"

```bash
# Installer les dépendances
pip install boto3 pyarrow pandas matplotlib seaborn numpy
```

## 📦 Dépendances

| Package | Version | Usage |
|---------|---------|-------|
| `boto3` | ≥1.26.0 | Client S3 pour MinIO |
| `pyarrow` | ≥11.0.0 | Lecture fichiers Parquet |
| `pandas` | ≥1.5.0 | Manipulation de données |
| `matplotlib` | ≥3.6.0 | Génération de graphiques |
| `seaborn` | ≥0.12.0 | Visualisations avancées |
| `numpy` | ≥1.24.0 | Calculs numériques |

## 🏥 Projet CHU - Big Data Healthcare Analytics

### Contexte
Analyse de performance d'un data lake médical avec architecture Medallion (Bronze/Silver/Gold).

### Technologies
- **Storage** : MinIO (S3-compatible)
- **Format** : Apache Parquet
- **Processing** : Apache Spark
- **Database** : PostgreSQL
- **Orchestration** : Apache Airflow
- **Visualization** : Python (Matplotlib, Seaborn)

### Objectifs
- ✅ Surveiller les performances de lecture
- ✅ Optimiser le stockage et le cache
- ✅ Détecter les goulots d'étranglement
- ✅ Garantir la qualité de service

## 📞 Support

Pour toute question :
1. Consulter `GUIDE_COMPLET.md` dans le bucket concerné
2. Vérifier `README_GRAPHIQUES.md` pour les détails techniques
3. Examiner les logs d'exécution

---

**Version** : 2.0  
**Date** : Octobre 2025  
**Auteur** : CHU Big Data Team
