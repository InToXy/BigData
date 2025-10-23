# 📖 Guide Complet - Analyse de Performance Silver Layer

## 📑 Table des matières

1. [Introduction](#introduction)
2. [Installation et Configuration](#installation-et-configuration)
3. [Utilisation](#utilisation)
4. [Interprétation des Graphiques](#interprétation-des-graphiques)
5. [Métriques de Performance](#métriques-de-performance)
6. [Recommandations](#recommandations)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Introduction

Ce guide explique comment utiliser et interpréter l'outil d'analyse de performance pour la **couche Silver** du data lake MinIO.

### Qu'est-ce que la couche Silver ?

La couche **Silver** contient les données :
- ✅ **Transformées** : ETL appliqué depuis Bronze
- ✅ **Nettoyées** : Données validées et cohérentes
- ✅ **Enrichies** : Jointures et calculs effectués
- ✅ **Normalisées** : Format standardisé

### Objectifs de l'analyse

- 📊 Mesurer les **performances de lecture** des données Silver
- 🔍 Identifier les **goulots d'étranglement**
- 💡 Optimiser les **requêtes** et le **stockage**
- 📈 Suivre l'évolution des **performances** dans le temps

---

## ⚙️ Installation et Configuration

### Prérequis système

```bash
# Python 3.10 ou supérieur
python3 --version

# Packages Python requis
pip install boto3 pyarrow pandas matplotlib seaborn numpy
```

### Configuration MinIO

Le script se connecte automatiquement à MinIO avec :
- **Endpoint** : `http://127.0.0.1:9000`
- **Access Key** : `minioadmin`
- **Secret Key** : `minioadmin123`
- **Bucket** : `silver`

Pour modifier ces paramètres, éditez les lignes 26-29 de `performance_minio.py`.

---

## 🚀 Utilisation

### Méthode 1 : Script automatique (recommandé)

```bash
cd /home/alban/BigData/BigData/graphes/bucket_silver
chmod +x generer_tout.sh
./generer_tout.sh
```

Ce script :
1. Vérifie la connexion MinIO
2. Exécute l'analyse de performance
3. Génère les 9 graphiques PNG
4. Crée le rapport HTML

### Méthode 2 : Exécution manuelle

```bash
# 1. Générer les graphiques
python3 performance_minio.py

# 2. Générer le rapport HTML
python3 generer_rapport.py
```

### Méthode 3 : Import Python

```python
# Dans un notebook Jupyter ou script Python
import subprocess
subprocess.run(['python3', 'performance_minio.py'])
```

---

## 📊 Interprétation des Graphiques

### 1️⃣ Temps de Réponse par Dataset

**Type** : Barres horizontales  
**Fichier** : `1_temps_reponse_barres.png`

**Lecture** :
- Chaque barre = temps de lecture d'un dataset
- Plus la barre est longue, plus le dataset est lent
- Valeurs affichées en secondes sur les barres

**Indicateurs** :
- ✅ **< 0.5s** : Performance excellente
- ⚠️ **0.5s - 2s** : Performance acceptable
- ❌ **> 2s** : Dataset à optimiser

**Actions** :
- Identifier les datasets > 2s
- Vérifier la taille et le nombre de colonnes
- Envisager le partitionnement

---

### 2️⃣ Évolution Temporelle (Cache)

**Type** : Graphique en courbes  
**Fichier** : `2_evolution_temporelle_courbes.png`

**Lecture** :
- 3 courbes : Froide (1ère lecture), Tiède (2ème), Chaude (3ème)
- Axe Y = temps de réponse
- Axe X = datasets (index)

**Interprétation** :
- **Courbe descendante** = cache efficace (normal)
- **Courbe stable/montante** = problème de cache
- **Écart Froide-Chaude** = impact du cache

**Indicateurs** :
- ✅ **Amélioration > 20%** : Cache très efficace
- ⚠️ **Amélioration 0-20%** : Cache peu efficace
- ❌ **Amélioration < 0%** : Problème (surcharge/conflit)

---

### 3️⃣ Distribution des Temps

**Type** : Histogramme  
**Fichier** : `3_distribution_histogramme.png`

**Lecture** :
- Axe X = plages de temps de réponse
- Axe Y = nombre de datasets dans chaque plage
- Ligne rouge = moyenne
- Ligne verte = médiane

**Interprétation** :
- **Distribution normale** : Performances homogènes
- **Distribution bimodale** : 2 groupes (rapides/lents)
- **Pics à droite** : Présence d'outliers

**Indicateurs** :
- **Moyenne ≈ Médiane** : Distribution symétrique ✅
- **Moyenne >> Médiane** : Outliers tirent vers le haut ⚠️

---

### 4️⃣ Dispersion par Type de Requête

**Type** : Boxplot  
**Fichier** : `4_dispersion_boxplot.png`

**Lecture** :
- Boîte = quartiles (Q1, médiane, Q3)
- Moustaches = min/max (hors outliers)
- Points = valeurs aberrantes

**Interprétation** :
- **Boîte petite** = performances stables ✅
- **Boîte large** = forte variabilité ⚠️
- **Nombreux outliers** = problème de cohérence ❌

**Comparaison Froide/Tiède/Chaude** :
- Requête Chaude doit avoir une boîte plus basse et plus petite

---

### 5️⃣ Dispersion par Dataset (Top 15)

**Type** : Boxplot multi-datasets  
**Fichier** : `4b_dispersion_boxplot_datasets.png`

**Lecture** :
- Un boxplot par dataset
- Compare la stabilité des 15 datasets les plus lents

**Actions** :
- Datasets avec larges boîtes → optimiser
- Datasets avec outliers → investiguer les pics

---

### 6️⃣ Corrélation Volume/Temps

**Type** : Scatter plot  
**Fichier** : `5_correlation_scatter.png`

**Lecture** :
- Axe X = taille du dataset (MB)
- Axe Y = temps de réponse (s)
- Taille des bulles = nombre de lignes
- Ligne rouge = tendance linéaire

**Interprétation** :
- **Points sur la ligne** = performance proportionnelle ✅
- **Points au-dessus** = sous-performance (à optimiser) ❌
- **Points en-dessous** = sur-performance (bien optimisé) ✅

**Indicateurs** :
- Corrélation forte (R² > 0.7) = comportement prévisible
- Corrélation faible (R² < 0.3) = facteurs multiples

---

### 7️⃣ Heatmap des Latences

**Type** : Carte thermique  
**Fichier** : `6_heatmap_latence.png`

**Lecture** :
- Lignes = datasets
- Colonnes = type de requête (Froide/Tiède/Chaude)
- Couleur = temps de réponse (jaune→rouge = lent)

**Interprétation** :
- **Diagonale de refroidissement** = cache normal
- **Ligne rouge uniforme** = dataset toujours lent
- **Colonne rouge** = problème de type de requête

---

### 8️⃣ Débit par Dataset

**Type** : Barres horizontales  
**Fichier** : `7_performance_debit.png`

**Lecture** :
- Débit en lignes/seconde (rows/s)
- Couleur verte = débit élevé
- Couleur rouge = débit faible

**Indicateurs** :
- ✅ **> 1M rows/s** : Excellent
- ⚠️ **100K - 1M rows/s** : Acceptable
- ❌ **< 100K rows/s** : À optimiser

---

### 9️⃣ Dashboard Récapitulatif

**Type** : Composite (5 panneaux)  
**Fichier** : `8_dashboard_complet.png`

**Contenu** :
1. Top 10 temps de réponse (bar)
2. Répartition des tailles (pie chart)
3. Distribution par type de requête (boxplot)
4. Volume vs temps (scatter)
5. Top 10 débit (bar)

**Usage** : Vue d'ensemble rapide pour présentation

---

## 📈 Métriques de Performance

### Métriques principales

| Métrique | Formule | Interprétation |
|----------|---------|----------------|
| **Temps de réponse** | `end_time - start_time` | Latence de lecture |
| **Débit** | `rows / time` | Lignes par seconde |
| **Throughput MB/s** | `size_mb / time` | Bande passante |
| **Coefficient de variation** | `(std / mean) × 100` | Variabilité (%) |

### Seuils de performance

**Temps de réponse** :
- 🟢 Excellent : < 0.5s
- 🟡 Acceptable : 0.5s - 2s
- 🔴 Problématique : > 2s

**Débit** :
- 🟢 Excellent : > 1M rows/s
- 🟡 Acceptable : 100K - 1M rows/s
- 🔴 Problématique : < 100K rows/s

**Cache** :
- 🟢 Efficace : Amélioration > 20%
- 🟡 Modéré : Amélioration 0-20%
- 🔴 Inefficace : Amélioration < 0%

**Variabilité (CV)** :
- 🟢 Stable : CV < 50%
- 🟡 Modéré : CV 50-100%
- 🔴 Instable : CV > 100%

---

## 💡 Recommandations

### Optimisation des performances

#### 1. Datasets lents (> 2s)

**Diagnostic** :
```python
# Vérifier la taille du dataset
size_mb = df.memory_usage(deep=True).sum() / (1024**2)

# Compter les colonnes
num_columns = len(df.columns)

# Vérifier le schéma
df.dtypes
```

**Solutions** :
- ✅ Partitionner par date/catégorie
- ✅ Réduire le nombre de colonnes
- ✅ Convertir types de données (int64 → int32)
- ✅ Compresser avec Snappy ou Gzip

#### 2. Cache inefficace (< 0%)

**Causes possibles** :
- Surcharge mémoire
- Éviction du cache
- Conflit de ressources

**Solutions** :
- ✅ Augmenter la RAM du conteneur MinIO
- ✅ Optimiser la taille des datasets
- ✅ Répartir les lectures dans le temps

#### 3. Forte variabilité (CV > 100%)

**Causes** :
- Réseau instable
- Charge système variable
- Taille hétérogène des datasets

**Solutions** :
- ✅ Vérifier la connectivité réseau
- ✅ Isoler les ressources Docker
- ✅ Normaliser la taille des partitions

#### 4. Faible débit (< 100K rows/s)

**Solutions** :
- ✅ Augmenter la taille des chunks Parquet
- ✅ Utiliser compression adaptée (Snappy pour vitesse)
- ✅ Paralléliser les lectures
- ✅ Optimiser le schéma (types de données)

---

## 🔧 Troubleshooting

### Problème : "Aucun dataset trouvé"

**Cause** : Bucket silver vide ou inaccessible

**Solution** :
```bash
# Vérifier le bucket
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://silver/

# Lancer la transformation Bronze → Silver
cd /home/alban/BigData
docker exec chu_spark spark-submit /spark_jobs/main_jobs/silver_transformation.py
```

---

### Problème : "Connexion MinIO échouée"

**Cause** : MinIO non démarré

**Solution** :
```bash
# Vérifier les conteneurs
docker ps | grep minio

# Démarrer MinIO
cd /home/alban/BigData/BigData
docker-compose up -d chu_minio
```

---

### Problème : "Module not found: boto3"

**Cause** : Packages Python manquants

**Solution** :
```bash
pip install boto3 pyarrow pandas matplotlib seaborn numpy
```

---

### Problème : Graphiques vides ou erreurs

**Cause** : Données corrompues ou format incorrect

**Solution** :
```python
# Vérifier un dataset manuellement
import pyarrow.parquet as pq
table = pq.read_table('silver/patients', filesystem=s3)
print(table.schema)
print(table.num_rows)
```

---

### Problème : Performances dégradées

**Diagnostic** :
```bash
# Vérifier les ressources Docker
docker stats chu_minio

# Vérifier l'espace disque
df -h
```

**Solution** :
```bash
# Augmenter les ressources dans docker-compose.yml
mem_limit: 4g
cpus: 2.0
```

---

## 📚 Références

### Documentation officielle

- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/minio-py.html)
- [PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [Matplotlib](https://matplotlib.org/stable/users/index.html)
- [Seaborn](https://seaborn.pydata.org/tutorial.html)

### Architecture du projet

```
Silver Layer
├── Source : Bronze Layer (données brutes)
├── Transformations : Nettoyage, jointures, enrichissement
├── Format : Parquet avec compression Snappy
└── Destination : Analyses et agrégations (Gold Layer)
```

---

## 🏥 Contact et Support

**Projet** : CHU - Big Data Healthcare Analytics  
**Layer** : Silver (Données transformées)  
**Version** : 1.0  
**Date** : Octobre 2025
