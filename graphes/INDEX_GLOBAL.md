# 📑 Index Global - Analyses de Performance MinIO

## 🗂️ Structure complète

```
graphes/
│
├── 📄 README.md                         # Guide principal de démarrage
├── 📄 COMPARAISON_BRONZE_SILVER.md     # Comparatif détaillé Bronze vs Silver
├── 🔧 analyser_tout.sh                 # Script global (Bronze + Silver)
│
├── 📁 bucket_bronze/                    # Analyses du bucket Bronze
│   ├── 📜 performance_minio.py         # Script principal d'analyse (524 lignes)
│   ├── 📜 generer_rapport.py           # Générateur de rapport HTML (357 lignes)
│   ├── 🔧 generer_tout.sh              # Script d'automatisation
│   ├── 📄 README.md                    # Guide de démarrage rapide
│   ├── 📄 GUIDE_COMPLET.md             # Documentation complète
│   ├── 📄 INDEX.md                     # Index des fichiers Bronze
│   ├── 📄 README_GRAPHIQUES.md         # Documentation technique
│   ├── 📊 rapport_performance.html     # Rapport interactif (généré)
│   └── 📊 [9 graphiques PNG]           # Graphiques de performance (générés)
│
└── 📁 bucket_silver/                    # Analyses du bucket Silver
    ├── 📜 performance_minio.py         # Script principal d'analyse (524 lignes)
    ├── 📜 generer_rapport.py           # Générateur de rapport HTML
    ├── 🔧 generer_tout.sh              # Script d'automatisation
    ├── 📄 README.md                    # Guide de démarrage rapide
    ├── 📄 GUIDE_COMPLET.md             # Documentation complète
    ├── 📄 INDEX.md                     # Index des fichiers Silver
    ├── 📄 README_GRAPHIQUES.md         # Documentation technique
    ├── 📊 rapport_performance.html     # Rapport interactif (généré)
    └── 📊 [9 graphiques PNG]           # Graphiques de performance (générés)
```

## 📚 Documentation par niveau

### Niveau 1 : Démarrage rapide (5 minutes)

```bash
# Lire en premier
cat README.md

# Exécuter l'analyse complète
./analyser_tout.sh
```

**Fichiers** :
- `README.md` - Vue d'ensemble et commandes essentielles

### Niveau 2 : Utilisation courante (15 minutes)

```bash
# Bronze
cd bucket_bronze
cat README.md
./generer_tout.sh

# Silver
cd ../bucket_silver
cat README.md
./generer_tout.sh
```

**Fichiers** :
- `bucket_bronze/README.md` - Guide Bronze
- `bucket_silver/README.md` - Guide Silver

### Niveau 3 : Compréhension approfondie (1 heure)

```bash
# Comparaison des layers
cat COMPARAISON_BRONZE_SILVER.md

# Guide complet Bronze
cat bucket_bronze/GUIDE_COMPLET.md

# Guide complet Silver
cat bucket_silver/GUIDE_COMPLET.md
```

**Fichiers** :
- `COMPARAISON_BRONZE_SILVER.md` - Différences Bronze/Silver
- `GUIDE_COMPLET.md` (Bronze) - Interprétation, métriques, troubleshooting
- `GUIDE_COMPLET.md` (Silver) - Interprétation, métriques, troubleshooting

### Niveau 4 : Documentation technique (2 heures)

```bash
# Documentation technique des graphiques
cat bucket_bronze/README_GRAPHIQUES.md
cat bucket_silver/README_GRAPHIQUES.md

# Index des fichiers
cat bucket_bronze/INDEX.md
cat bucket_silver/INDEX.md
```

**Fichiers** :
- `README_GRAPHIQUES.md` - Spécifications techniques des visualisations
- `INDEX.md` - Inventaire détaillé des fichiers

## 🚀 Scripts disponibles

### Scripts globaux (racine)

| Script | Description | Usage |
|--------|-------------|-------|
| `analyser_tout.sh` | Analyse Bronze + Silver en une commande | `./analyser_tout.sh` |

### Scripts Bronze (bucket_bronze/)

| Script | Description | Usage |
|--------|-------------|-------|
| `performance_minio.py` | Analyse de performance du bucket Bronze | `python3 performance_minio.py` |
| `generer_rapport.py` | Génère le rapport HTML interactif | `python3 generer_rapport.py` |
| `generer_tout.sh` | Automatisation complète (graphiques + rapport) | `./generer_tout.sh` |

### Scripts Silver (bucket_silver/)

| Script | Description | Usage |
|--------|-------------|-------|
| `performance_minio.py` | Analyse de performance du bucket Silver | `python3 performance_minio.py` |
| `generer_rapport.py` | Génère le rapport HTML interactif | `python3 generer_rapport.py` |
| `generer_tout.sh` | Automatisation complète (graphiques + rapport) | `./generer_tout.sh` |

## 📊 Graphiques générés

### Pour chaque bucket (Bronze et Silver)

| # | Fichier | Type | Description |
|---|---------|------|-------------|
| 1 | `1_temps_reponse_barres.png` | Bar Chart | Temps de réponse par dataset |
| 2 | `2_evolution_temporelle_courbes.png` | Line Chart | Évolution cache (cold/warm/hot) |
| 3 | `3_distribution_histogramme.png` | Histogram | Distribution statistique des temps |
| 4 | `4_dispersion_boxplot.png` | Boxplot | Dispersion par type de requête |
| 5 | `4b_dispersion_boxplot_datasets.png` | Boxplot | Dispersion par dataset (top 15) |
| 6 | `5_correlation_scatter.png` | Scatter Plot | Corrélation volume vs temps |
| 7 | `6_heatmap_latence.png` | Heatmap | Carte thermique des latences |
| 8 | `7_performance_debit.png` | Bar Chart | Débit (lignes/seconde) |
| 9 | `8_dashboard_complet.png` | Dashboard | Vue d'ensemble (5 panneaux) |

**Total** : 18 graphiques (9 Bronze + 9 Silver)

## 📄 Rapports HTML

### Rapports interactifs

| Fichier | Bucket | Contenu |
|---------|--------|---------|
| `bucket_bronze/rapport_performance.html` | Bronze | Rapport avec 9 graphiques + statistiques |
| `bucket_silver/rapport_performance.html` | Silver | Rapport avec 9 graphiques + statistiques |

**Visualisation** :
```bash
# Bronze
xdg-open bucket_bronze/rapport_performance.html

# Silver
xdg-open bucket_silver/rapport_performance.html
```

## 🔍 Guide de navigation

### Objectif : "Je veux démarrer rapidement"
➡️ Lire `README.md` → Exécuter `./analyser_tout.sh`

### Objectif : "Analyser uniquement Bronze"
➡️ `cd bucket_bronze` → `./generer_tout.sh`

### Objectif : "Analyser uniquement Silver"
➡️ `cd bucket_silver` → `./generer_tout.sh`

### Objectif : "Comprendre les différences Bronze/Silver"
➡️ Lire `COMPARAISON_BRONZE_SILVER.md`

### Objectif : "Interpréter les graphiques"
➡️ Lire `bucket_bronze/GUIDE_COMPLET.md` section "Interprétation"

### Objectif : "Modifier les graphiques"
➡️ Lire `README_GRAPHIQUES.md` section "Personnalisation"

### Objectif : "Résoudre un problème"
➡️ Lire `GUIDE_COMPLET.md` section "Troubleshooting"

### Objectif : "Comprendre les métriques"
➡️ Lire `GUIDE_COMPLET.md` section "Métriques de Performance"

## 📐 Taille des fichiers

### Documentation (Markdown)

```
README.md                      : ~11 KB
COMPARAISON_BRONZE_SILVER.md   : ~11 KB
bucket_bronze/README.md        : ~3 KB
bucket_bronze/GUIDE_COMPLET.md : ~18 KB
bucket_bronze/INDEX.md         : ~3 KB
bucket_bronze/README_GRAPHIQUES.md : ~12 KB
bucket_silver/README.md        : ~3 KB
bucket_silver/GUIDE_COMPLET.md : ~18 KB
bucket_silver/INDEX.md         : ~3 KB
bucket_silver/README_GRAPHIQUES.md : ~12 KB

Total documentation : ~94 KB
```

### Scripts Python

```
bucket_bronze/performance_minio.py : ~15 KB (524 lignes)
bucket_bronze/generer_rapport.py   : ~11 KB (357 lignes)
bucket_silver/performance_minio.py : ~15 KB (524 lignes)
bucket_silver/generer_rapport.py   : ~11 KB (357 lignes)

Total scripts Python : ~52 KB (1,762 lignes)
```

### Scripts Bash

```
analyser_tout.sh                   : ~6 KB
bucket_bronze/generer_tout.sh      : ~1.5 KB
bucket_silver/generer_tout.sh      : ~1.5 KB

Total scripts Bash : ~9 KB
```

### Graphiques (après génération)

```
Chaque graphique PNG : ~150-250 KB (résolution 150 DPI)
Total 18 graphiques  : ~3.6 MB
```

### Rapports HTML (après génération)

```
Chaque rapport HTML : ~15 KB
Total 2 rapports    : ~30 KB
```

**Taille totale projet** : ~155 KB (sans graphiques générés)  
**Taille avec graphiques** : ~3.8 MB

## 🔧 Maintenance

### Mise à jour des scripts

```bash
# Modifier le script Bronze
nano bucket_bronze/performance_minio.py

# Modifier le script Silver
nano bucket_silver/performance_minio.py

# Tester
cd bucket_bronze && python3 performance_minio.py
cd ../bucket_silver && python3 performance_minio.py
```

### Ajouter un nouveau graphique

1. Éditer `performance_minio.py` (section graphiques)
2. Ajouter la génération du graphique
3. Mettre à jour `README_GRAPHIQUES.md`
4. Tester la génération
5. Mettre à jour `generer_rapport.py` si nécessaire

### Mettre à jour la documentation

```bash
# Guides de démarrage
nano README.md
nano bucket_bronze/README.md
nano bucket_silver/README.md

# Guides complets
nano bucket_bronze/GUIDE_COMPLET.md
nano bucket_silver/GUIDE_COMPLET.md

# Documentation technique
nano bucket_bronze/README_GRAPHIQUES.md
nano bucket_silver/README_GRAPHIQUES.md
```

## 📞 Support et ressources

### Documentation interne

- **README.md** : Point d'entrée principal
- **GUIDE_COMPLET.md** : Documentation exhaustive
- **README_GRAPHIQUES.md** : Référence technique
- **COMPARAISON_BRONZE_SILVER.md** : Comparatif détaillé

### Commandes utiles

```bash
# Vérifier MinIO
curl http://127.0.0.1:9000/minio/health/live

# Lister les buckets
docker exec chu_minio mc ls local/

# Vérifier les conteneurs
docker ps | grep chu

# Voir les logs
docker logs chu_minio
docker logs chu_spark
```

### Dépendances

```bash
# Installation
pip install boto3 pyarrow pandas matplotlib seaborn numpy

# Vérification
python3 -c "import boto3, pyarrow, pandas, matplotlib, seaborn, numpy; print('OK')"
```

## 🏥 Contexte projet

**Nom** : CHU - Big Data Healthcare Analytics  
**Architecture** : Medallion (Bronze/Silver/Gold)  
**Technologies** : MinIO, Spark, Parquet, Python  
**Objectif** : Analyse de performance du data lake médical  

---

**Version** : 2.0  
**Date** : Octobre 2025  
**Auteur** : CHU Big Data Team
