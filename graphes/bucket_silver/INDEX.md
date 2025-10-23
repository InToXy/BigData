# 📁 Index des Fichiers - Bucket Silver

## 📜 Scripts Python

| Fichier | Description | Taille | Lignes |
|---------|-------------|--------|--------|
| `performance_minio.py` | Script principal d'analyse de performance | ~15 KB | ~524 |
| `generer_rapport.py` | Générateur de rapport HTML interactif | ~11 KB | ~357 |

## 🔧 Scripts Bash

| Fichier | Description |
|---------|-------------|
| `generer_tout.sh` | Script d'automatisation complète (analyse + rapport) |

## 📊 Graphiques générés (après exécution)

| Fichier | Type | Description |
|---------|------|-------------|
| `1_temps_reponse_barres.png` | Bar Chart | Temps de réponse par dataset |
| `2_evolution_temporelle_courbes.png` | Line Chart | Évolution temporelle (cache) |
| `3_distribution_histogramme.png` | Histogram | Distribution des temps |
| `4_dispersion_boxplot.png` | Boxplot | Dispersion par type de requête |
| `4b_dispersion_boxplot_datasets.png` | Boxplot | Dispersion par dataset (top 15) |
| `5_correlation_scatter.png` | Scatter Plot | Corrélation volume/temps |
| `6_heatmap_latence.png` | Heatmap | Carte thermique des latences |
| `7_performance_debit.png` | Bar Chart | Débit par dataset |
| `8_dashboard_complet.png` | Dashboard | Vue d'ensemble complète |

## 📄 Rapports

| Fichier | Format | Description |
|---------|--------|-------------|
| `rapport_performance.html` | HTML | Rapport interactif avec tous les graphiques |

## 📚 Documentation

| Fichier | Contenu |
|---------|---------|
| `README.md` | Guide de démarrage rapide |
| `INDEX.md` | Ce fichier - Index des fichiers |
| `GUIDE_COMPLET.md` | Guide complet d'utilisation et d'interprétation |
| `README_GRAPHIQUES.md` | Documentation technique des graphiques |

## 🔄 Workflow typique

1. **Exécution** : `./generer_tout.sh` ou `python3 performance_minio.py`
2. **Génération** : 9 graphiques PNG créés automatiquement
3. **Rapport** : `python3 generer_rapport.py` génère le HTML
4. **Visualisation** : Ouvrir `rapport_performance.html` dans un navigateur

## 📊 Bucket concerné

- **Bucket MinIO** : `silver`
- **Endpoint** : `http://127.0.0.1:9000`
- **Type de données** : Parquet (transformées et nettoyées)
- **Découverte** : Automatique via S3 API

## 🏥 Projet

**CHU - Big Data Healthcare Analytics**  
Layer : **Silver** (Données transformées)
