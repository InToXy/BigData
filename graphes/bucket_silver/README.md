# 📊 Analyse de Performance MinIO - Silver Layer

## 🚀 Démarrage rapide

```bash
# Méthode 1 : Script automatique tout-en-un
chmod +x generer_tout.sh
./generer_tout.sh

# Méthode 2 : Étape par étape
python3 performance_minio.py    # Génère les graphiques
python3 generer_rapport.py      # Génère le rapport HTML
```

## 📋 Prérequis

- **Python 3.10+** avec les packages :
  - `boto3` : Client S3 pour MinIO
  - `pyarrow` : Lecture des fichiers Parquet
  - `pandas` : Manipulation de données
  - `matplotlib` : Génération de graphiques
  - `seaborn` : Visualisations avancées
  - `numpy` : Calculs numériques

- **MinIO** : Serveur accessible sur `http://127.0.0.1:9000`
- **Bucket Silver** : Contenant les données transformées au format Parquet

## 📦 Installation des dépendances

```bash
pip install boto3 pyarrow pandas matplotlib seaborn numpy
```

## 📊 Fichiers générés

Après exécution, vous obtiendrez :

### Graphiques PNG (haute résolution 150 DPI)

1. **1_temps_reponse_barres.png** - Temps de réponse par dataset
2. **2_evolution_temporelle_courbes.png** - Évolution cache froid/chaud
3. **3_distribution_histogramme.png** - Distribution des temps
4. **4_dispersion_boxplot.png** - Boxplot par type de requête
5. **4b_dispersion_boxplot_datasets.png** - Boxplot top 15 datasets
6. **5_correlation_scatter.png** - Corrélation volume/temps
7. **6_heatmap_latence.png** - Carte thermique
8. **7_performance_debit.png** - Débit par dataset
9. **8_dashboard_complet.png** - Dashboard récapitulatif

### Rapport HTML

- **rapport_performance.html** - Rapport interactif avec toutes les métriques

## 🎯 Fonctionnalités

- ✅ **Découverte automatique** des datasets Silver
- ✅ **Analyse multi-passes** (cache cold/warm/hot)
- ✅ **9 graphiques professionnels** de performance
- ✅ **Rapport HTML interactif** avec métriques détaillées
- ✅ **Statistiques complètes** : débit, latence, dispersion
- ✅ **Détection automatique** des outliers et datasets lents

## 📖 Documentation complète

Voir `GUIDE_COMPLET.md` pour plus de détails sur :
- L'interprétation des graphiques
- Les métriques de performance
- Les recommandations d'optimisation
- Le troubleshooting

## 🏥 Projet CHU - Big Data Healthcare Analytics

Couche **Silver** : Données transformées, nettoyées et enrichies
