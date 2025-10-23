# 📊 Analyse de Performance - Data Lake MinIO

## 🎯 Vue d'ensemble

Système complet d'analyse de performance pour le Data Lake MinIO (couche Bronze).
Génère automatiquement **9 graphiques** et un **rapport HTML interactif**.

## ⚡ Quick Start

```bash
# Générer tous les graphiques et le rapport
./generer_tout.sh

# Ou manuellement :
python3 performance_minio.py    # Génère 9 graphiques
python3 generer_rapport.py      # Crée le rapport HTML
```

## 📊 Graphiques Fournis

| # | Type | Fichier | Description |
|---|------|---------|-------------|
| 1 | **Barres** | `1_temps_reponse_barres.png` | Temps de réponse par dataset |
| 2 | **Courbes** ⭐ | `2_evolution_temporelle_courbes.png` | Cache chaud/froid |
| 3 | **Histogramme** ⭐ | `3_distribution_histogramme.png` | Distribution des temps |
| 4 | **Boxplot** ⭐ | `4_dispersion_boxplot.png` | Dispersion par type |
| 5 | **Boxplot** | `4b_dispersion_boxplot_datasets.png` | Dispersion par dataset |
| 6 | **Scatter** ⭐ | `5_correlation_scatter.png` | Corrélation volume/temps |
| 7 | **Heatmap** ⭐ | `6_heatmap_latence.png` | Carte thermique |
| 8 | **Barres** | `7_performance_debit.png` | Débit de lecture |
| 9 | **Dashboard** ⭐⭐⭐ | `8_dashboard_complet.png` | Vue d'ensemble 6-en-1 |

✅ **Tous les types demandés sont fournis** : Courbes, Histogramme, Boxplot, Scatter, Heatmap

## 📈 Résultats (Dernière Exécution)

```
Datasets analysés : 10
Total de lignes   : 5.15M
Taille totale     : 2.09 GB
Débit moyen       : 911,632 lignes/s ⚡
Temps moyen       : 0.59s par requête
```

## 📚 Documentation

- **[INDEX.md](./INDEX.md)** - Vue d'ensemble et index des fichiers
- **[GUIDE_COMPLET.md](./GUIDE_COMPLET.md)** - Guide utilisateur complet
- **[README_GRAPHIQUES.md](./README_GRAPHIQUES.md)** - Documentation technique

## 🔧 Prérequis

```bash
# Installer les dépendances
pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy

# Vérifier que MinIO est démarré
docker ps | grep minio
```

## 🌐 Rapport HTML

Un rapport HTML interactif est généré automatiquement :

```bash
# Générer le rapport
python3 generer_rapport.py

# Ouvrir dans le navigateur (WSL)
explorer.exe rapport_performance.html
```

## 📁 Structure

```
graphes/
├── generer_tout.sh              🚀 Script de génération complète
├── performance_minio.py         ⭐ Script principal
├── generer_rapport.py           📄 Générateur HTML
├── rapport_performance.html     🌐 Rapport interactif
├── INDEX.md                     📍 Index
├── GUIDE_COMPLET.md             📖 Guide complet
├── README.md                    📘 Ce fichier
└── [9 fichiers PNG]             📊 Graphiques
```

## 🎯 Cas d'Usage

### Pour une présentation
→ Utiliser `8_dashboard_complet.png` ou ouvrir `rapport_performance.html`

### Pour un rapport écrit
→ Insérer les PNG individuels dans Word/PowerPoint

### Pour une analyse détaillée
→ Lire `GUIDE_COMPLET.md` et examiner chaque graphique

## 💡 Conseils

- **Exécuter régulièrement** : Quotidien en prod, hebdomadaire en dev
- **Comparer les résultats** : Suivre l'évolution dans le temps
- **Optimiser les lents** : Focus sur top 3 datasets les plus lents

## ⚠️ Troubleshooting

**MinIO non accessible** :
```bash
docker restart chu_minio
```

**Dépendances manquantes** :
```bash
pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy
```

**Cache négatif** :
- Fermer applications lourdes
- Exécuter à un moment moins chargé
- Augmenter mémoire WSL

## 📅 Informations

- **Projet** : CHU Big Data Healthcare Analytics
- **Composant** : Analyse Performance Data Lake
- **Version** : 2.0 (Octobre 2025)
- **Auteur** : Système d'analyse automatisé

---

🚀 **Pour commencer** : `./generer_tout.sh`
