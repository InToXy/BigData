# 📊 Index des Fichiers - Analyse de Performance MinIO

## 📂 Structure du Dossier

```
graphes/
│
├── 📜 Scripts Python
│   ├── performance_minio.py          ⭐ Script principal d'analyse
│   ├── generer_rapport.py            Générateur de rapport HTML
│   └── response_time                 [DÉPRÉCIÉ] Ancien script
│
├── 📖 Documentation
│   ├── INDEX.md                      📍 Ce fichier (index)
│   ├── GUIDE_COMPLET.md              Guide d'utilisation complet
│   └── README_GRAPHIQUES.md          Documentation des graphiques
│
├── 🌐 Rapports
│   └── rapport_performance.html      Rapport HTML interactif
│
└── 📊 Graphiques PNG (9 fichiers)
    ├── 1_temps_reponse_barres.png
    ├── 2_evolution_temporelle_courbes.png
    ├── 3_distribution_histogramme.png
    ├── 4_dispersion_boxplot.png
    ├── 4b_dispersion_boxplot_datasets.png
    ├── 5_correlation_scatter.png
    ├── 6_heatmap_latence.png
    ├── 7_performance_debit.png
    └── 8_dashboard_complet.png        ⭐ Vue d'ensemble complète
```

---

## 🚀 Quick Start (Démarrage Rapide)

### 1. Analyser les performances
```bash
cd /home/alban/BigData/BigData/graphes
python3 performance_minio.py
```
→ Génère 9 graphiques PNG en ~15-20 secondes

### 2. Créer le rapport HTML
```bash
python3 generer_rapport.py
```
→ Crée `rapport_performance.html`

### 3. Visualiser les résultats
```bash
# Sous WSL
explorer.exe rapport_performance.html
```
→ Ouvre le rapport dans votre navigateur

---

## 📋 Descriptions des Fichiers

### Scripts

| Fichier | Type | Description | Usage |
|---------|------|-------------|-------|
| `performance_minio.py` | Python | **Script principal** - Analyse complète avec 9 graphiques | `python3 performance_minio.py` |
| `generer_rapport.py` | Python | Génère rapport HTML interactif | `python3 generer_rapport.py` |
| `response_time` | Python | ⚠️ DÉPRÉCIÉ - Bug Hadoop | Ne pas utiliser |

### Documentation

| Fichier | Format | Contenu | Audience |
|---------|--------|---------|----------|
| `INDEX.md` | Markdown | 📍 Ce fichier - Vue d'ensemble | Tous |
| `GUIDE_COMPLET.md` | Markdown | Guide détaillé d'utilisation | Utilisateurs |
| `README_GRAPHIQUES.md` | Markdown | Documentation technique des graphiques | Analystes |

### Rapports

| Fichier | Format | Description |
|---------|--------|-------------|
| `rapport_performance.html` | HTML | Rapport interactif avec tous les graphiques |

### Graphiques

| Fichier | Type de graphique | Objectif | Priorité |
|---------|-------------------|----------|----------|
| `1_temps_reponse_barres.png` | Barres | Temps par dataset | ⭐⭐⭐ |
| `2_evolution_temporelle_courbes.png` | Courbes | Cache chaud/froid | ⭐⭐⭐ |
| `3_distribution_histogramme.png` | Histogramme | Distribution temps | ⭐⭐ |
| `4_dispersion_boxplot.png` | Boxplot | Dispersion par type | ⭐⭐ |
| `4b_dispersion_boxplot_datasets.png` | Boxplot | Dispersion par dataset | ⭐ |
| `5_correlation_scatter.png` | Scatter | Corrélation volume/temps | ⭐⭐⭐ |
| `6_heatmap_latence.png` | Heatmap | Carte thermique | ⭐⭐ |
| `7_performance_debit.png` | Barres | Débit lecture | ⭐⭐ |
| `8_dashboard_complet.png` | Dashboard | **Vue complète 6-en-1** | ⭐⭐⭐⭐ |

**Légende priorité** :
- ⭐⭐⭐⭐ = Essentiel pour présentation
- ⭐⭐⭐ = Très important
- ⭐⭐ = Important
- ⭐ = Complémentaire

---

## 📊 Types de Graphiques Fournis

Conforme aux exigences du cahier des charges :

✅ **1. Graphique en courbes (Line Chart)**
   → `2_evolution_temporelle_courbes.png`
   
✅ **2. Diagramme en barres / Histogramme**
   → `1_temps_reponse_barres.png` + `3_distribution_histogramme.png`
   
✅ **3. Boxplot (boîte à moustaches)**
   → `4_dispersion_boxplot.png` + `4b_dispersion_boxplot_datasets.png`
   
✅ **4. Scatter Plot (nuage de points)**
   → `5_correlation_scatter.png`
   
✅ **5. Heatmap (carte thermique)**
   → `6_heatmap_latence.png`

---

## 🎯 Scénarios d'Utilisation

### Pour une présentation rapide
1. Ouvrir `rapport_performance.html` dans un navigateur
2. Montrer le dashboard `8_dashboard_complet.png`

### Pour une analyse détaillée
1. Lire `GUIDE_COMPLET.md`
2. Examiner chaque graphique individuellement
3. Consulter `README_GRAPHIQUES.md` pour interprétation

### Pour un rapport Word/PowerPoint
1. Exécuter `performance_minio.py`
2. Insérer les PNG depuis le dossier `graphes/`
3. Recommandés : `8_dashboard_complet.png`, `2_evolution_temporelle_courbes.png`, `5_correlation_scatter.png`

---

## 📏 Statistiques Actuelles

**Dernière exécution** :
- 📁 Datasets : 10
- 📏 Total lignes : 5,151,487
- 💾 Taille totale : 2.09 GB
- ⚡ Débit moyen : 911,632 lignes/s
- ⏱️ Temps moyen : 0.59s

---

## 🔗 Liens Rapides

- [Guide Complet](./GUIDE_COMPLET.md) - Tout ce qu'il faut savoir
- [Documentation Graphiques](./README_GRAPHIQUES.md) - Détails techniques
- [Rapport HTML](./rapport_performance.html) - Visualisation interactive

---

**Créé le** : Octobre 2025  
**Projet** : CHU Big Data Healthcare Analytics  
**Composant** : Analyse de Performance Data Lake MinIO
