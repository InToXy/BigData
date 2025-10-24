# 📊 GRAPHIQUES DE PERFORMANCE - ZONE GOLD

**Date de génération:** 2025-10-24 11:36:59  
**Nombre de graphiques:** 8  
**Données source:** 17 requêtes de test

---

## 📈 LISTE DES GRAPHIQUES

### 1. Line Chart - Évolution Temps de Réponse
**Fichier:** `1_line_chart_temps_reponse.png`

Montre l'évolution des temps de réponse pour les 17 requêtes testées, regroupées par catégorie (KPI Analytiques, Temporel, Technique, Data Science).

**Utilisation:** Identifier les tendances et pics de latence par type de requête.

---

### 2. Bar Chart - Distribution par Catégorie
**Fichier:** `2_bar_chart_distribution.png`

Diagramme en barres des temps moyens par catégorie avec barres d'erreur (écart-type).

**Utilisation:** Comparer les performances moyennes entre catégories de requêtes.

---

### 3. Boxplot - Dispersion des Performances
**Fichier:** `3_boxplot_dispersion.png`

Boîte à moustaches montrant la distribution, médiane, moyenne et valeurs aberrantes par catégorie.

**Utilisation:** Analyser la variabilité des performances et identifier les outliers.

---

### 4. Scatter Plot - Corrélation Volume/Temps
**Fichier:** `4_scatter_plot_correlation.png`

Nuage de points montrant la corrélation entre le volume de données retournées et le temps de réponse.

**Utilisation:** Identifier les goulets d'étranglement liés au volume de données.

---

### 5. Heatmap - Latence par Heure
**Fichier:** `5_heatmap_latence.png`

Carte thermique des temps de réponse par type de requête et heure de la journée.

**Utilisation:** Repérer les schémas récurrents et heures de pointe.

---

### 6. Comparaison Zones Bronze/Silver/Gold
**Fichier:** `6_comparaison_zones.png`

Comparaison des performances (temps de lecture, compression) entre les 3 zones du Data Lake.

**Utilisation:** Justifier l'architecture en 3 zones et montrer les gains de performance.

---

### 7. Pie Chart - Répartition du Temps
**Fichier:** `7_pie_chart_repartition.png`

Diagramme circulaire de la répartition du temps total par catégorie de requête.

**Utilisation:** Visualiser les catégories les plus coûteuses en temps.

---

### 8. Performance Cumulative
**Fichier:** `8_cumulative_performance.png`

Courbe de performance cumulative montrant le temps total d'exécution des 17 requêtes.

**Utilisation:** Comparer les performances réelles vs objectifs.

---

## 📊 DONNÉES SOURCES

- **Nombre de requêtes testées:** 17
- **Catégories:** 4 (KPI Analytiques, Temporel, Technique, Data Science)
- **Temps total d'exécution:** 3.26s
- **Temps moyen par requête:** 0.192s
- **Objectif:** < 0.5s par requête

---

## 🎯 MÉTRIQUES CLÉS

| Catégorie | Requêtes | Temps Moyen | Temps Total |
|-----------|----------|-------------|-------------|
| KPI Analytiques | 5 | 0.116s | 0.58s |
| Temporel | 3 | 0.127s | 0.38s |
| Technique | 5 | 0.220s | 1.10s |
| Data Science | 4 | 0.300s | 1.20s |

---

## 💡 INSIGHTS

### ✅ Points Forts
- **Toutes les requêtes** respectent l'objectif de < 0.5s
- **KPI Analytiques** sont les plus rapides (moyenne: 0.116s)
- **Compression exceptionnelle:** 99.996% (Bronze → Gold)
- **Performance stable:** Faible écart-type dans chaque catégorie

### ⚠️ Points d'Attention
- **Requêtes Data Science** légèrement plus lentes (moyenne: 0.300s)
- **Pics de latence** aux heures de pointe (8h-10h, 14h-16h)
- **Corrélation volume/temps:** Augmentation linéaire au-delà de 500 lignes

---

## 🔧 UTILISATION DANS VOTRE RAPPORT

### Pour un rapport managérial:
- Graphiques 1, 2, 6, 7 (vue d'ensemble)

### Pour un rapport technique:
- Graphiques 3, 4, 5, 8 (analyse détaillée)

### Pour une présentation:
- Graphiques 1, 6, 7 (impact visuel)

---

## 📝 GÉNÉRATION

Pour régénérer les graphiques:

```bash
cd /home/alban/BigData/BigData/tests_gold
python3 generate_performance_charts.py
```

**Prérequis:** matplotlib, seaborn, pandas, numpy

---

**Dernière mise à jour:** 2025-10-24 11:36:59  
**Auteur:** Équipe Data Engineering CHU
