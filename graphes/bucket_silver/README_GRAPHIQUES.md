# 📊 Documentation Technique des Graphiques - Silver Layer

## Vue d'ensemble

Cette documentation fournit les spécifications techniques détaillées de chaque graphique généré par `performance_minio.py` pour la couche **Silver**.

---

## 1️⃣ Temps de Réponse par Dataset

### Spécifications techniques

```python
Type: matplotlib.pyplot.barh (Bar Chart Horizontal)
Dimensions: 14" × 8" (1400 × 800 pixels à 100 DPI)
Résolution: 150 DPI
Format: PNG avec compression
Palette: viridis (gradient de couleurs)
```

### Données utilisées

- **Axe X** : `df_perf['time']` (temps en secondes, float)
- **Axe Y** : `df_perf['dataset']` (nom du dataset, string)
- **Annotations** : Valeurs de temps sur chaque barre

### Code source

```python
fig, ax = plt.subplots(figsize=(14, 8))
colors = plt.cm.viridis(np.linspace(0, 1, len(df_perf)))
bars = ax.barh(df_perf['dataset'], df_perf['time'], color=colors)

for i, (bar, time_val) in enumerate(zip(bars, df_perf['time'])):
    ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
            f'{time_val:.3f}s', va='center', fontsize=9)
```

### Interprétation

- Permet de comparer visuellement les temps de réponse
- Les datasets sont triés par défaut (ordre d'insertion)
- Les couleurs aident à distinguer les datasets

---

## 2️⃣ Évolution Temporelle (Cache)

### Spécifications techniques

```python
Type: matplotlib.pyplot.plot (Line Chart)
Dimensions: 14" × 8"
Résolution: 150 DPI
Nombre de courbes: 3 (Froide, Tiède, Chaude)
Markers: cercles (o), taille 6
Line width: 2 pixels
Alpha: 0.8 (80% opacité)
```

### Données utilisées

```python
for query_type in ['Froide', 'Tiède', 'Chaude']:
    subset = detailed_df[detailed_df['query_type'] == query_type]
    ax.plot(range(len(subset)), subset['time'], ...)
```

### Métriques clés

- **Requête Froide** : Première lecture (cache froid)
- **Requête Tiède** : Deuxième lecture (cache partiel)
- **Requête Chaude** : Troisième lecture (cache chaud)

### Formule d'amélioration du cache

```python
cache_improvement = ((cold_times.mean() - hot_times.mean()) / cold_times.mean()) * 100
```

**Valeurs attendues** :
- Positif : Le cache améliore les performances ✅
- Négatif : Problème de cache ou surcharge ❌

---

## 3️⃣ Distribution des Temps

### Spécifications techniques

```python
Type: matplotlib.pyplot.hist (Histogram)
Dimensions: 12" × 7"
Bins: 20 intervalles automatiques
Couleur: skyblue (#87CEEB)
Edgecolor: black
Alpha: 0.7 (70% opacité)
```

### Statistiques affichées

```python
Moyenne (ligne rouge pointillée):
mean = df_perf['time'].mean()

Médiane (ligne verte pointillée):
median = df_perf['time'].median()
```

### Métriques de dispersion

```python
# Écart-type
std = df_perf['time'].std()

# Coefficient de variation
cv = (std / mean) * 100

# Plage interquartile
q1 = df_perf['time'].quantile(0.25)
q3 = df_perf['time'].quantile(0.75)
iqr = q3 - q1
```

### Interprétation

- **Distribution normale** : Performances prévisibles
- **Queue à droite** : Présence d'outliers lents
- **Bimodale** : Deux groupes de datasets distincts

---

## 4️⃣ Dispersion par Type de Requête

### Spécifications techniques

```python
Type: seaborn.boxplot
Dimensions: 12" × 7"
Palette: Set2 (couleurs pastel)
Orientation: Verticale
```

### Composants du boxplot

```
        Maximum ─────────┬─────────
                         │
                      ┌──┴──┐
            Q3 (75%) ─┤     │
                      │  M  │  ← Médiane
            Q1 (25%) ─┤     │
                      └──┬──┘
                         │
        Minimum ─────────┴─────────

Points = Outliers (valeurs > Q3 + 1.5×IQR ou < Q1 - 1.5×IQR)
```

### Données

```python
X-axis: detailed_df['query_type']  # Froide, Tiède, Chaude
Y-axis: detailed_df['time']        # Temps en secondes
```

### Détection des outliers

```python
# Méthode IQR (Interquartile Range)
q1 = df['time'].quantile(0.25)
q3 = df['time'].quantile(0.75)
iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

outliers = df[(df['time'] < lower_bound) | (df['time'] > upper_bound)]
```

---

## 5️⃣ Dispersion par Dataset (Top 15)

### Spécifications techniques

```python
Type: seaborn.boxplot
Dimensions: 14" × 8"
Palette: Set3
Sélection: 15 datasets les plus lents
Rotation labels: 45°, horizontal alignment: right
```

### Sélection des données

```python
top_datasets = df_perf.nlargest(15, 'time')['dataset'].tolist()
subset_data = detailed_df[detailed_df['dataset'].isin(top_datasets)]
```

### Usage

- Comparer la stabilité des datasets problématiques
- Identifier les datasets avec forte variabilité
- Prioriser les optimisations

---

## 6️⃣ Corrélation Volume/Temps

### Spécifications techniques

```python
Type: matplotlib.pyplot.scatter
Dimensions: 12" × 7"
Palette: plasma (gradient violet-jaune)
Taille des bulles: Proportionnelle au nombre de lignes
Ligne de tendance: Régression linéaire (degré 1)
```

### Calcul de la taille des bulles

```python
sizes = (df_perf['rows'] / df_perf['rows'].max()) * 500 + 50
# Taille minimale: 50
# Taille maximale: 550
```

### Régression linéaire

```python
# Calculer les coefficients
z = np.polyfit(df_perf['size_mb'], df_perf['time'], 1)
# z[0] = pente
# z[1] = ordonnée à l'origine

# Créer la fonction polynomiale
p = np.poly1d(z)

# Tracer la ligne
x_trend = np.linspace(df_perf['size_mb'].min(), df_perf['size_mb'].max(), 100)
ax.plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2)
```

### Coefficient de corrélation

```python
import scipy.stats as stats
correlation, p_value = stats.pearsonr(df_perf['size_mb'], df_perf['time'])

# Interprétation:
# |r| > 0.7 : Forte corrélation
# |r| 0.3-0.7 : Corrélation modérée
# |r| < 0.3 : Faible corrélation
```

### Annotations

Les datasets outliers sont annotés automatiquement :
```python
if row['time'] > df_perf['time'].quantile(0.75) or 
   row['size_mb'] > df_perf['size_mb'].quantile(0.75):
    ax.annotate(row['dataset'], (row['size_mb'], row['time']), ...)
```

---

## 7️⃣ Heatmap des Latences

### Spécifications techniques

```python
Type: seaborn.heatmap
Dimensions: 10" × 12"
Colormap: YlOrRd (Yellow-Orange-Red)
Annotations: Valeurs numériques (format .3f)
Linewidths: 0.5 (séparation des cellules)
```

### Structure des données

```python
# Pivot table
heatmap_data = detailed_df.pivot_table(
    values='time',           # Valeurs affichées
    index='dataset',         # Lignes
    columns='query_type',    # Colonnes
    aggfunc='mean'           # Agrégation
)

# Résultat:
#                        Froide  Tiède  Chaude
# activites_profess...   1.630   1.487  1.250
# consultations          0.817   0.750  0.680
# ...
```

### Échelle de couleurs

- **Jaune clair** : Temps court (< 0.5s)
- **Orange** : Temps moyen (0.5-1.5s)
- **Rouge** : Temps long (> 1.5s)

### Patterns à détecter

- **Ligne uniformément rouge** : Dataset toujours lent
- **Colonne rouge** : Problème de type de cache
- **Dégradé horizontal** : Amélioration du cache normale

---

## 8️⃣ Débit par Dataset

### Spécifications techniques

```python
Type: matplotlib.pyplot.barh
Dimensions: 14" × 8"
Colormap: RdYlGn (Red-Yellow-Green)
Tri: Par débit croissant (du plus lent au plus rapide)
```

### Calcul du débit

```python
throughput = rows / time  # Lignes par seconde

# Exemples de valeurs:
# 2,883,867 rows/s = Excellent (vert foncé)
# 500,000 rows/s = Bon (jaune-vert)
# 50,000 rows/s = Faible (rouge)
```

### Échelle de couleurs

```python
colors = plt.cm.RdYlGn(np.linspace(0.2, 0.9, len(df_sorted)))
# 0.2 = Rouge (débit faible)
# 0.5 = Jaune (débit moyen)
# 0.9 = Vert (débit élevé)
```

### Annotations

```python
for bar, throughput in zip(bars, df_sorted['throughput']):
    ax.text(bar.get_width() + offset, bar.get_y() + bar.get_height()/2,
            f'{throughput:,.0f}', va='center', fontsize=9)
# Format avec séparateur de milliers
```

---

## 9️⃣ Dashboard Récapitulatif

### Spécifications techniques

```python
Type: matplotlib Figure avec GridSpec
Dimensions: 18" × 12"
Layout: 3 lignes × 2 colonnes
Nombre de panneaux: 5
- Panel 1: (0,0) - Bar chart temps
- Panel 2: (0,1) - Pie chart tailles
- Panel 3: (1,:) - Boxplot (span 2 colonnes)
- Panel 4: (2,0) - Scatter
- Panel 5: (2,1) - Bar chart débit
```

### GridSpec configuration

```python
gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
# hspace = espacement vertical (30%)
# wspace = espacement horizontal (30%)
```

### Panel 1: Top 10 Temps

```python
ax1 = fig.add_subplot(gs[0, 0])
top10 = df_perf.nlargest(10, 'time')
ax1.barh(top10['dataset'], top10['time'], color='coral')
```

### Panel 2: Répartition Tailles

```python
ax2 = fig.add_subplot(gs[0, 1])
top5_size = df_perf.nlargest(5, 'size_mb')
other_size = df_perf['size_mb'].sum() - top5_size['size_mb'].sum()

# Pie chart avec top 5 + "Autres"
sizes_pie = list(top5_size['size_mb']) + [other_size]
labels_pie = list(top5_size['dataset']) + ['Autres']
```

### Panel 3: Boxplot Cache

```python
ax3 = fig.add_subplot(gs[1, :])  # Span 2 colonnes
sns.boxplot(data=detailed_df, x='query_type', y='time', ax=ax3)
```

### Panel 4: Scatter Volume vs Temps

```python
ax4 = fig.add_subplot(gs[2, 0])
ax4.scatter(df_perf['rows'], df_perf['time'], s=100, alpha=0.6, c='purple')
```

### Panel 5: Top 10 Débit

```python
ax5 = fig.add_subplot(gs[2, 1])
top10_throughput = df_perf.nlargest(10, 'throughput')
ax5.barh(top10_throughput['dataset'], top10_throughput['throughput'], color='lightgreen')
```

---

## 📐 Standards et Conventions

### Résolution et taille

```python
# Tous les graphiques
dpi = 150  # Points par pouce
bbox_inches = 'tight'  # Enlever les espaces blancs

# Dimensions recommandées:
# - Graphiques individuels: 12-14" de largeur
# - Dashboard: 18" de largeur
# - Hauteur: 7-8" (simple), 12" (dashboard)
```

### Palettes de couleurs

```python
# Par type de graphique:
viridis   → Barres (gradient bleu-jaune)
Set2      → Boxplots (couleurs pastel)
Set3      → Comparaisons multiples
plasma    → Scatter (gradient violet-jaune)
YlOrRd    → Heatmaps (jaune-orange-rouge)
RdYlGn    → Performance (rouge-jaune-vert)
husl      → Palette générale
```

### Polices et tailles

```python
# Titres principaux
fontsize = 14
fontweight = 'bold'

# Titres d'axes
fontsize = 12
fontweight = 'bold'

# Labels et annotations
fontsize = 9

# Légendes
fontsize = 11
```

### Grilles et transparence

```python
# Grille
ax.grid(True, alpha=0.3)  # 30% d'opacité

# Transparence générale
alpha = 0.6-0.8  # Pour scatter plots et overlays
```

---

## 🔧 Personnalisation

### Modifier les couleurs

```python
# Dans performance_minio.py, ligne ~22
sns.set_palette("viridis")  # Changer ici
# Autres options: "mako", "rocket", "crest", "flare"
```

### Changer la résolution

```python
# Dans chaque plt.savefig()
plt.savefig("fichier.png", dpi=300)  # Haute résolution
# dpi=72  : Écran
# dpi=150 : Standard
# dpi=300 : Impression
```

### Ajouter un watermark

```python
# Avant plt.savefig()
fig.text(0.99, 0.01, 'CHU Big Data', 
         ha='right', va='bottom', 
         fontsize=8, color='gray', alpha=0.5)
```

---

## 📊 Exports et Formats

### Formats supportés

```python
plt.savefig("graph.png")   # PNG (recommandé)
plt.savefig("graph.pdf")   # PDF (vectoriel)
plt.savefig("graph.svg")   # SVG (vectoriel)
plt.savefig("graph.jpg")   # JPEG (avec perte)
```

### Paramètres d'export

```python
plt.savefig(
    "fichier.png",
    dpi=150,              # Résolution
    bbox_inches='tight',  # Enlever marges
    transparent=False,    # Fond transparent
    facecolor='white',    # Couleur de fond
    edgecolor='none'      # Pas de bordure
)
```

---

## 🏥 Projet

**CHU - Big Data Healthcare Analytics**  
**Layer** : Silver (Données transformées)  
**Version** : 1.0  
**Technologies** : Python, Matplotlib, Seaborn, PyArrow
