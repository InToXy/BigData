# 📊 Analyse de Performance - Bronze Layer MinIO

## 🎯 Description

Ce dossier contient les graphiques d'analyse de performance pour l'accès au Data Lake MinIO (couche Bronze).
Le script `performance_minio.py` effectue une analyse complète des temps de réponse et génère 9 graphiques différents.

---

## 📈 Graphiques Générés

### 1️⃣ **Temps de réponse par dataset** (`1_temps_reponse_barres.png`)
**Type**: Diagramme en barres  
**Objectif**: Visualiser le temps de réponse de chaque dataset lors de la première lecture  
**Utilité**: Identifier rapidement les datasets les plus lents à charger

---

### 2️⃣ **Évolution temporelle - Requêtes Chaudes/Froides** (`2_evolution_temporelle_courbes.png`)
**Type**: Graphique en courbes (Line Chart)  
**Objectif**: Montrer l'évolution des temps de réponse selon le type de requête :
- 🧊 **Requêtes froides** : Première lecture (données non en cache)
- 🌡️ **Requêtes tièdes** : Deuxième lecture (cache partiel)
- 🔥 **Requêtes chaudes** : Troisième lecture (données en cache)

**Utilité**: Évaluer l'impact du cache et identifier les opportunités d'optimisation

---

### 3️⃣ **Distribution des temps de réponse** (`3_distribution_histogramme.png`)
**Type**: Histogramme  
**Objectif**: Afficher la distribution statistique des temps de réponse  
**Métriques affichées**:
- Moyenne (ligne rouge)
- Médiane (ligne verte)
- Distribution par plages de temps

**Utilité**: Comprendre la variabilité des performances et détecter les anomalies

---

### 4️⃣ **Dispersion par type de requête** (`4_dispersion_boxplot.png`)
**Type**: Boxplot (Boîte à moustaches)  
**Objectif**: Analyser la dispersion des temps de réponse pour chaque type de requête  
**Éléments visualisés**:
- Quartiles (Q1, Q2/médiane, Q3)
- Valeurs aberrantes (outliers)
- Étendue des données

**Utilité**: Identifier la stabilité des performances et les valeurs extrêmes

---

### 5️⃣ **Dispersion par dataset** (`4b_dispersion_boxplot_datasets.png`)
**Type**: Boxplot (Boîte à moustaches)  
**Objectif**: Comparer la dispersion des temps pour les 8 plus gros datasets  
**Utilité**: Évaluer la cohérence des performances par dataset sur plusieurs lectures

---

### 6️⃣ **Corrélation volume/temps** (`5_correlation_scatter.png`)
**Type**: Scatter Plot (Nuage de points) - Double vue  
**Objectif**: Analyser la corrélation entre :
- **Gauche**: Nombre de lignes → Temps de réponse
- **Droite**: Taille des données (MB) → Temps de réponse

**Code couleur**:
- Gauche: Couleur = Taille en MB
- Droite: Couleur = Nombre de lignes

**Utilité**: Identifier les goulets d'étranglement et les datasets mal optimisés

---

### 7️⃣ **Carte thermique des latences** (`6_heatmap_latence.png`)
**Type**: Heatmap (Carte thermique)  
**Objectif**: Visualiser la latence moyenne pour chaque combinaison Dataset × Type de requête  
**Code couleur**: 
- 🟨 Jaune = Rapide
- 🟧 Orange = Moyen
- 🟥 Rouge = Lent

**Utilité**: Repérer rapidement les patterns de performance et ajuster les ressources

---

### 8️⃣ **Débit par dataset** (`7_performance_debit.png`)
**Type**: Diagramme en barres  
**Objectif**: Afficher le débit de lecture en lignes/seconde pour chaque dataset  
**Utilité**: Comparer l'efficacité de lecture entre datasets

---

### 9️⃣ **Dashboard récapitulatif** (`8_dashboard_complet.png`)
**Type**: Dashboard multi-graphiques (6 panneaux)  
**Contenu**:
1. Temps de réponse par dataset (barres)
2. Distribution des temps (histogramme)
3. Dispersion par type de requête (boxplot)
4. Corrélation lignes/temps (scatter)
5. Débit de lecture (barres)
6. Statistiques globales (texte)

**Utilité**: Vue d'ensemble complète de la performance en un seul graphique

---

## 📊 Statistiques Collectées

Le script mesure les métriques suivantes :

### Métriques de base
- ⏱️ Temps de réponse (secondes)
- 📏 Nombre de lignes
- 💾 Taille des données (MB)
- 🚀 Débit (lignes/seconde)
- 📈 Débit (MB/seconde)

### Métriques statistiques
- Moyenne, médiane, min, max
- Écart-type
- Coefficient de variation
- Quartiles (Q1, Q2, Q3)

### Analyse du cache
- Temps moyen requête froide
- Temps moyen requête tiède
- Temps moyen requête chaude
- Pourcentage d'amélioration du cache

---

## 🚀 Utilisation

### Exécution du script
```bash
cd /home/alban/BigData/BigData/graphes
python3 performance_minio.py
```

### Prérequis
- Python 3.10+
- Packages : `boto3`, `pyarrow`, `pandas`, `matplotlib`, `seaborn`, `numpy`
- MinIO accessible sur `http://127.0.0.1:9000`

### Configuration
Les paramètres MinIO sont définis dans le script :
- **Endpoint** : `http://127.0.0.1:9000`
- **Access Key** : `minioadmin`
- **Secret Key** : `minioadmin123`
- **Bucket** : `bronze`

---

## 📁 Datasets Analysés

Le script teste automatiquement les datasets suivants :
1. `activites_professionnels`
2. `adherents`
3. `consultations`
4. `deces`
5. `diagnostics`
6. `patients`
7. `professionnels_sante_pg`
8. `etablissements`
9. `hospitalisations`
10. `mutuelles`

---

## 🔍 Interprétation des Résultats

### Temps de réponse optimal
- ✅ **< 0.5s** : Excellent
- ⚠️ **0.5s - 2s** : Acceptable
- ❌ **> 2s** : À optimiser

### Débit optimal
- ✅ **> 500,000 lignes/s** : Excellent
- ⚠️ **100,000 - 500,000 lignes/s** : Acceptable
- ❌ **< 100,000 lignes/s** : À optimiser

### Amélioration du cache
- ✅ **> 20%** : Cache efficace
- ⚠️ **5% - 20%** : Cache modéré
- ❌ **< 5%** : Cache peu efficace

### Coefficient de variation
- ✅ **< 50%** : Performances stables
- ⚠️ **50% - 100%** : Variabilité modérée
- ❌ **> 100%** : Performances instables

---

## 🎯 Recommandations

### Pour améliorer les performances :

1. **Optimiser les datasets lents**
   - Vérifier le format Parquet (compression, colonnes)
   - Augmenter le partitionnement

2. **Améliorer le cache**
   - Augmenter la mémoire système
   - Utiliser des requêtes récurrentes

3. **Réduire la variabilité**
   - Équilibrer la charge réseau
   - Optimiser les requêtes concurrentes

4. **Ajuster les ressources**
   - Augmenter les workers MinIO
   - Optimiser la configuration réseau

---

## 📝 Notes Techniques

### Méthodologie de test
- 3 passes de lecture par dataset
- Mesure du temps réel (wall-clock time)
- Calcul de la taille réelle en mémoire

### Limitations
- Les tests sont effectués en local (WSL)
- Le cache système peut influencer les résultats
- Les performances réseau ne sont pas simulées

---

## 📅 Date de génération
Script mis à jour : Octobre 2025  
Auteur : Script automatisé de performance MinIO
