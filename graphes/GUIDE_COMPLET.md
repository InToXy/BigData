# 🎯 Guide Complet - Analyse de Performance MinIO

## 📋 Table des Matières
1. [Vue d'ensemble](#vue-densemble)
2. [Scripts disponibles](#scripts-disponibles)
3. [Graphiques générés](#graphiques-générés)
4. [Comment utiliser](#comment-utiliser)
5. [Interprétation des résultats](#interprétation-des-résultats)

---

## 📊 Vue d'ensemble

Ce dossier contient un système complet d'analyse de performance pour le Data Lake MinIO (couche Bronze).

### Objectifs
- ✅ Mesurer les temps de réponse des requêtes
- ✅ Analyser l'impact du cache (requêtes chaudes/froides)
- ✅ Identifier les goulets d'étranglement
- ✅ Optimiser les performances du Data Lake

### Technologies utilisées
- **Python 3.10+**
- **boto3** : Accès S3/MinIO
- **pyarrow** : Lecture Parquet
- **pandas** : Analyse de données
- **matplotlib + seaborn** : Visualisations

---

## 🔧 Scripts disponibles

### 1. `performance_minio.py` ⭐ (Script principal)
**Fonction** : Analyse complète de performance avec génération de 9 graphiques

**Ce qu'il fait** :
- Lit tous les datasets du bucket `bronze`
- Effectue 3 passes de lecture (froide, tiède, chaude)
- Mesure temps, débit, taille des données
- Génère 9 graphiques d'analyse
- Affiche des statistiques détaillées

**Exécution** :
```bash
cd /home/alban/BigData/BigData/graphes
python3 performance_minio.py
```

**Durée** : ~15-20 secondes (3 passes × 10 datasets)

---

### 2. `generer_rapport.py`
**Fonction** : Génère un rapport HTML interactif

**Ce qu'il fait** :
- Crée une page HTML élégante
- Intègre tous les graphiques
- Affiche les statistiques clés
- Fournit des recommandations

**Exécution** :
```bash
python3 generer_rapport.py
```

**Résultat** : `rapport_performance.html`

---

### 3. `response_time` (Ancien - Déprécié)
⚠️ **Ne pas utiliser** - Remplacé par `performance_minio.py`

Problème : Bug Hadoop avec timeout "60s"

---

## 📈 Graphiques générés

### Typologie selon votre cahier des charges

| # | Graphique | Type | Objectif | Fichier |
|---|-----------|------|----------|---------|
| 1 | Temps de réponse | **Barres** | Comparer les temps par dataset | `1_temps_reponse_barres.png` |
| 2 | Évolution temporelle | **Courbes** | Analyser cache chaud/froid | `2_evolution_temporelle_courbes.png` |
| 3 | Distribution | **Histogramme** | Variabilité des temps | `3_distribution_histogramme.png` |
| 4 | Dispersion requêtes | **Boxplot** | Outliers par type | `4_dispersion_boxplot.png` |
| 5 | Dispersion datasets | **Boxplot** | Outliers par dataset | `4b_dispersion_boxplot_datasets.png` |
| 6 | Corrélation | **Scatter Plot** | Volume vs Temps | `5_correlation_scatter.png` |
| 7 | Latence | **Heatmap** | Patterns temporels | `6_heatmap_latence.png` |
| 8 | Débit | **Barres** | Performance lecture | `7_performance_debit.png` |
| 9 | Dashboard | **Multi-panneaux** | Vue d'ensemble | `8_dashboard_complet.png` |

### Correspondance avec les types demandés ✅

✅ **Graphique en courbes (Line Chart)** → `2_evolution_temporelle_courbes.png`
- Évolution des temps selon le type de requête (chaude/froide)

✅ **Diagramme en barres / Histogramme** → `1_temps_reponse_barres.png` + `3_distribution_histogramme.png`
- Temps par dataset + Distribution statistique

✅ **Boxplot (boîte à moustaches)** → `4_dispersion_boxplot.png` + `4b_dispersion_boxplot_datasets.png`
- Dispersion et outliers

✅ **Scatter Plot (nuage de points)** → `5_correlation_scatter.png`
- Corrélation volume/temps

✅ **Heatmap (carte thermique)** → `6_heatmap_latence.png`
- Latence par dataset × type de requête

---

## 🚀 Comment utiliser

### Installation des dépendances

```bash
# Si ce n'est pas déjà fait
pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy
```

### Exécution complète (recommandé)

```bash
cd /home/alban/BigData/BigData/graphes

# 1. Analyser les performances (génère les graphiques)
python3 performance_minio.py

# 2. Générer le rapport HTML
python3 generer_rapport.py

# 3. Ouvrir le rapport dans le navigateur
# Sous WSL :
explorer.exe rapport_performance.html
# Ou copier le chemin et ouvrir dans un navigateur
```

### Vérifier les graphiques

```bash
# Lister tous les graphiques
ls -lh *.png

# Ouvrir un graphique spécifique
# Sous WSL :
explorer.exe 8_dashboard_complet.png
```

---

## 📊 Interprétation des résultats

### Métriques clés

#### 1. Temps de réponse
- ✅ **< 0.5s** : Excellent
- ⚠️ **0.5s - 2s** : Acceptable
- ❌ **> 2s** : Nécessite optimisation

#### 2. Débit (lignes/seconde)
- ✅ **> 500K** : Excellent
- ⚠️ **100K - 500K** : Acceptable
- ❌ **< 100K** : Lent

#### 3. Débit (MB/seconde)
- ✅ **> 300 MB/s** : Excellent
- ⚠️ **100-300 MB/s** : Acceptable
- ❌ **< 100 MB/s** : Lent

#### 4. Amélioration du cache
- ✅ **> 20%** : Cache très efficace
- ⚠️ **5-20%** : Cache modéré
- ❌ **< 5%** : Cache peu efficace
- 🔴 **Négatif** : Problème (surcharge)

#### 5. Coefficient de variation (CV)
- ✅ **< 50%** : Performances stables
- ⚠️ **50-100%** : Variabilité modérée
- ❌ **> 100%** : Performances instables

### Résultats actuels (Dernière exécution)

```
📊 STATISTIQUES GLOBALES:
   • Datasets analysés: 10
   • Total de lignes: 5,151,487
   • Taille totale: 2090.16 MB
   • Temps total: 5.65s
   • Débit moyen: 911,632 lignes/seconde ✅
   • Débit en MB/s: 369.88 MB/s ✅

🔥 ANALYSE CACHE:
   • Amélioration du cache: -15.5% ❌
   
📈 MÉTRIQUES:
   • Coefficient de variation: 147.1% ❌
```

**Interprétation** :
- ✅ **Points positifs** :
  - Débit excellent (> 900K lignes/s)
  - Transfert rapide (370 MB/s)
  - Temps moyens acceptables (0.59s)

- ❌ **Points à améliorer** :
  - Cache négatif (-15.5%) → Conflit de ressources
  - Forte variabilité (CV=147%) → Performances instables
  - 3 datasets lents à optimiser

### Top datasets problématiques

```
⚠️ TOP 3 DATASETS LES PLUS LENTS:
1. activites_professionnels: 1.81s (1.8M lignes)
2. consultations: 0.98s (1M lignes)
3. etablissements: 0.82s (417K lignes)
```

**Actions recommandées** :
1. Vérifier le partitionnement Parquet
2. Optimiser la compression (actuellement Snappy)
3. Analyser la structure des colonnes

---

## 🔍 Analyse détaillée par graphique

### 1️⃣ Temps de réponse (Barres)
**Comment lire** :
- Plus la barre est haute, plus le dataset est lent
- Comparer visuellement tous les datasets

**Action** :
- Identifier les 3 datasets les plus hauts
- Les prioriser pour l'optimisation

### 2️⃣ Évolution temporelle (Courbes)
**Comment lire** :
- Ligne bleue = Requête froide (1ère lecture)
- Ligne orange = Requête tiède (2ème lecture)
- Ligne verte = Requête chaude (3ème lecture)

**Attendu** :
- Ligne verte devrait être plus basse (cache efficace)
- Si lignes plates = pas de cache

**Action** :
- Si pas d'amélioration : augmenter mémoire système

### 3️⃣ Distribution (Histogramme)
**Comment lire** :
- Pic à gauche = Beaucoup de requêtes rapides ✅
- Pic à droite = Beaucoup de requêtes lentes ❌
- Ligne rouge = Moyenne
- Ligne verte = Médiane

**Attendu** :
- Distribution concentrée à gauche
- Peu de valeurs à droite

### 4️⃣ Dispersion (Boxplot)
**Comment lire** :
- Boîte = 50% des valeurs (Q1-Q3)
- Ligne au milieu = Médiane
- Points isolés = Outliers (valeurs anormales)

**Action** :
- Si beaucoup d'outliers : investiguer causes

### 6️⃣ Corrélation (Scatter)
**Comment lire** :
- Gauche : Plus de lignes → Plus de temps ?
- Droite : Plus de MB → Plus de temps ?

**Attendu** :
- Corrélation linéaire modérée
- Points groupés = cohérence

**Action** :
- Points éloignés = datasets à optimiser

### 7️⃣ Heatmap
**Comment lire** :
- Jaune = Rapide ✅
- Rouge = Lent ❌

**Action** :
- Cases rouges = combinaisons à optimiser

---

## 🎯 Recommandations d'optimisation

### Court terme (Rapide)
1. **Augmenter cache système**
   ```bash
   # Vérifier cache actuel
   free -h
   ```

2. **Optimiser datasets lents**
   - Recompresser avec ZSTD au lieu de Snappy
   - Vérifier partitionnement

3. **Réduire concurrence**
   - Limiter requêtes simultanées à MinIO

### Moyen terme
1. **Ajuster MinIO**
   - Augmenter workers
   - Optimiser configuration réseau

2. **Optimiser Parquet**
   - Row groups plus petits
   - Meilleure compression

3. **Monitoring continu**
   - Exécuter script quotidiennement
   - Suivre tendances

### Long terme
1. **Architecture**
   - Considérer CDN/cache
   - Optimiser réseau WSL

2. **Indexation**
   - Ajouter index Z-order
   - Optimiser colonnes fréquentes

---

## 📁 Structure des fichiers

```
graphes/
├── performance_minio.py          ⭐ Script principal
├── generer_rapport.py            📄 Générateur HTML
├── README_GRAPHIQUES.md          📖 Documentation détaillée
├── GUIDE_COMPLET.md              📘 Ce fichier
├── rapport_performance.html      🌐 Rapport interactif
│
├── 1_temps_reponse_barres.png
├── 2_evolution_temporelle_courbes.png
├── 3_distribution_histogramme.png
├── 4_dispersion_boxplot.png
├── 4b_dispersion_boxplot_datasets.png
├── 5_correlation_scatter.png
├── 6_heatmap_latence.png
├── 7_performance_debit.png
└── 8_dashboard_complet.png       ⭐ Vue complète
```

---

## ❓ FAQ

### Q: Pourquoi le cache est négatif (-15.5%) ?
**R**: Probablement une surcharge système lors des passes 2-3. Recommandations :
- Fermer applications lourdes
- Augmenter mémoire allouée à WSL
- Exécuter à un moment moins chargé

### Q: Comment améliorer les performances ?
**R**: Ordre de priorité :
1. Optimiser les 3 datasets les plus lents
2. Améliorer le cache système
3. Ajuster la configuration MinIO

### Q: À quelle fréquence exécuter l'analyse ?
**R**: 
- **Quotidien** : Si en production
- **Hebdomadaire** : En développement
- **Après chaque optimisation** : Pour valider

### Q: Les graphiques sont-ils exportables ?
**R**: Oui ! Format PNG haute résolution (150 DPI).
Idéal pour rapports Word/PowerPoint.

---

## 🆘 Troubleshooting

### Erreur : "ModuleNotFoundError: No module named 'boto3'"
```bash
pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy
```

### Erreur : "Connection refused to MinIO"
```bash
# Vérifier que MinIO est démarré
docker ps | grep minio

# Redémarrer si nécessaire
docker restart chu_minio
```

### Les graphiques ne s'affichent pas dans le rapport HTML
- Vérifier que tous les PNG sont dans le même dossier que le HTML
- Ouvrir le HTML depuis le dossier `graphes/`

---

## 📞 Support

Pour toute question :
1. Consulter `README_GRAPHIQUES.md`
2. Vérifier les logs du script
3. Analyser le rapport HTML

---

**Dernière mise à jour** : Octobre 2025
**Auteur** : Système d'analyse automatisé MinIO
**Version** : 2.0 (Analyse avancée avec 9 graphiques)
