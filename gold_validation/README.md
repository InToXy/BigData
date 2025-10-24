# 🔍 VALIDATION DES TABLES GOLD

**Dossier:** `gold_validation/`  
**Objectif:** Vérifier le peuplement et la qualité des données de la zone Gold

---

## 📋 CONTENU DU DOSSIER

```
gold_validation/
├── README.md                      # Ce fichier
├── validate_gold_tables.py        # Script Python complet (via Spark)
├── validate_gold.sh               # Wrapper bash pour le script Python
├── quick_check_trino.sh           # Vérification rapide via Trino
└── reports/                       # Rapports générés (créé automatiquement)
```

---

## 🚀 UTILISATION

### Méthode 1: Vérification Rapide (Trino) - RECOMMANDÉ

**Avantages:** Ultra-rapide, léger, pas besoin de Spark

```bash
cd /home/alban/BigData/BigData/gold_validation

# Vérification basique
./quick_check_trino.sh

# Avec échantillon de données
./quick_check_trino.sh --sample
```

**Prérequis:** Container Trino démarré (`docker-compose up -d trino`)

**Sortie:**
```
TABLE                                         LIGNES    COLONNES  STATUS
─────────────────────────────────────────── ──────────── ────────── ────────
kpi_taux_consultation_periode                    5          5      ✅ OK
kpi_hospitalisation_par_diagnostic             768          5      ✅ OK
...
```

---

### Méthode 2: Validation Complète (PySpark)

**Avantages:** Analyse détaillée, statistiques approfondies, export CSV

```bash
cd /home/alban/BigData/BigData/gold_validation

# Validation basique (synthèse uniquement)
./validate_gold.sh

# Validation détaillée avec affichage des données
./validate_gold.sh --detailed

# Validation détaillée avec export CSV
./validate_gold.sh --detailed --export

# Avec échantillon personnalisé (10 lignes par table)
./validate_gold.sh --detailed --sample 10
```

**Prérequis:** Container Jupyter démarré (`docker-compose up -d jupyter`)

**Sortie détaillée:**
```
────────────────────────────────────────────────────────────────────────────
📊 TABLE: kpi_taux_hospitalisation_global
────────────────────────────────────────────────────────────────────────────
   Existe:        ✅ OUI
   Lignes:        1
   Colonnes:      7
   Taille estim.: 0.001 MB

   📋 Colonnes (7):
       1. periode_debut
       2. periode_fin
       3. nb_patients_distincts
       4. nb_patients_hospitalises
       5. nb_hospitalisations_total
       6. taux_hospitalisation
       7. taux_rehospitalisation

   ✅ Aucun problème de qualité détecté

   📄 Échantillon de données (premières 1 lignes):
      Ligne 1:
         periode_debut: 2019-01-01
         periode_fin: 2020-12-31
         nb_patients_distincts: 1,234,567
         ...
```

---

### Méthode 3: Script Python Direct

**Pour développeurs uniquement**

```bash
# Depuis le container Jupyter
docker exec -it chu_jupyter python3 /home/jovyan/gold_validation/validate_gold_tables.py --detailed

# Ou en local (si PySpark installé)
python3 validate_gold_tables.py --detailed --export-csv
```

---

## 📊 TABLES VÉRIFIÉES

Le script valide automatiquement **8 tables Gold:**

| # | Table | Description |
|---|-------|-------------|
| 1 | `kpi_taux_consultation_periode` | Consultations par période |
| 2 | `kpi_consultation_par_diagnostic` | Consultations par diagnostic |
| 3 | `kpi_taux_hospitalisation_global` | **KPI principal** |
| 4 | `kpi_hospitalisation_par_diagnostic` | Hospitalisations par pathologie |
| 5 | `kpi_hospitalisation_sexe_age` | Hospitalisations par démographie |
| 6 | `kpi_consultation_par_professionnel` | Consultations par professionnel |
| 7 | `kpi_deces_par_region_2019` | Décès par région |
| 8 | `kpi_satisfaction_par_region_2020` | Satisfaction patients |

---

## 🔍 CONTRÔLES EFFECTUÉS

### 1. Peuplement des Tables

- ✅ Vérification de l'existence de chaque table
- ✅ Comptage du nombre de lignes
- ✅ Comptage du nombre de colonnes
- ✅ Détection des tables vides

### 2. Qualité des Données

- ✅ **Valeurs nulles:** Comptage et pourcentage par colonne
- ✅ **Valeurs négatives:** Détection dans les colonnes numériques
- ✅ **Cohérence:** Vérification des types de données
- ✅ **Échantillons:** Affichage des premières lignes

### 3. Statistiques

- ✅ Nombre total de lignes
- ✅ Nombre total de colonnes
- ✅ Taille estimée de chaque table
- ✅ Moyenne lignes/table

---

## 📄 RAPPORTS GÉNÉRÉS

### Format Console

Affichage direct dans le terminal avec codes couleur:
- 🟢 **Vert:** Validation réussie
- 🟡 **Jaune:** Avertissements (tables vides, valeurs nulles)
- 🔴 **Rouge:** Erreurs (tables manquantes)

### Format CSV (option --export)

Deux fichiers CSV générés dans `reports/`:

1. **`gold_stats_YYYYMMDD_HHMMSS.csv`**
   - Statistiques de chaque table
   - Colonnes: table, exists, row_count, column_count, estimated_size_mb

2. **`gold_quality_YYYYMMDD_HHMMSS.csv`**
   - Problèmes de qualité détectés
   - Colonnes: table, issues

**Exemple:**
```
reports/
├── gold_stats_20251024_143022.csv
└── gold_quality_20251024_143022.csv
```

---

## 🎯 CODES DE SORTIE

Le script retourne un code de sortie pour automatisation:

| Code | Signification | Description |
|------|---------------|-------------|
| **0** | ✅ Succès | Toutes les tables présentes et conformes |
| **1** | ⚠️ Tables manquantes | Au moins une table n'existe pas |
| **2** | ⚠️ Problèmes de qualité | Tables présentes mais avec anomalies |

**Utilisation dans scripts:**
```bash
./validate_gold.sh
if [ $? -eq 0 ]; then
    echo "Validation OK, lancement du pipeline..."
    # Suite du traitement
fi
```

---

## 📋 EXEMPLES D'UTILISATION

### Cas 1: Vérification Rapide Quotidienne

```bash
# Chaque matin, vérifier que les données sont OK
./quick_check_trino.sh

# Si OK (code 0), envoyer un email de confirmation
# Si KO, envoyer une alerte
```

### Cas 2: Validation Avant Mise en Production

```bash
# Validation complète avec rapport détaillé
./validate_gold.sh --detailed --export

# Vérifier le code de sortie
if [ $? -eq 0 ]; then
    echo "✅ Validation réussie, données prêtes pour production"
else
    echo "❌ Validation échouée, corriger les problèmes"
    exit 1
fi
```

### Cas 3: Analyse Approfondie d'une Table

```bash
# Validation détaillée avec 20 lignes d'échantillon
./validate_gold.sh --detailed --sample 20

# Les rapports CSV peuvent être ouverts dans Excel/PowerBI
```

### Cas 4: Intégration CI/CD

```bash
# Dans un pipeline GitLab CI / GitHub Actions
script:
  - docker-compose up -d
  - ./gold_validation/quick_check_trino.sh
  - if [ $? -ne 0 ]; then exit 1; fi
```

---

## 🔧 OPTIONS DES SCRIPTS

### quick_check_trino.sh

```bash
./quick_check_trino.sh [OPTIONS]

Options:
  --sample    Affiche un échantillon de données
```

### validate_gold.sh

```bash
./validate_gold.sh [OPTIONS]

Options:
  -d, --detailed     Affiche les détails de chaque table
  -e, --export       Exporte les résultats en CSV
  -s, --sample N     Nombre de lignes d'échantillon (défaut: 5)
  -h, --help         Affiche l'aide
```

### validate_gold_tables.py (direct)

```bash
python3 validate_gold_tables.py [OPTIONS]

Options:
  --detailed         Affiche les détails de chaque table
  --export-csv       Exporte les résultats en CSV
  --sample-size N    Nombre de lignes d'échantillon
```

---

## 🐛 DÉPANNAGE

### Problème: "Container chu_trino n'est pas démarré"

**Solution:**
```bash
docker-compose up -d trino
sleep 30
./trino/init_trino_tables.sh
```

### Problème: "Container chu_jupyter n'est pas démarré"

**Solution:**
```bash
docker-compose up -d jupyter
```

### Problème: "Catalog 'minio' does not exist"

**Solution:**
```bash
./trino/init_trino_tables.sh
```

### Problème: Toutes les tables sont absentes

**Causes possibles:**
1. Pipeline Gold pas encore exécuté
2. MinIO bucket 'gold' vide
3. Configuration S3 incorrecte

**Solution:**
```bash
# Exécuter le job Gold
docker exec chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation.py
```

---

## 📈 PERFORMANCES

| Script | Méthode | Temps Exécution | Détail |
|--------|---------|-----------------|--------|
| `quick_check_trino.sh` | Trino SQL | **~5-10s** ⚡ | Comptage uniquement |
| `validate_gold.sh` (basique) | PySpark | ~30-60s | Avec statistiques |
| `validate_gold.sh --detailed` | PySpark | ~1-2 min | Analyse complète |

---

## ✅ CHECKLIST VALIDATION

```
☐ Container Trino démarré (pour quick_check)
☐ Container Jupyter démarré (pour validation complète)
☐ Pipeline Gold exécuté au moins une fois
☐ Tables Gold peuplées dans MinIO
☐ Accès réseau Docker fonctionnel
☐ Script exécutable (chmod +x)
```

---

## 📞 INTÉGRATION AVEC AUTRES OUTILS

### PowerBI

Les rapports CSV peuvent être importés dans PowerBI:
```
PowerBI → Obtenir données → CSV
→ Sélectionner gold_stats_*.csv
→ Créer visualisations
```

### Airflow

Intégrer la validation dans un DAG:
```python
from airflow.operators.bash import BashOperator

validate_task = BashOperator(
    task_id='validate_gold',
    bash_command='/path/to/quick_check_trino.sh',
    dag=dag
)
```

### Grafana / Prometheus

Parser les résultats pour créer des métriques:
```bash
# Extraire le nombre de lignes total
TOTAL_ROWS=$(./quick_check_trino.sh | grep "Lignes totales" | awk '{print $3}')
# Envoyer à Prometheus pushgateway
```

---

## 🎉 RÉSUMÉ

**Pour une vérification rapide quotidienne:**
```bash
./quick_check_trino.sh
```

**Pour une validation complète avant production:**
```bash
./validate_gold.sh --detailed --export
```

**En cas de problème:**
1. Consulter les logs des containers
2. Vérifier le peuplement MinIO
3. Exécuter le pipeline Gold
4. Relancer la validation

---

**Créé le:** 24 Octobre 2025  
**Maintenu par:** Équipe Data Engineering CHU  
**Contact:** data-engineering@chu.fr
