# DOCUMENTATION DES TABLES GOLD - DATA LAKE MÉDICAL

**Date de génération:** 24 Octobre 2025  
**Zone:** Gold (Agrégation)  
**Format:** Apache Parquet  
**Stockage:** MinIO S3A (s3a://gold/)

---

## 📊 VUE D'ENSEMBLE

La zone Gold contient **12 tables d'agrégation** représentant les KPIs métier clés du système de santé. Ces tables sont optimisées pour l'analyse et le reporting, avec une réduction de **99.996%** du volume de données par rapport à la zone Bronze.

### Statistiques Globales

| Métrique | Valeur |
|----------|--------|
| **Nombre total de tables** | 12 |
| **Lignes totales** | 1,563 |
| **Colonnes totales** | 55 |
| **Colonnes uniques** | 32 |
| **Stockage total** | ~0.03 MB |
| **Moyenne lignes/table** | 130 |
| **Moyenne colonnes/table** | 4.6 |

---

## 📋 CATALOGUE DES TABLES

### 1. KPI - Taux de Consultation par Période

**Nom de la table:** `kpi_taux_consultation_periode`  
**Chemin:** `s3a://gold/kpi_taux_consultation_periode`  
**Lignes:** ~1-10 lignes (selon périodes)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `periode_debut` | Date | Date de début de la période d'analyse |
| `periode_fin` | Date | Date de fin de la période d'analyse |
| `nb_patients_distincts` | Long | Nombre de patients uniques ayant consulté |
| `nb_consultations_total` | Long | Nombre total de consultations |
| `taux_consultation_moyen` | Double | Taux moyen de consultation (consultations/patient) |

#### Utilisation
- Analyse de l'activité de consultation sur une période donnée
- Suivi de l'évolution temporelle de l'activité
- Calcul de la charge de travail

#### Exemple de données
```
periode_debut: 2019-01-01, periode_fin: 2020-12-31
nb_patients_distincts: 1,234,567
nb_consultations_total: 3,456,789
taux_consultation_moyen: 2.8
```

---

### 2. KPI - Taux de Consultation par Établissement

**Nom de la table:** `kpi_taux_consultation_etablissement`  
**Chemin:** `s3a://gold/kpi_taux_consultation_etablissement`  
**Lignes:** ~100-500 lignes (nombre d'établissements)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `etablissement_id` | String | Identifiant de l'établissement |
| `nb_consultations` | Long | Nombre de consultations dans cet établissement |
| `nb_patients_distincts` | Long | Nombre de patients uniques |
| `taux_consultation` | Double | Taux de consultation moyen |
| `periode_debut` | Date | Début de la période analysée |
| `periode_fin` | Date | Fin de la période analysée |

#### Utilisation
- Comparaison de l'activité entre établissements
- Identification des établissements à forte/faible activité
- Planification des ressources

---

### 3. KPI - Taux de Consultation pour Diagnostic Spécifique

**Nom de la table:** `consultation_rate_diag_I10`  
**Chemin:** `s3a://gold/consultation_rate_diag_I10`  
**Lignes:** 1 ligne (agrégation globale)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `diagnostic_code` | String | Code du diagnostic (ex: I10 - Hypertension) |
| `nb_patients` | Long | Nombre de patients avec ce diagnostic |
| `nb_consultations` | Long | Nombre de consultations pour ce diagnostic |
| `taux_consultation` | Double | Taux de consultation moyen |

#### Utilisation
- Analyse ciblée d'une pathologie spécifique
- Étude de la prévalence d'un diagnostic
- Suivi épidémiologique

---

### 4. KPI - Taux d'Hospitalisation Global

**Nom de la table:** `kpi_taux_hospitalisation_global`  
**Chemin:** `s3a://gold/kpi_taux_hospitalisation_global`  
**Lignes:** 1 ligne (KPI unique)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `periode_debut` | Date | Début de la période |
| `periode_fin` | Date | Fin de la période |
| `nb_patients_distincts` | Long | Nombre de patients distincts |
| `nb_patients_hospitalises` | Long | Nombre de patients ayant été hospitalisés |
| `nb_hospitalisations_total` | Long | Nombre total d'hospitalisations |
| `taux_hospitalisation` | Double | Pourcentage de patients hospitalisés |
| `taux_rehospitalisation` | Double | Ratio hospitalisations/patients hospitalisés |

#### Utilisation
- Indicateur de performance globale du système
- Suivi de l'évolution de l'hospitalisation
- Benchmark avec d'autres périodes/régions

#### Exemple de données
```
periode: 2019-2020
nb_patients_distincts: 2,000,000
nb_patients_hospitalises: 150,000
taux_hospitalisation: 7.5%
taux_rehospitalisation: 1.2
```

---

### 5. KPI - Hospitalisations par Diagnostic ⭐

**Nom de la table:** `kpi_hospitalisation_par_diagnostic`  
**Chemin:** `s3a://gold/kpi_hospitalisation_par_diagnostic`  
**Lignes:** 768 lignes (diagnostics avec hospitalisation)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `diagnostic_principal` | String | Code CIM-10 du diagnostic principal |
| `nb_hospitalisations` | Long | Nombre total d'hospitalisations |
| `nb_patients_hospitalises` | Long | Nombre de patients distincts hospitalisés |
| `taux_hospitalisation` | Double | Ratio hospitalisations/patients |
| `total_patients_periode` | Long | Population totale de référence |

#### Utilisation
- **Analyse prioritaire:** Identification des pathologies nécessitant le plus d'hospitalisations
- Planification des lits et ressources hospitalières
- Étude épidémiologique des hospitalisations
- Priorisation des programmes de prévention

#### Top 5 Diagnostics (exemple)
```
1. I10 - Hypertension: 45,234 hospitalisations
2. E11 - Diabète type 2: 32,145 hospitalisations
3. J44 - BPCO: 28,901 hospitalisations
4. I50 - Insuffisance cardiaque: 24,567 hospitalisations
5. F32 - Dépression: 19,234 hospitalisations
```

---

### 6. KPI - Hospitalisations par Sexe et Âge ⭐

**Nom de la table:** `kpi_hospitalisation_sexe_age`  
**Chemin:** `s3a://gold/kpi_hospitalisation_sexe_age`  
**Lignes:** 10 lignes (2 sexes × 5 tranches d'âge)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `sexe` | String | Sexe (M/F) |
| `tranche_age` | String | Tranche d'âge (ex: 0-18, 19-35, 36-50, 51-65, 66+) |
| `nb_patients_hospitalises` | Long | Nombre de patients hospitalisés |
| `nb_hospitalisations` | Long | Nombre total d'hospitalisations |
| `taux_hospitalisation` | Double | Pourcentage d'hospitalisation |

#### Utilisation
- **Analyse démographique:** Identification des populations à risque
- Planification des services spécialisés (pédiatrie, gériatrie)
- Études de genre en santé publique
- Prévision des besoins futurs

#### Exemple de distribution
```
Hommes 66+: 25,000 patients, 35,000 hospitalisations (taux: 140%)
Femmes 66+: 28,000 patients, 38,000 hospitalisations (taux: 135%)
Hommes 0-18: 5,000 patients, 5,500 hospitalisations (taux: 110%)
```

---

### 7. KPI - Consultations par Professionnel

**Nom de la table:** `kpi_consultation_par_professionnel`  
**Chemin:** `s3a://gold/kpi_consultation_par_professionnel`  
**Lignes:** ~50-200 lignes (professionnels de santé)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `professionnel_id` | String | Identifiant du professionnel |
| `specialite` | String | Spécialité médicale |
| `nb_consultations` | Long | Nombre de consultations effectuées |
| `nb_patients_distincts` | Long | Nombre de patients uniques suivis |
| `taux_consultation_moyen` | Double | Consultations par patient |

#### Utilisation
- Analyse de la charge de travail par professionnel
- Évaluation de la productivité
- Planification des recrutements
- Distribution des patients entre professionnels

---

### 8. KPI - Décès par Région 2019

**Nom de la table:** `kpi_deces_par_region_2019`  
**Chemin:** `s3a://gold/kpi_deces_par_region_2019`  
**Lignes:** 1-20 lignes (régions)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `region` | String | Nom ou code de la région |
| `annee` | Integer | Année (2019) |
| `nb_deces` | Long | Nombre total de décès |
| `nb_patients_decedes` | Long | Nombre de patients distincts décédés |

#### Utilisation
- Analyse de mortalité régionale
- Comparaison inter-régionale
- Études épidémiologiques
- Indicateurs de santé publique

---

### 9. KPI - Satisfaction par Région et Année

**Nom de la table:** `kpi_satisfaction_region_annee`  
**Chemin:** `s3a://gold/kpi_satisfaction_region_annee`  
**Lignes:** ~20-100 lignes (régions × années)

#### Schéma

| Colonne | Type | Description |
|---------|------|-------------|
| `region` | String | Région géographique |
| `annee` | Integer | Année d'enquête |
| `score_satisfaction_moyen` | Double | Score moyen de satisfaction (0-10) |
| `nb_repondants` | Long | Nombre de patients ayant répondu |

#### Utilisation
- Évaluation de la qualité des soins perçue
- Comparaison régionale de la satisfaction
- Suivi temporel de l'amélioration
- Identification des zones nécessitant des améliorations

---

## 🔍 TABLES ANCIENNES (RÉFÉRENCE)

Les tables suivantes sont des versions antérieures maintenues pour compatibilité:

### hospitalization_by_diagnosis
- **Remplacée par:** `kpi_hospitalisation_par_diagnostic`
- **Statut:** Conservée pour référence historique

### hospitalization_by_sex_age
- **Remplacée par:** `kpi_hospitalisation_sexe_age`
- **Statut:** Conservée pour référence historique

### consultation_rate
- **Remplacée par:** `kpi_taux_consultation_periode`
- **Statut:** Conservée pour référence historique

---

## 💡 RECOMMANDATIONS D'UTILISATION

### Pour les Analystes Métier
✅ Utiliser les tables `kpi_*` pour tous les nouveaux rapports  
✅ Prioriser `kpi_hospitalisation_par_diagnostic` et `kpi_hospitalisation_sexe_age`  
✅ Combiner plusieurs KPIs pour des analyses croisées  

### Pour les Data Scientists
✅ Tables optimisées pour le feature engineering  
✅ Format Parquet compatible avec Pandas, Spark, Dask  
✅ Données pré-agrégées, calculs rapides  

### Pour les Développeurs BI
✅ Connecteurs S3A standards  
✅ Compatible Tableau, Power BI, Superset  
✅ Temps de réponse < 100ms pour la plupart des requêtes  

---

## 📈 MÉTRIQUES DE PERFORMANCE

| Opération | Temps Moyen | Notes |
|-----------|-------------|-------|
| Lecture complète d'une table | < 0.1s | Format Parquet optimisé |
| Agrégation simple | < 0.2s | Données déjà pré-agrégées |
| Jointure entre KPIs | < 0.5s | Tables de petite taille |
| Scan complet zone Gold | < 2s | 12 tables, 1,563 lignes |

### Compression Réalisée

```
Bronze → Silver → Gold
7.6M lignes → 2.17M lignes → 1,563 lignes
726 MB → 207 MB → 0.03 MB
```

**Taux de compression global:** 99.996% (Bronze → Gold)

---

## 🔗 INTÉGRATION

### Accès depuis Spark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .getOrCreate()

# Lire un KPI
df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
df.show()
```

### Accès depuis Pandas
```python
import pandas as pd
from pyspark.sql import SparkSession

spark = get_spark_session()
df_spark = spark.read.parquet("s3a://gold/kpi_hospitalisation_sexe_age")
df_pandas = df_spark.toPandas()
```

### Accès depuis des outils BI
- **Superset:** Connexion via SQLAlchemy + Spark Thrift Server
- **Tableau:** Connexion Spark ODBC/JDBC
- **Power BI:** Connecteur Spark natif

---

## 📚 DOCUMENTATION COMPLÉMENTAIRE

- **Guide d'exécution:** `spark_jobs/main_jobs/README_GOLD.md`
- **Détails KPIs:** `spark_jobs/main_jobs/GOLD_KPI_SUMMARY.md`
- **Performance comparative:** `PERFORMANCE_ZONES.md`

---

**Dernière mise à jour:** 24 Octobre 2025  
**Responsable:** Équipe Data Engineering CHU
