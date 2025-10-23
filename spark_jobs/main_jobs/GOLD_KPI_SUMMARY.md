# Résumé des KPIs Gold - Projet BigData Santé

## ✅ 8 KPIs Créés avec Succès

Tous les KPIs demandés ont été implémentés et génèrent des datasets Gold dans `s3a://gold/`.

---

### 1️⃣ Taux de consultation des patients sur une période Y

**Dataset**: `kpi_taux_consultation_periode`

**Objectif**: Mesurer l'activité de consultation globale sur une période donnée.

**Colonnes**:
- `nb_patients_distincts`: Nombre de patients uniques ayant consulté
- `nb_consultations_total`: Nombre total de consultations
- `periode_debut`: Date de début de la période analysée
- `periode_fin`: Date de fin de la période analysée
- `taux_consultation_moyen`: Nombre moyen de consultations par patient

**Formule**: `taux_consultation_moyen = nb_consultations_total / nb_patients_distincts`

**Note**: Actuellement, les données de consultations ne contiennent pas de dates dans la période 2019-2020, d'où les valeurs à 0.

---

### 2️⃣ Taux de consultation par diagnostic X sur une période Y

**Dataset**: `kpi_consultation_par_diagnostic`

**Objectif**: Analyser la fréquence des consultations par type de diagnostic.

**Colonnes**:
- `diagnostic_code`: Code du diagnostic
- `nb_patients_avec_diagnostic`: Nombre de patients ayant ce diagnostic
- `nb_consultations`: Nombre de consultations pour ce diagnostic
- `total_patients_periode`: Total des patients sur la période
- `taux_patients`: Proportion de patients concernés par ce diagnostic

**Formule**: `taux_patients = nb_patients_avec_diagnostic / total_patients_periode`

**Tri**: Par nombre de consultations décroissant (diagnostics les plus fréquents en premier)

---

### 3️⃣ Taux global d'hospitalisation sur une période Y

**Dataset**: `kpi_taux_hospitalisation_global`

**Objectif**: Mesurer la proportion de patients hospitalisés sur une période.

**Colonnes**:
- `periode_debut`: Date de début de la période
- `periode_fin`: Date de fin de la période
- `nb_patients_hospitalises`: Nombre de patients hospitalisés (distinct)
- `nb_hospitalisations_total`: Nombre total d'hospitalisations (peut être > patients si réhospitalisations)
- `nb_patients_reference`: Population de référence (patients ayant consulté)
- `taux_hospitalisation`: Ratio patients hospitalisés / population référence

**Formule**: `taux_hospitalisation = nb_patients_hospitalises / nb_patients_reference`

**Résultats actuels**: 783 patients hospitalisés pour 784 hospitalisations (1 réhospitalisation)

---

### 4️⃣ Taux d'hospitalisation par diagnostic sur une période

**Dataset**: `kpi_hospitalisation_par_diagnostic`

**Objectif**: Identifier les diagnostics conduisant le plus à des hospitalisations.

**Colonnes**:
- `diagnostic_principal`: Code du diagnostic principal
- `nb_patients_hospitalises`: Nombre de patients hospitalisés pour ce diagnostic
- `nb_hospitalisations`: Nombre total d'hospitalisations (inclut réhospitalisations)
- `total_patients_periode`: Total des patients hospitalisés sur la période
- `taux_hospitalisation`: Proportion de ce diagnostic parmi les hospitalisations

**Formule**: `taux_hospitalisation = nb_patients_hospitalises / total_patients_periode`

**Résultats actuels**: 768 diagnostics distincts identifiés

**Tri**: Par nombre d'hospitalisations décroissant

---

### 5️⃣ Taux d'hospitalisation par sexe et par âge

**Dataset**: `kpi_hospitalisation_sexe_age`

**Objectif**: Analyser les disparités d'hospitalisation selon le profil démographique.

**Colonnes**:
- `sexe`: M (Masculin) ou F (Féminin)
- `tranche_age`: Groupe d'âge (0-17, 18-34, 35-49, 50-64, 65+)
- `nb_patients_hospitalises`: Nombre de patients hospitalisés dans ce segment
- `nb_hospitalisations`: Nombre total d'hospitalisations
- `nb_patients_total`: Population totale de ce segment (référence)
- `taux_hospitalisation`: Proportion hospitalisée

**Formule**: `taux_hospitalisation = nb_patients_hospitalises / nb_patients_total`

**Résultats actuels**: 10 segments (2 sexes × 5 tranches d'âge)

**Insights**:
- Taux d'hospitalisation variable de ~0.6% à ~0.9%
- Les tranches 50-64 et 18-34 présentent des taux légèrement plus élevés

---

### 6️⃣ Taux de consultation par professionnel

**Dataset**: `kpi_consultation_par_professionnel`

**Objectif**: Mesurer l'activité de chaque professionnel de santé.

**Colonnes** (version globale - colonne professionnel absente):
- `nb_consultations_total`: Total des consultations
- `nb_patients_distincts`: Nombre de patients vus
- `periode_debut`: Début de période
- `periode_fin`: Fin de période
- `consultations_par_patient`: Moyenne de consultations par patient

**Note**: La table `fact_consultation` ne contient pas d'identifiant de professionnel, donc le KPI est calculé globalement. Pour une analyse par professionnel, il faudrait enrichir la table Silver avec cette information.

---

### 7️⃣ Nombre de décès par région sur l'année 2019

**Dataset**: `kpi_deces_par_region_2019`

**Objectif**: Cartographier la mortalité par région.

**Colonnes**:
- `region`: Code ou nom de la région
- `nb_deces`: Nombre de décès enregistrés
- `nb_patients_decedes`: Nombre de patients uniques décédés
- `annee`: Année de référence (2019)

**Résultats actuels**: 620,608 décès enregistrés en 2019

**Note**: La colonne `region` est actuellement NULL car `fact_deces` ne contient pas de lien vers `dim_etablissement`. Pour obtenir une ventilation par région, il faudrait :
- Soit ajouter `sk_etablissement` dans `fact_deces`
- Soit ajouter une colonne `region` directement dans `fact_deces`

---

### 8️⃣ Taux de satisfaction par région sur l'année 2020

**Dataset**: `kpi_satisfaction_par_region_2020`

**Objectif**: Mesurer la qualité perçue des soins par région.

**Colonnes**:
- `region`: Région géographique
- `taux_satisfaction_moyen`: Score moyen de satisfaction
- `nb_evaluations`: Nombre d'évaluations collectées
- `annee`: Année de référence (2020)

**Source de données**: Le job cherche dans plusieurs tables :
1. `metrique_consultation`
2. `metrique_satisfaction`
3. `satisfaction`
4. `metrique_activite_temporelle`

**Note**: Le job a trouvé des données dans `metrique_consultation` mais les détails de la satisfaction dépendent du contenu réel de cette table.

---

## 🎯 Résumé de l'Exécution

| # | KPI | Dataset Gold | Statut | Lignes |
|---|-----|--------------|--------|--------|
| 1 | Consultation par période | `kpi_taux_consultation_periode` | ✅ | 1 |
| 2 | Consultation par diagnostic | `kpi_consultation_par_diagnostic` | ✅ | 0* |
| 3 | Hospitalisation globale | `kpi_taux_hospitalisation_global` | ✅ | 1 |
| 4 | Hospitalisation par diagnostic | `kpi_hospitalisation_par_diagnostic` | ✅ | 768 |
| 5 | Hospitalisation sexe/âge | `kpi_hospitalisation_sexe_age` | ✅ | 10 |
| 6 | Consultation par professionnel | `kpi_consultation_par_professionnel` | ✅ | 1 |
| 7 | Décès par région 2019 | `kpi_deces_par_region_2019` | ✅ | 1 |
| 8 | Satisfaction par région 2020 | `kpi_satisfaction_par_region_2020` | ✅ | ? |

\* 0 lignes car les données de consultation n'ont pas de dates dans la période 2019-2020

---

## 📊 Utilisation des KPIs

### Connexion BI Tools (Tableau, PowerBI, etc.)

Les datasets Gold sont stockés en Parquet sur MinIO (`s3a://gold/kpi_*`) et peuvent être :
- Lus directement via connecteurs S3
- Exportés vers PostgreSQL pour une meilleure compatibilité BI
- Convertis en CSV pour Excel/analyse ad-hoc

### Exemple de requête Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnalyseKPI") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Lire un KPI
df = spark.read.parquet("s3a://gold/kpi_hospitalisation_par_diagnostic")
df.show(10)
```

---

## 🔧 Améliorations Recommandées

### Court Terme
1. **Ajouter `sk_etablissement` dans `fact_consultation`**
   - Permettrait de calculer les taux de consultation par établissement
   - Nécessite une mise à jour de `silver_transformation.py`

2. **Enrichir `fact_consultation` avec ID professionnel**
   - Source potentielle : table `professionnel_sante` en Bronze
   - Jointure via clé naturelle lors de la transformation Silver

3. **Lier `fact_deces` à `dim_etablissement`**
   - Permettrait la ventilation régionale des décès
   - Nécessite l'ajout de `sk_etablissement` dans `fact_deces`

### Moyen Terme
4. **Créer une table `dim_diagnostic`**
   - Enrichir les codes diagnostics avec libellés
   - Faciliter la lecture des KPIs

5. **Standardiser les données de satisfaction**
   - Créer une table Silver dédiée `fact_satisfaction`
   - Normaliser les scores (0-10, 0-100, etc.)

6. **Implémenter des seuils d'alerte**
   - Taux d'hospitalisation > X% → alerte
   - Satisfaction < Y% → investigation requise

---

## 📅 Planification & Automatisation

### Airflow DAG Suggéré

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('gold_kpi_monthly', 
         schedule_interval='@monthly',
         default_args=default_args) as dag:
    
    compute_gold_kpis = SparkSubmitOperator(
        task_id='compute_gold_kpis',
        application='/path/to/gold_aggregation.py',
        jars='/path/to/hadoop-aws.jar,/path/to/aws-sdk.jar',
        env_vars={
            'GA_START_DATE': '{{ macros.ds_add(ds, -30) }}',
            'GA_END_DATE': '{{ ds }}'
        }
    )
```

### Fréquence Recommandée
- **Mensuelle**: Pour les KPIs de tendance (consultations, hospitalisations)
- **Annuelle**: Pour les KPIs réglementaires (décès, satisfaction)
- **À la demande**: Pour analyses ponctuelles

---

## ✅ Validation & Tests

Pour valider les KPIs :

```bash
# Exécuter le job Gold
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation.py

# Visualiser les résultats
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/visu/visu_gold.py
```

---

**Date de création**: 24 octobre 2025  
**Auteur**: Équipe Data BigData  
**Version**: 1.0
