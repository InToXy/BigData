# Gold Aggregation Job

## Vue d'ensemble

Le job `gold_aggregation.py` produit des **datasets Gold** (KPIs/métriques agrégées) à partir de la zone Silver, optimisés pour la consommation par les outils BI et d'analyse.

## Datasets produits

Le job crée actuellement **5 datasets Gold** dans `s3a://gold/` :

### 1. `consultation_rate_diag_I10`
- **Description** : Taux de consultations pour un diagnostic spécifique (I10 par défaut)
- **Colonnes** : `diagnosis_code`, `nb_patients_with_diag`, `nb_patients_total`, `rate`
- **Source** : `fact_consultation`
- **Période** : Configurable via `GA_START_DATE` / `GA_END_DATE`

### 2. `global_hospitalization_rate`
- **Description** : Taux global d'hospitalisation sur la période
- **Colonnes** : `metric`, `nb_hospitalized`, `nb_patients`, `rate`
- **Source** : `fact_hospitalisation`, `fact_consultation`
- **Notes** : Compare patients hospitalisés vs patients consultant

### 3. `hospitalization_by_diagnosis`
- **Description** : Taux d'hospitalisation ventilé par diagnostic principal
- **Colonnes** : `diagnostic_principal`, `nb_hospitalized`, `nb_patients_total`, `rate`
- **Source** : `fact_hospitalisation`
- **Lignes** : ~768 diagnostics distincts

### 4. `hospitalization_by_sex_age`
- **Description** : Taux d'hospitalisation par sexe et tranche d'âge
- **Colonnes** : `sexe`, `age_bucket`, `nb_hospitalized`, `nb_patients`, `rate`
- **Source** : `fact_hospitalisation`, `dim_patient`
- **Tranches d'âge** : 0-17, 18-34, 35-49, 50-64, 65+

### 5. `deaths_by_region_2019`
- **Description** : Nombre de décès par région pour l'année 2019
- **Colonnes** : `region_normalisee`, `nb_deces`
- **Source** : `fact_deces`, `dim_etablissement`
- **Année** : Fixée à 2019 (configurable dans le code)

## KPIs non produits (colonnes manquantes)

Certains KPIs n'ont pas pu être générés en raison de colonnes absentes dans Silver :

- ❌ **`consultation_rate_by_establishment`** : `sk_etablissement` absent de `fact_consultation` et `dim_patient`
- ❌ **`consultation_by_professional`** : Aucune colonne identifiant le professionnel dans `fact_consultation`
- ❌ **`satisfaction_by_region_2020`** : Données de satisfaction non détectées dans les tables métriques

## Exécution

### Commande standard
```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation.py
```

### Avec paramètres personnalisés
```bash
docker exec -it chu_jupyter bash -c "
export GA_START_DATE='2020-01-01'
export GA_END_DATE='2020-12-31'
export GA_DIAGNOSIS_CODE='J18'
spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/main_jobs/gold_aggregation.py
"
```

### Variables d'environnement disponibles

| Variable | Par défaut | Description |
|----------|------------|-------------|
| `GA_START_DATE` | `2019-01-01` | Date de début de la période d'analyse |
| `GA_END_DATE` | `2020-12-31` | Date de fin de la période d'analyse |
| `GA_DIAGNOSIS_CODE` | `I10` | Code diagnostic pour le KPI consultation_rate_diag |
| `MINIO_ENDPOINT` | `http://minio:9000` | Endpoint MinIO/S3 |
| `MINIO_ACCESS` | `minioadmin` | Access key MinIO |
| `MINIO_SECRET` | `minioadmin123` | Secret key MinIO |
| `SILVER_BUCKET` | `silver` | Nom du bucket Silver |
| `GOLD_BUCKET` | `gold` | Nom du bucket Gold |

## Visualisation des données Gold

Pour explorer les datasets créés :

```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/visu/visu_gold.py
```

## Architecture

```
Silver Zone (s3a://silver/)
  ├─ fact_consultation      (1,027,157 lignes)
  ├─ fact_hospitalisation   (2,479 lignes)
  ├─ fact_deces             (620,608 lignes)
  ├─ dim_patient            (100,000 lignes)
  └─ dim_etablissement      (416,665 lignes)
        ↓
    [gold_aggregation.py]
        ↓
Gold Zone (s3a://gold/)
  ├─ consultation_rate_diag_I10
  ├─ global_hospitalization_rate
  ├─ hospitalization_by_diagnosis
  ├─ hospitalization_by_sex_age
  └─ deaths_by_region_2019
```

## Détection flexible des colonnes

Le job utilise des heuristiques pour trouver les colonnes nécessaires :

- **Dates** : cherche `date_consultation`, `date`, `date_entree`, `date_hospitalisation`, etc.
- **Diagnostics** : cherche des colonnes contenant `diag`, `diagn`, `code`
- **Professionnels** : cherche `prof`, `pract`, `medecin`
- **Régions** : cherche `region`, `region_normalisee`, `region_code`

Si une colonne requise n'est pas trouvée, le KPI correspondant est **sauté** avec un message d'avertissement.

## Gestion des erreurs

- **Table Silver introuvable** → KPI sauté
- **Colonne manquante** → KPI sauté avec message explicite
- **Écriture S3A échouée** → Fallback sur `/tmp/gold/<dataset>/` (local)
- **Données vides** → DataFrame créé avec 0 lignes (pas d'erreur)

## Performances

- **Temps d'exécution** : ~1-2 minutes (dépend du volume de données)
- **Mémoire Spark** : 434 MiB par défaut (suffisant pour les volumétries actuelles)
- **Parallélisme** : Configuré automatiquement par Spark (1 executor local)

## Améliorations futures

1. **Ajouter les colonnes manquantes dans Silver** :
   - `sk_etablissement` dans `fact_consultation` (via jointure avec `dim_patient`)
   - Colonne identifiant professionnel dans `fact_consultation`
   - Données de satisfaction normalisées dans une table dédiée

2. **Étendre les KPIs** :
   - Évolution temporelle (tendances mensuelles/annuelles)
   - Benchmarks par région/établissement
   - Prédictions (avec MLlib)

3. **Optimisations** :
   - Utiliser Delta Lake pour ACID et time-travel
   - Partitionner les datasets Gold par date/région
   - Cache des tables fréquemment utilisées

4. **Intégration Airflow** :
   - Créer un DAG pour planifier l'exécution quotidienne/hebdomadaire
   - Ajouter alertes et monitoring (échecs, volumétrie, SLA)

## Contact / Support

En cas de problème :
1. Vérifier les logs : `docker logs chu_jupyter`
2. Valider les données Silver : `/home/jovyan/jobs/visu/visu_silver.py`
3. Tester la connexion MinIO : `/home/jovyan/jobs/main_jobs/test_gold_connection.py`
