# Architecture Gold - Modèle Dimensionnel en Étoile

## Vue d'ensemble

Le layer Gold implémente un **modèle dimensionnel en étoile (Star Schema)** optimisé pour les analyses OLAP et le Business Intelligence.

### Pourquoi un modèle en étoile ?

✅ **Performance**: Requêtes analytiques très rapides grâce à des jointures simples
✅ **Simplicité**: Structure intuitive pour les analystes métier
✅ **Flexibilité**: Facilite le drill-down et les agrégations multi-dimensionnelles
✅ **Maintenance**: Évolutif et facile à maintenir

---

## Structure du Modèle

### 📐 Tables de Dimension (6)

Les dimensions fournissent le contexte descriptif pour l'analyse :

#### 1. **DIM_TEMPS**
- Calendrier complet 2000-2030
- Granularité: jour
- Attributs: année, trimestre, mois, semaine, jour_semaine, saison
- Utilisé par: toutes les tables de faits

#### 2. **DIM_PATIENT**
- Démographie des patients
- Clé: `patient_sk`
- Attributs: sexe, âge, catégorie_age, segment_patient, statut_activite

#### 3. **DIM_DIAGNOSTIC**
- Catalogue des diagnostics médicaux
- Clé: `diagnostic_sk`
- Attributs: code_diag, diagnostic, type_pathologie, gravité, prévalence

#### 4. **DIM_ETABLISSEMENT**
- Établissements de santé
- Clé: `etablissement_sk`
- Attributs: FINESS, raison_sociale, région, département, niveau_activité, performance

#### 5. **DIM_PROFESSIONNEL**
- Professionnels de santé
- Clé: `professionnel_sk`
- Attributs: profession, catégorie, spécialité, niveau_activité

#### 6. **DIM_LOCALISATION**
- Géographie
- Clé: `localisation_sk`
- Attributs: région, département, zone_géographique

---

### 📊 Tables de Faits (3)

Les faits contiennent les mesures quantitatives et les clés étrangères vers les dimensions :

#### 1. **FACT_CONSULTATION**
Événements de consultation

**Clés étrangères:**
- `date_consultation_fk` → DIM_TEMPS
- `patient_fk` → DIM_PATIENT
- `diagnostic_fk` → DIM_DIAGNOSTIC
- `professionnel_fk` → DIM_PROFESSIONNEL
- `etablissement_fk` → DIM_ETABLISSEMENT

**Mesures:**
- `duree_heures`: Durée de la consultation
- `est_consultation_longue`: Indicateur binaire
- `nb_consultations`: Compteur (toujours 1 au niveau grain)

#### 2. **FACT_HOSPITALISATION**
Événements d'hospitalisation

**Clés étrangères:**
- `date_admission_fk` → DIM_TEMPS
- `patient_fk` → DIM_PATIENT
- `diagnostic_fk` → DIM_DIAGNOSTIC
- `etablissement_fk` → DIM_ETABLISSEMENT

**Mesures:**
- `duree_sejour_jours`: Durée du séjour
- `score_gravite`: Score de gravité (1-3)
- `nb_hospitalisations`: Compteur

#### 3. **FACT_DECES**
Événements de décès (2019 uniquement)

**Clés étrangères:**
- `date_deces_fk` → DIM_TEMPS
- Région, département (dénormalisés pour performance)

**Mesures:**
- `age`: Âge au décès
- `nb_deces`: Compteur

---

### 📈 Data Marts Analytiques (6)

Les data marts sont des vues pré-agrégées optimisées pour répondre aux exigences métier :

#### 1. **MART_PERFORMANCE_ETABLISSEMENT**
**Exigences couvertes:** 1, 3

Indicateurs de performance par établissement et période :
- Nombre de consultations
- Nombre d'hospitalisations
- Taux de consultation par patient
- Taux d'hospitalisation global

**Dimensions:** établissement, temps, région

#### 2. **MART_DIAGNOSTIC_EPIDEMIO**
**Exigences couvertes:** 2, 4

Analyses épidémiologiques par diagnostic :
- Taux de consultation par diagnostic
- Taux d'hospitalisation par diagnostic
- Durée moyenne de séjour
- Prévalence

**Dimensions:** diagnostic, temps

#### 3. **MART_DEMOGRAPHIE**
**Exigence couverte:** 5

Analyses démographiques :
- Taux d'hospitalisation par sexe
- Taux d'hospitalisation par âge
- Durée moyenne de séjour par catégorie

**Dimensions:** sexe, categorie_age, temps

#### 4. **MART_PROFESSIONNEL**
**Exigence couverte:** 6

Performance des professionnels de santé :
- Nombre de consultations par professionnel
- Taux de consultation par patient
- Diversité des diagnostics
- Taux de consultations longues

**Dimensions:** professionnel, profession, spécialité

#### 5. **MART_DECES_LOCALISATION_2019**
**Exigence couverte:** 7

Décès par localisation en 2019 :
- Nombre de décès par région
- Nombre de décès par département
- Répartition par sexe
- Âge moyen au décès

**Dimensions:** région, département

#### 6. **MART_SATISFACTION_REGION_2020**
**Exigence couverte:** 8

Satisfaction des patients par région en 2020 :
- Score de satisfaction moyen
- Taux de recommandation
- Classement par région
- Répartition des établissements par niveau

**Dimensions:** région

---

## Diagramme du Modèle en Étoile

```
                          ┌─────────────────┐
                          │   DIM_TEMPS     │
                          ├─────────────────┤
                          │ date_id (PK)    │
                          │ annee           │
                          │ trimestre       │
                          │ mois            │
                          │ saison          │
                          └────────┬────────┘
                                   │
                                   │
         ┌─────────────────┐       │       ┌──────────────────┐
         │  DIM_PATIENT    │       │       │ DIM_DIAGNOSTIC   │
         ├─────────────────┤       │       ├──────────────────┤
         │ patient_sk (PK) │       │       │ diagnostic_sk(PK)│
         │ sexe            │       │       │ code_diag        │
         │ age             │       │       │ type_pathologie  │
         │ categorie_age   │       │       │ gravite          │
         └────────┬────────┘       │       └─────────┬────────┘
                  │                │                 │
                  │                │                 │
                  └────────────────┼─────────────────┘
                                   │
                                   │
                     ┌─────────────▼───────────────┐
                     │  FACT_CONSULTATION         │
                     ├────────────────────────────┤
                     │ date_consultation_fk  (FK) │
                     │ patient_fk            (FK) │
                     │ diagnostic_fk         (FK) │
                     │ professionnel_fk      (FK) │
                     │ etablissement_fk      (FK) │
                     ├────────────────────────────┤
                     │ duree_heures          (M)  │
                     │ nb_consultations      (M)  │
                     └──────────┬──┬──────────────┘
                                │  │
                   ┌────────────┘  └────────────┐
                   │                            │
         ┌─────────▼──────────┐      ┌─────────▼───────────┐
         │ DIM_PROFESSIONNEL  │      │ DIM_ETABLISSEMENT   │
         ├────────────────────┤      ├─────────────────────┤
         │ professionnel_sk(PK)      │ etablissement_sk(PK)│
         │ profession         │      │ finess_site         │
         │ specialite         │      │ region              │
         └────────────────────┘      └─────────────────────┘


                     ┌─────────────────────────┐
                     │  FACT_HOSPITALISATION   │
                     ├─────────────────────────┤
                     │ date_admission_fk  (FK) │
                     │ patient_fk         (FK) │
                     │ diagnostic_fk      (FK) │
                     │ etablissement_fk   (FK) │
                     ├─────────────────────────┤
                     │ duree_sejour_jours  (M) │
                     │ score_gravite       (M) │
                     │ nb_hospitalisations (M) │
                     └─────────────────────────┘


                     ┌─────────────────────┐
                     │    FACT_DECES       │
                     ├─────────────────────┤
                     │ date_deces_fk  (FK) │
                     │ region              │
                     │ departement         │
                     ├─────────────────────┤
                     │ age             (M) │
                     │ nb_deces        (M) │
                     └─────────────────────┘
```

**Légende:**
- (PK) = Primary Key / Clé primaire
- (FK) = Foreign Key / Clé étrangère
- (M) = Measure / Mesure

---

## Mapping Exigences Métier → Data Marts

| # | Exigence Métier | Data Mart | Tables Sources |
|---|----------------|-----------|----------------|
| 1 | Taux de consultation par établissement X période Y | `mart_performance_etablissement` | fact_consultation, dim_etablissement, dim_temps |
| 2 | Taux de consultation par diagnostic X période Y | `mart_diagnostic_epidemio` | fact_consultation, dim_diagnostic, dim_temps |
| 3 | Taux global d'hospitalisation période Y | `mart_performance_etablissement` | fact_hospitalisation, dim_temps |
| 4 | Taux d'hospitalisation par diagnostic période Y | `mart_diagnostic_epidemio` | fact_hospitalisation, dim_diagnostic, dim_temps |
| 5 | Taux d'hospitalisation par sexe et âge | `mart_demographie` | fact_hospitalisation, dim_patient |
| 6 | Taux de consultation par professionnel | `mart_professionnel` | fact_consultation, dim_professionnel |
| 7 | Nombre de décès par localisation (2019) | `mart_deces_localisation_2019` | fact_deces, dim_localisation |
| 8 | Taux de satisfaction par région (2020) | `mart_satisfaction_region_2020` | silver_satisfaction |

---

## Utilisation

### Exécution du pipeline Gold

```bash
# Dans le container Spark
cd /home/jovyan/work/spark_jobs/main_jobs
python3 gold_star_schema.py
```

### Exemple de requêtes analytiques

#### Exigence 1: Consultations par établissement
```sql
SELECT
    finess_site,
    raison_sociale_site,
    region,
    annee,
    trimestre,
    nb_consultations,
    taux_consultation_par_patient
FROM mart_performance_etablissement
WHERE finess_site = 'XXXXXXX'
  AND annee = 2019
ORDER BY trimestre;
```

#### Exigence 5: Hospitalisations par démographie
```sql
SELECT
    sexe,
    categorie_age,
    nb_hospitalisations,
    nb_patients_hospitalises,
    duree_moyenne_sejour
FROM mart_demographie
WHERE annee = 2019
ORDER BY sexe, categorie_age;
```

#### Exigence 7: Décès par région (2019)
```sql
SELECT
    region,
    nb_deces_total,
    age_moyen_deces,
    taux_deces_hommes_pct,
    taux_deces_femmes_pct
FROM mart_deces_localisation_2019
ORDER BY nb_deces_total DESC;
```

---

## Avantages du Modèle en Étoile

### 🚀 Performance
- Jointures simples (1 niveau maximum)
- Index efficaces sur les clés étrangères
- Requêtes rapides même sur gros volumes

### 📊 Analytique
- Drill-down/roll-up naturels via les hiérarchies
- Agrégations multi-dimensionnelles
- Slicing & dicing facilités

### 🔧 Maintenance
- Structure stable dans le temps
- Ajout de nouvelles dimensions facile
- Évolution des mesures sans impact structure

### 👥 Adoption
- Compréhension intuitive pour les analystes
- Compatibilité avec les outils BI (Superset, Tableau, Power BI)
- Documentation naturelle via les noms de colonnes

---

## Intégration avec Superset

Les data marts sont conçus pour être directement connectés à Apache Superset :

1. **Connecter Superset au bucket Gold MinIO**
2. **Créer des datasets** pour chaque mart
3. **Construire des dashboards** répondant aux exigences métier

Exemples de dashboards :
- Performance des établissements (Exigences 1, 3)
- Épidémiologie et diagnostics (Exigences 2, 4)
- Analyses démographiques (Exigence 5)
- Performance des professionnels (Exigence 6)
- Mortalité par région 2019 (Exigence 7)
- Satisfaction régionale 2020 (Exigence 8)

---

## Évolutions Futures

### SCD Type 2 (Slowly Changing Dimensions)
Pour historiser les changements dans les dimensions :
- Ajout de `date_debut_validite`, `date_fin_validite`
- Ajout de `version`, `is_current`

### Agrégats pré-calculés
Pour optimiser les requêtes fréquentes :
- Agrégats annuels
- Agrégats régionaux
- Top N pré-calculés

### Nouvelles dimensions
- Dim_Mutuelle
- Dim_Pathologie (hiérarchie CIM-10)
- Dim_Service_Hospitalier

### Nouveaux faits
- Fact_Prescription
- Fact_Urgence
- Fact_Rendez_vous

---

## Références

- Script principal: `/spark_jobs/main_jobs/gold_star_schema.py`
- Architecture Bronze: `/spark_jobs/main_jobs/bronze_ingestion.py`
- Architecture Silver: `/spark_jobs/main_jobs/silver_transformation.py`

**Documentation Kimball:** [The Data Warehouse Toolkit](https://www.kimballgroup.com/)
