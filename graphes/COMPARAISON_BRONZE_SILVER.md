# 🔄 Comparaison Bronze vs Silver

## 📊 Vue d'ensemble

| Caractéristique | Bronze Layer | Silver Layer |
|-----------------|--------------|--------------|
| **Type de données** | Brutes, non transformées | Transformées, nettoyées |
| **Source** | PostgreSQL + CSV | Bucket Bronze |
| **Nombre de datasets** | ~28 tables | ~10 tables |
| **Volume total** | ~2.9 GB | ~0.7 GB |
| **Lignes totales** | ~7.4M lignes | ~1.8M lignes |
| **Format** | Parquet Snappy | Parquet Snappy |
| **Schéma** | Original (multi-sources) | Normalisé (star schema) |
| **Qualité** | Non validée | Validée et cohérente |
| **Transformation** | Aucune | ETL complet |

## 🗂️ Structure des données

### Bronze Layer - Architecture Plat

```
bronze/
├── activites_professionnels/     (données brutes activité)
├── adherents/                     (données brutes adhérents)
├── consultations/                 (données brutes consultations)
├── dan_mco_2015/                  (données brutes MCO)
├── deces/                         (données brutes décès)
├── diagnostics/                   (données brutes diagnostics)
├── dpa_had_2015/                  (données brutes HAD)
├── dpa_ssr_2013/                  (données brutes SSR)
├── etablissements/                (données brutes établissements)
├── hospitalisations/              (données brutes hospitalisations)
├── medicaments/                   (données brutes médicaments)
├── mutuelles/                     (données brutes mutuelles)
├── patients/                      (données brutes patients)
├── prescriptions/                 (données brutes prescriptions)
├── professionnels/                (données brutes professionnels)
└── ... (13 autres tables)
```

**Caractéristiques Bronze** :
- ❌ Pas de normalisation
- ❌ Données redondantes
- ❌ Schémas hétérogènes
- ❌ Valeurs nulles et incohérences
- ✅ Historique complet préservé
- ✅ Traçabilité totale

### Silver Layer - Star Schema

```
silver/
├── Dimensions/
│   ├── dim_etablissement/         (dimension établissements)
│   ├── dim_patient/               (dimension patients)
│   └── dim_temp/                  (dimension temporelle)
│
├── Faits/
│   ├── fact_consultation/         (fait consultations)
│   ├── fact_deces/                (fait décès)
│   └── fact_hospitalisation/      (fait hospitalisations)
│
└── Métriques/
    ├── metrique_activite_temporelle/
    ├── metrique_consultation/
    ├── metrique_deces_demographie/
    └── metrique_hospitalisation_etablissement/
```

**Caractéristiques Silver** :
- ✅ Schéma en étoile (star schema)
- ✅ Dimensions dédupliquées
- ✅ Faits normalisés
- ✅ Métriques pré-calculées
- ✅ Types de données optimisés
- ✅ Clés étrangères cohérentes

## 🔄 Transformations ETL (Bronze → Silver)

### 1. Nettoyage des données

```python
# Exemples de transformations
- Suppression des doublons
- Gestion des valeurs nulles
- Validation des formats (dates, IDs)
- Normalisation des chaînes (trim, lowercase)
- Conversion des types de données
```

### 2. Enrichissement

```python
# Ajouts de colonnes calculées
- Âge calculé depuis date de naissance
- Durée d'hospitalisation (date_sortie - date_entrée)
- Catégorisation (tranches d'âge, types de pathologies)
- Agrégations temporelles (année, mois, trimestre)
```

### 3. Jointures et dénormalisation contrôlée

```python
# Création des tables de faits
fact_consultation = (
    consultations
    .join(patients, "patient_id")
    .join(etablissements, "etablissement_id")
    .select(clés_étrangères + mesures)
)
```

### 4. Optimisations

```python
# Réduction de la taille
- int64 → int32 pour les IDs
- float64 → float32 pour les mesures
- String → Categorical pour les dimensions
- Partitionnement par date
```

## 📈 Comparaison des performances

### Bronze Layer - Résultats typiques

```
📊 Datasets analysés: 28
📏 Total de lignes: 7,435,042
💾 Taille totale: 2910.70 MB
⚡ Débit moyen: 1,280,073 lignes/seconde
⏱️ Temps moyen: 0.207s par dataset
🔥 Cache: -32.1% (problème de surcharge)
```

**Observations** :
- ⚠️ Cache inefficace (amélioration négative)
- ⚠️ Forte variabilité (CV = 171.8%)
- ⚠️ Datasets très hétérogènes en taille
- ✅ Débit global acceptable

### Silver Layer - Résultats typiques

```
📊 Datasets analysés: 10
📏 Total de lignes: ~1,771,000
💾 Taille totale: ~700 MB
⚡ Débit moyen: ~800,000 lignes/seconde
⏱️ Temps moyen: ~0.15s par dataset
🔥 Cache: ~15-25% (amélioration attendue)
```

**Observations** :
- ✅ Cache plus efficace (datasets homogènes)
- ✅ Meilleure stabilité (CV attendu < 100%)
- ✅ Taille réduite (compression + normalisation)
- ✅ Performances prévisibles

## 🎯 Cas d'usage

### Quand utiliser Bronze ?

1. **Audit et traçabilité**
   - Historique complet des données sources
   - Données brutes non modifiées
   - Conformité réglementaire

2. **Rechargement en cas d'erreur**
   - Possibilité de re-transformer
   - Récupération après incident
   - Debugging ETL

3. **Exploration exploratoire**
   - Analyse de données brutes
   - Découverte de patterns
   - Validation de qualité

### Quand utiliser Silver ?

1. **Analyses métier**
   - Requêtes analytiques (OLAP)
   - Reporting et dashboards
   - KPIs et métriques

2. **Machine Learning**
   - Features engineering
   - Entraînement de modèles
   - Prédictions

3. **Data Warehouse**
   - Requêtes performantes
   - Agrégations complexes
   - Cubes OLAP

## 📊 Graphiques comparatifs

### Temps de réponse

```
Bronze:  ████████████████░░░░ (max: 1.85s)
Silver:  ██████████░░░░░░░░░░ (max: 1.24s)

Bronze plus hétérogène, Silver plus prévisible
```

### Distribution des tailles

```
Bronze:
  Très petits (<1MB):     15 datasets
  Moyens (1-100MB):       8 datasets
  Gros (100-700MB):       5 datasets

Silver:
  Très petits (<1MB):     3 datasets
  Moyens (1-100MB):       5 datasets
  Gros (100-700MB):       2 datasets
```

### Débit (lignes/seconde)

```
Bronze:
  Top 1: prescriptions        2,883,867 r/s
  Top 2: salles               2,585,656 r/s
  Top 3: professionnels_pg    1,910,096 r/s

Silver:
  Top 1: fact_deces           1,380,253 r/s
  Top 2: fact_consultation      828,730 r/s
  Top 3: dim_patient            759,404 r/s
```

## 🔧 Optimisations recommandées

### Pour Bronze

1. **Partitionnement**
   ```python
   # Partitionner les grandes tables par date
   df.write.partitionBy("year", "month").parquet("bronze/hospitalisations")
   ```

2. **Compression adaptative**
   ```python
   # Snappy pour tables fréquemment lues
   # Gzip pour tables volumineuses rarement lues
   ```

3. **Cache sélectif**
   ```python
   # Ne cacher que les tables les plus utilisées
   spark.catalog.cacheTable("bronze.patients")
   ```

### Pour Silver

1. **Indexation**
   ```python
   # Créer des indexes sur les clés étrangères
   # Optimiser les jointures fréquentes
   ```

2. **Matérialisation des métriques**
   ```python
   # Pré-calculer les agrégations coûteuses
   # Rafraîchir périodiquement
   ```

3. **Partitionnement intelligent**
   ```python
   # Partitionner par dimensions business
   fact_consultation.write.partitionBy("annee", "region").parquet("silver/")
   ```

## 📚 Pipeline de données

```
Sources                Bronze                Silver                 Gold
═══════               ════════              ════════              ═══════

PostgreSQL  ────┐
                │
CSV Files   ────┼────>  Données     ────>  Star Schema   ────>  Agrégations
                │       Brutes             Normalisé             Métier
APIs        ────┘
                        
                        • 28 tables         • 10 tables           • Cubes OLAP
                        • 7.4M lignes       • 1.8M lignes         • Dashboards
                        • 2.9 GB            • 0.7 GB              • Optimisé
                        • Schéma source     • Schéma étoile       • Pré-agrégé
```

## 🎓 Bonnes pratiques

### Bronze Layer

```python
✅ DO:
- Préserver les données brutes intactes
- Ajouter des métadonnées (timestamp, source)
- Partitionner par date d'ingestion
- Documenter le schéma source

❌ DON'T:
- Modifier ou nettoyer les données
- Supprimer des colonnes
- Changer les types de données
- Joindre plusieurs sources
```

### Silver Layer

```python
✅ DO:
- Valider et nettoyer les données
- Normaliser le schéma (star/snowflake)
- Optimiser les types de données
- Créer des indexes et partitions
- Documenter les transformations

❌ DON'T:
- Faire des agrégations métier (→ Gold)
- Supprimer des données historiques
- Complexifier les jointures excessivement
```

## 🏥 Projet CHU - Contexte

### Bronze : Données sources médicales

- **Patients** : Données démographiques
- **Consultations** : Historique des consultations
- **Hospitalisations** : Séjours hospitaliers
- **Professionnels** : Médecins, infirmiers, spécialistes
- **Établissements** : Hôpitaux, cliniques, centres
- **Prescriptions** : Ordonnances et médicaments
- **Diagnostics** : CIM-10, pathologies
- **Données qualité** : IPAQSS, ESATIS

### Silver : Modèle analytique médical

**Dimensions** :
- `dim_patient` : Démographie patients
- `dim_etablissement` : Caractéristiques établissements
- `dim_temp` : Calendrier et périodes

**Faits** :
- `fact_consultation` : Événements de consultation
- `fact_hospitalisation` : Événements d'hospitalisation
- `fact_deces` : Événements de décès

**Métriques** :
- `metrique_activite_temporelle` : KPIs temporels
- `metrique_consultation` : Statistiques consultations
- `metrique_deces_demographie` : Analyse mortalité
- `metrique_hospitalisation_etablissement` : Performance établissements

---

**Version** : 1.0  
**Date** : Octobre 2025  
**Projet** : CHU - Big Data Healthcare Analytics
