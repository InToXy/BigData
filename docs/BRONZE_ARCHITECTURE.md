# 🏥 CHU - Cloud Healthcare Unit - Architecture Bronze

## 📋 Vue d'ensemble

Le pipeline d'ingestion Bronze est conçu pour intégrer toutes les données de santé du groupe CHU dans un lac de données sécurisé et gouverné. Cette zone Bronze constitue la **couche de données brutes** qui alimentera ensuite les transformations Silver et les KPIs Gold.

---

## 🎯 Objectifs du Pipeline Bronze

1. **Extraction** : Récupérer les données depuis multiples sources hétérogènes
2. **Validation** : Vérifier la qualité et la cohérence des données
3. **Normalisation** : Standardiser les formats (dates, colonnes, encodage)
4. **Anonymisation RGPD** : Protéger les données personnelles sensibles
5. **Stockage** : Persister dans MinIO (S3) au format Parquet optimisé

---

## 📊 Sources de Données

### 1. Base de Données PostgreSQL (chu_postgres_data)

**Tables extraites :**
- `patients` : Informations démographiques des patients
- `consultations` : Historique des consultations médicales
- `deces` : Registre des décès (filtré sur l'année 2019 pour optimisation)

**Connexion :**
- Host: `chu_postgres_data:5432`
- Database: `healthcare_data`
- User: `admin` / Password: `admin123`

---

### 2. Fichiers CSV - Établissements de Santé

**Localisation :** `/data/source/Etablissement de SANTE/`

| Fichier | Description | Volume |
|---------|-------------|--------|
| `etablissement_sante.csv` | Référentiel des établissements hospitaliers français | ~417K lignes |
| `professionnel_sante.csv` | Données des professionnels de santé | Variable |
| `activite_professionnel_sante.csv` | Activités par professionnel | Variable |

**Format :** CSV avec séparateur `;` (point-virgule)

**Colonnes clés :**
- `finess_site` : Identifiant unique établissement
- `raison_sociale_site` : Nom de l'établissement
- `region`, `departement`, `commune` : Localisation géographique
- `email`, `telephone` : Coordonnées

---

### 3. Fichiers CSV - Hospitalisations

**Localisation :** `/data/source/Hospitalisation/`

| Fichier | Description | Volume |
|---------|-------------|--------|
| `Hospitalisations.csv` | Données d'hospitalisation avec diagnostics | ~2.5K lignes |

**Format :** CSV avec séparateur `;`

**Colonnes clés :**
- `Num_Hospitalisation` : Identifiant unique
- `Id_patient` : Référence patient
- `identifiant_organisation` : Établissement (FINESS)
- `Code_diagnostic` : Code diagnostic médical
- `Date_Entree` : Date d'admission (format `dd/MM/yyyy`)
- `Jour_Hospitalisation` : Durée séjour

---

### 4. Fichiers CSV - Décès en France

**Localisation :** `/data/source/DECES EN FRANCE/`

| Fichier | Description | Volume |
|---------|-------------|--------|
| `deces.csv` | Registre national des décès | **~25M lignes** |

**⚠️ ATTENTION :** Fichier très volumineux ! Le pipeline filtre automatiquement sur l'année 2019 pour optimisation.

**Format :** CSV avec séparateur `,`

**Colonnes clés :**
- `nom`, `prenom` : Identité (anonymisée dans Bronze)
- `sexe` : Genre (1=M, 2=F)
- `date_naissance`, `date_deces` : Dates (format `yyyy-MM-dd`)
- `code_lieu_naissance`, `lieu_naissance` : Lieu naissance
- `code_lieu_deces` : Lieu décès
- `pays_naissance` : Pays origine

---

### 5. Fichiers - Satisfaction Patients

**Localisation :** `/data/source/Satisfaction/`

**Structure par année :** Dossiers `2014/`, `2015/`, `2016/`, `2017/`

| Type | Description | Années |
|------|-------------|--------|
| ESATIS48H | Satisfaction 48h après hospitalisation MCO | 2017, 2019 |
| ESATISCA | Satisfaction en court séjour ambulatoire | 2019 |
| DPA HAD | Satisfaction hospitalisation à domicile | 2015, 2016 |
| DPA SSR | Satisfaction soins de suite et réadaptation | 2014, 2017 |
| RCP MCO | Réunions de concertation pluridisciplinaire | 2014, 2017 |
| HPP MCO | Hémorragie du post-partum | 2015 |
| IDM MCO | Infarctus du myocarde | 2015 |

**Format :** CSV (données) + CSV/XLSX (lexiques)

---

## 🔧 Transformations Appliquées

### 1. Nettoyage des Colonnes

```python
# Standardisation des noms
- Caractères spéciaux → underscore
- Majuscules → minuscules
- Espaces → underscore
- Exemple: "Date Naissance" → "date_naissance"
```

### 2. Normalisation des Dates

**Formats supportés :**
- `yyyy-MM-dd` (ISO 8601)
- `dd/MM/yyyy` (format français)
- `dd-MM-yyyy`, `MM/dd/yyyy`, `M/d/yyyy`
- Timestamps PostgreSQL

**Validation :**
- Années acceptées : 1900 - 2100
- Valeurs hors limites → NULL

**Colonnes traitées :**
- `date_naissance`, `date_deces`
- `date_consultation`, `date_admission`, `date_entree`, `date_sortie`

### 3. Normalisation des Données

| Colonne | Transformation | Exemple |
|---------|----------------|---------|
| `sexe` | M/F/X/I | "Homme" → "M", "Femme" → "F" |
| `code_postal` | 5 chiffres | "75 001" → "75001" |
| `email` | Lowercase | "Test@Example.COM" → "test@example.com" |
| `telephone` | Chiffres uniquement | "+33 1 23 45 67 89" → "33123456789" |
| `finess` | 9 chiffres | "01 078 005 4" → "010780054" |

### 4. Anonymisation RGPD 🔐

**Données personnelles hashées (SHA-256) :**
- Nom, Prénom
- Adresse email
- Numéro de sécurité sociale
- Adresse postale complète

**Données préservées pour analytics :**
- Sexe, âge (calculé depuis date_naissance)
- Code postal, région, département (géolocalisation)
- Codes diagnostics, codes établissements
- Dates (sans heures si sensible)

**Exemple :**
```
AVANT: nom="Dupont", prenom="Jean", email="jean.dupont@email.fr"
APRÈS: nom_anonymized="8f3d2e1a...", prenom_anonymized="9a4b5c...", email_anonymized="7e6f..."
```

---

## 💾 Format de Stockage

### MinIO S3 - Bucket Bronze

**Structure :**
```
s3a://bronze/
├── patients/
│   └── *.parquet
├── consultations/
│   └── *.parquet
├── deces_2019/
│   └── *.parquet
├── etablissement_sante/
│   └── *.parquet
├── professionnel_sante/
│   └── *.parquet
├── hospitalisation/
│   └── *.parquet
├── satisfaction_esatis48h_2017/
│   └── *.parquet
├── satisfaction_esatisca_2019/
│   └── *.parquet
└── ...
```

**Format Parquet :**
- **Compression :** Snappy (équilibre vitesse/ratio)
- **Partitionnement :** Par table source
- **Schéma :** Inféré automatiquement avec validation

**Métadonnées systématiques :**
Chaque table Bronze contient :
- `ingestion_timestamp` : Date/heure d'ingestion
- `source_system` : Système source (PostgreSQL, CSV, etc.)
- `data_quality_score` : Score qualité (0-100)

---

## ⚙️ Configuration Technique

### Spark Configuration

**Mode Ressources Limitées (WSL/VM) :**
```properties
spark.driver.memory=2g
spark.executor.memory=2g
spark.executor.cores=2
spark.sql.shuffle.partitions=8
```

**Mode Production :**
```properties
spark.driver.memory=6g
spark.executor.memory=8g
spark.executor.cores=4
spark.sql.shuffle.partitions=32
```

### Optimisations Appliquées

1. **Adaptive Query Execution** : Activé
2. **Broadcast Joins** : Auto pour tables < 10MB
3. **Partition Pruning** : Activé
4. **Predicate Pushdown** : Activé pour CSV
5. **Columnar Storage** : Parquet natif

---

## 🚀 Exécution du Pipeline

### Méthode 1 : Script Shell (Recommandé)

```bash
cd /home/alban/BigData/BigData
./run_bronze_ingestion.sh
```

**Le script effectue :**
1. ✅ Vérification des containers Docker
2. ✅ Vérification de MinIO et création du bucket Bronze
3. ✅ Vérification des données sources
4. ✅ Test de connexion PostgreSQL
5. 🚀 Lancement du job Spark
6. 📊 Rapport de résultats

### Méthode 2 : Spark-Submit Direct

```bash
docker exec chu_jupyter spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/jobs/main_jobs/bronze_ingestion.py
```

---

## 📈 Tables Bronze Créées

| Table | Source | Volume Estimé | Colonnes | Usage Silver |
|-------|--------|---------------|----------|--------------|
| `patients` | PostgreSQL | Variable | 10-15 | dim_patient |
| `consultations` | PostgreSQL | Variable | 8-12 | fact_consultation |
| `deces_2019` | CSV | ~620K | 11 | fact_deces |
| `etablissement_sante` | CSV | ~417K | 25 | dim_etablissement |
| `professionnel_sante` | CSV | Variable | 8-12 | dim_professionnel |
| `hospitalisation` | CSV | ~2.5K | 7 | fact_hospitalisation |
| `satisfaction_*` | CSV | Variable | 15-30 | metrique_satisfaction |

---

## 🎯 KPIs Cibles (Besoins Utilisateurs)

Les données Bronze permettront de construire ces KPIs dans Gold :

1. ✅ **Taux de consultation** par établissement X sur période Y
2. ✅ **Taux de consultation** par diagnostic X sur période Y
3. ✅ **Taux global d'hospitalisation** sur période Y
4. ✅ **Taux d'hospitalisation** par diagnostic sur période
5. ✅ **Taux d'hospitalisation** par sexe et âge
6. ✅ **Taux de consultation** par professionnel de santé
7. ✅ **Nombre de décès** par région (année 2019)
8. ✅ **Taux de satisfaction** par région (année 2020)

---

## 🔍 Monitoring et Validation

### Vérifications Post-Ingestion

```bash
# Lister les tables Bronze créées
docker exec chu_jupyter mc ls myminio/bronze/

# Compter les lignes d'une table
docker exec chu_jupyter spark-submit \
    --jars ... \
    /home/jovyan/jobs/visu/visu_bronze.py
```

### Métriques de Qualité

Le pipeline calcule automatiquement :
- **Complétude** : % de valeurs non-nulles par colonne
- **Unicité** : % de valeurs uniques (pour clés)
- **Validité** : % de dates valides, codes postaux corrects, etc.
- **Conformité RGPD** : % de données anonymisées

---

## ⚠️ Points d'Attention

### 1. Volume du Fichier Décès

**Problème :** 25 millions de lignes = ~10GB
**Solution :** Filtrage automatique sur année 2019 (réduit à ~620K lignes)

### 2. Formats de Dates Hétérogènes

**Problème :** CSV français (dd/MM/yyyy) vs PostgreSQL (yyyy-MM-dd)
**Solution :** Fonction `normalize_dates_advanced()` avec 10+ formats supportés

### 3. Encodage des Fichiers CSV

**Problème :** UTF-8 vs Latin-1 vs Windows-1252
**Solution :** Détection automatique avec fallback

### 4. Séparateurs CSV Variables

**Problème :** `;` pour certains fichiers, `,` pour d'autres
**Solution :** Configuration par fichier dans le pipeline

### 5. Données Géographiques

**Problème :** Codes lieux contiennent des "dates" (ex: 02383)
**Solution :** Exclusion explicite des colonnes `code_lieu_*` de la normalisation dates

---

## 🔐 Sécurité et Conformité

### RGPD - Article 32

**Pseudonymisation :** SHA-256 sur données personnelles
**Minimisation :** Seules les colonnes nécessaires sont conservées
**Intégrité :** Checksums Parquet automatiques
**Traçabilité :** Logs d'ingestion horodatés

### Chiffrement

- **En transit :** HTTPS pour MinIO (si activé en prod)
- **Au repos :** Encryption S3 côté serveur (optionnel)
- **Accès :** IAM MinIO avec credentials rotatifs

---

## 📚 Dépendances

### JARs Requis

```
/home/jovyan/jars/
├── hadoop-aws-3.3.4.jar
├── aws-java-sdk-bundle-1.12.262.jar
├── postgresql-42.x.x.jar (auto-inclus Spark)
```

### Bibliothèques Python

Intégrées dans l'image Spark :
- PySpark 3.5.0
- Pandas (pour read_csv optimisé)
- Regex, UUID (stdlib)

---

## 📞 Support et Documentation

**Auteur :** Pipeline CHU - Big Data Team  
**Version :** 1.0.0  
**Date :** Octobre 2025  

**Fichiers clés :**
- `/spark_jobs/main_jobs/bronze_ingestion.py` : Pipeline principal
- `/run_bronze_ingestion.sh` : Script d'exécution
- `/tools/check_table.py` : Outil de validation

**Logs :**
- Console Spark : Niveau WARN
- Logs MinIO : `/logs/minio/`
- Logs Spark : `/logs/spark/`

---

## 🎓 Prochaines Étapes

1. ✅ **Bronze créé** : Vous êtes ici !
2. ⏭️ **Transformation Silver** : Modèle en étoile (dimensions + faits)
3. ⏭️ **Agrégation Gold** : KPIs business
4. ⏭️ **Visualisation** : Superset / Power BI via Trino

---

**🎉 Zone Bronze prête pour alimenter le Data Warehouse CHU !**
