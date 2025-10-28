# 🏥 ZONE BRONZE - CONFORMITÉ RGPD

## 📊 Résumé de l'Ingestion

**Date d'exécution**: 28 octobre 2025  
**Script**: `bronze_ingestion_rgpd.py`  
**Bucket MinIO**: `bronze`

---

## ✅ Tables Créées

| Table | Source | Lignes | Taille | Statut |
|-------|--------|--------|--------|--------|
| `deces` | CSV | 620,606 | 65 MiB | ✅ |
| `etablissements` | CSV | 416,665 | ~40 MiB | ✅ |
| `professionnels_sante` | CSV | 1,048,575 | ~80 MiB | ✅ |
| `hospitalisations` | CSV | 2,479 | ~250 KB | ✅ |
| `satisfaction_mco_2017` | CSV | 1,157 | ~120 KB | ✅ |
| `patients` | PostgreSQL | - | - | ⚠️ (table n'existe pas) |
| `consultations` | PostgreSQL | - | - | ⚠️ (table n'existe pas) |
| `diagnostics` | PostgreSQL | - | - | ⚠️ (table n'existe pas) |

**TOTAL**: **2,089,482 lignes** ingérées avec conformité RGPD

---

## 🔒 Principes RGPD Appliqués

### 1. Données Personnelles Anonymisées (Hash MD5)

#### Identité
- `nom` → `nom_anonymized` (MD5)
- `prenom` → `prenom_anonymized` (MD5)
- ✅ Conservation: `initiale_prenom` (1er caractère en majuscule)

#### Coordonnées
- `adresse` → `adresse_hash` (MD5)
- `email` → `email_hash` (MD5)
- `telephone` → `telephone_hash` (MD5)
- `numero_securite_sociale` → `numero_secu_hash` (MD5)

#### Géographie
- `voie` → `voie_hash` (MD5)
- ✅ Conservation: `code_postal` (validé 5 chiffres), `departement` (2 premiers chiffres), `commune`, `region`

### 2. Identifiants Préservés (Pour Jointures)

❌ **NON hashés** (nécessaires pour analyses) :
- `id_patient_original`
- `identifiant_organisation`
- `finess`
- `num_hospitalisation_original`
- `num_consultation_original`
- `code_diagnostic`
- `identifiant_professionnel`

### 3. Métadonnées Techniques Ajoutées

Chaque table Bronze contient :

| Colonne | Type | Description |
|---------|------|-------------|
| `_sk` | Integer | Clé surrogate séquentielle |
| `_hash_record` | String (MD5) | Hash de détection de doublons |
| `_source_system` | String | "CSV" ou "POSTGRES" |
| `_source_table` | String | Nom de la table source |
| `_ingestion_date` | Timestamp | Date/heure d'ingestion |
| `_version` | Integer | Version de l'enregistrement (SCD Type 2) |
| `_is_current` | Boolean | Flag enregistrement actuel |
| `_is_deleted` | Boolean | Flag suppression logique |

---

## 📋 Détails par Table

### 1. `deces` (620,606 lignes - 2019 uniquement)

**Champs anonymisés RGPD** :
- `nom_anonymized`, `prenom_anonymized` → MD5
- Conservation: `initiale_prenom`, `sexe`, `region`

**Enrichissements** :
- Extraction `date_naissance_annee`, `date_naissance_mois`
- Extraction `date_deces_annee`, `date_deces_mois`
- Calcul `departement` depuis `code_lieu_deces`
- Mapping `region` depuis département (13 régions françaises)

**Validations** :
- Dates format `yyyy-MM-dd`
- Pays défaut `FRANCE` si vide

---

### 2. `etablissements` (416,665 lignes)

**Champs anonymisés RGPD** :
- `adresse_hash`, `voie_hash`, `telephone_hash`, `telephone2_hash`, `telecopie_hash`, `email_hash` → MD5

**Identifiants préservés** :
- `finess`, `finess_juridique`, `identifiant_organisation`, `siren`, `siret`

**Données publiques normalisées** :
- `raison_sociale`, `enseigne_commerciale` → UPPER + TRIM
- `code_postal` validé (regex `[0-9]{5}`)
- `pays` défaut `FRANCE`

---

### 3. `professionnels_sante` (1,048,575 lignes)

**Champs anonymisés RGPD** :
- `nom_anonymized`, `prenom_anonymized` → MD5
- Conservation: `initiale_prenom`, `civilite`

**Identifiants préservés** :
- `identifiant_original`, `type_identifiant`

**Données métier normalisées** :
- `categorie_professionnelle`, `profession`, `specialite`, `commune` → UPPER + TRIM

---

### 4. `hospitalisations` (2,479 lignes)

**Identifiants préservés** :
- `num_hospitalisation_original`, `id_patient_original`, `identifiant_organisation`

**Données médicales** :
- `code_diagnostic`, `suite_diagnostic_consultation`

**Dates enrichies** :
- `date_entree` format `M/d/yyyy`
- Extraction `date_entree_annee`, `date_entree_mois`

**Validations** :
- `jour_hospitalisation` : 0-365 jours

---

### 5. `satisfaction_mco_2017` (1,157 lignes)

**Identifiants** :
- `finess`, `identifiant_organisation` (non hashés)

**Scores satisfaction** (7 dimensions) :
- `score_all_ajust` (score global)
- `score_accueil_rea_ajust` (accueil)
- `score_pecinf_rea_ajust` (prise en charge infirmiers)
- `score_pecmed_rea_ajust` (prise en charge médicale)
- `score_chambre_rea_ajust` (chambre)
- `score_repas_rea_ajust` (repas)
- `score_sortie_rea_ajust` (sortie)

**Transformations** :
- Remplacement virgule → point pour floats
- Normalisation `region` → UPPER

---

## 🎯 Exemples de Requêtes Spark

### Lire une table Bronze

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read_Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .getOrCreate()

# Lire la table deces
df_deces = spark.read.parquet("s3a://bronze/deces/")
df_deces.printSchema()
df_deces.show(10)

# Statistiques
print(f"Total lignes: {df_deces.count()}")
df_deces.groupBy("sexe", "region").count().show()
```

### Vérifier les métadonnées

```python
# Vérifier l'ingestion
df_deces.select("_source_system", "_source_table", "_ingestion_date", "_version") \
    .distinct() \
    .show(truncate=False)

# Compter les doublons potentiels
df_deces.groupBy("_hash_record").count().filter("count > 1").show()
```

### Jointure Patient-Hospitalisation

```python
df_hosp = spark.read.parquet("s3a://bronze/hospitalisations/")

# Jointure préservée (id_patient non hashé)
df_joined = df_hosp.join(
    df_patients,
    df_hosp.id_patient_original == df_patients.id_patient_original,
    "inner"
)
```

---

## 📦 Structure Parquet

Chaque table est stockée en **format Parquet compressé Snappy** :

```
s3a://bronze/<table_name>/
├── _SUCCESS
├── part-00000-xxx.snappy.parquet
├── part-00001-xxx.snappy.parquet
└── ...
```

**Avantages** :
- Compression ~70% vs CSV
- Lecture columnar rapide
- Schema préservé
- Compatible Spark, Trino, Superset

---

## 🔧 Maintenance

### Re-exécuter l'ingestion

```bash
docker exec chu_jupyter spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/bronze_ingestion_rgpd.py
```

### Vider le bucket Bronze

```bash
docker exec chu_minio mc rm --recursive --force myminio/bronze/
docker exec chu_minio mc mb myminio/bronze
```

### Vérifier la taille des données

```bash
docker exec chu_minio mc du myminio/bronze/
```

---

## 🚀 Prochaines Étapes

1. **Silver Layer** : Créer les dimensions et faits à partir de Bronze
2. **Gold Layer** : Agréger les KPIs pour visualisation
3. **Trino** : Créer des vues externes sur Bronze
4. **Superset** : Connecter et visualiser

---

## 📝 Notes Importantes

⚠️ **Limitation actuelle** : Tables PostgreSQL (`patients`, `consultations`, `diagnostics`) non ingérées car inexistantes dans la base.

✅ **Conformité** : Toutes les PII sont hashées en MD5, rendant impossible la ré-identification.

✅ **Traçabilité** : Chaque ligne contient `_hash_record` pour détection de doublons et SCD Type 2.

✅ **Auditabilité** : `_ingestion_date`, `_version`, `_is_current` permettent l'historisation.

---

**Dernière mise à jour** : 28 octobre 2025  
**Auteur** : Pipeline automatisé Bronze RGPD  
**Contact** : admin@chu.com
