# Utilitaires Spark Jobs

## Initialisation automatique du Hive Metastore

### Description

Ce module permet d'initialiser automatiquement les schémas (databases) dans le Hive Metastore lors de l'exécution des jobs Spark. Les schémas sont créés avec les bonnes locations S3 et sont automatiquement disponibles dans Trino.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Spark Job (Bronze/Silver/Gold)                             │
│  ├─ get_spark_session()                                     │
│  ├─ initialize_for_layer(spark, "bronze")  ◄─── AUTO       │
│  │   └─ CREATE DATABASE IF NOT EXISTS bronze               │
│  │      LOCATION 's3a://bronze/'                            │
│  └─ Traitement des données...                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Hive Metastore (MariaDB)                                   │
│  ├─ bronze → s3a://bronze/                                  │
│  ├─ silver → s3a://silver/                                  │
│  ├─ gold   → s3a://gold/                                    │
│  └─ warehouse → s3a://warehouse/                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Trino                                                       │
│  SHOW SCHEMAS FROM minio;                                   │
│  → bronze, silver, gold, warehouse ✅                        │
└─────────────────────────────────────────────────────────────┘
```

### Utilisation

#### Dans les jobs Spark

Les jobs Bronze, Silver et Gold ont été automatiquement configurés pour initialiser leurs schémas respectifs :

```python
from pyspark.sql import SparkSession
sys.path.insert(0, '/home/jovyan/jobs/utils')
from metastore_init import initialize_for_layer

def main():
    spark = get_spark_session()

    # ✅ Initialisation automatique du schéma
    initialize_for_layer(spark, "bronze")

    # Le schéma est maintenant disponible dans le metastore
    # et visible dans Trino !
```

#### Initialisation manuelle de tous les schémas

```python
from metastore_init import initialize_metastore_schemas

spark = get_spark_session()
initialize_metastore_schemas(spark)  # Crée bronze, silver, gold, warehouse
```

#### Initialisation d'un schéma spécifique

```python
from metastore_init import initialize_for_layer

spark = get_spark_session()
initialize_for_layer(spark, "silver")  # Crée uniquement silver
```

### Configuration

Les schémas et leurs configurations sont définis dans `metastore_init.py` :

```python
SCHEMAS = {
    "bronze": {
        "bucket": "bronze",
        "description": "Raw data ingestion layer"
    },
    "silver": {
        "bucket": "silver",
        "description": "Cleaned and validated data layer"
    },
    "gold": {
        "bucket": "gold",
        "description": "Business-ready data layer / Data Marts"
    },
    "warehouse": {
        "bucket": "warehouse",
        "description": "General purpose warehouse"
    }
}
```

### Fonctions disponibles

#### `initialize_for_layer(spark, layer)`
Initialise le schéma pour une couche spécifique (bronze, silver ou gold).

**Paramètres:**
- `spark`: SparkSession active
- `layer`: Nom de la couche ("bronze", "silver", "gold", "warehouse")

**Retour:** `True` si succès, `False` sinon

**Exemple:**
```python
initialize_for_layer(spark, "bronze")
```

---

#### `initialize_metastore_schemas(spark, schemas_to_create=None)`
Initialise plusieurs schémas d'un coup.

**Paramètres:**
- `spark`: SparkSession active
- `schemas_to_create`: Liste des schémas à créer (défaut: tous)

**Retour:** `True` si tous les schémas ont été créés, `False` sinon

**Exemple:**
```python
# Tous les schémas
initialize_metastore_schemas(spark)

# Seulement bronze et silver
initialize_metastore_schemas(spark, ["bronze", "silver"])
```

---

#### `verify_schema_exists(spark, schema_name)`
Vérifie qu'un schéma existe dans le metastore.

**Paramètres:**
- `spark`: SparkSession active
- `schema_name`: Nom du schéma à vérifier

**Retour:** `True` si le schéma existe, `False` sinon

**Exemple:**
```python
if verify_schema_exists(spark, "bronze"):
    print("Le schéma bronze existe !")
```

---

#### `create_schema_in_metastore(spark, schema_name, bucket)`
Crée un schéma dans le metastore avec une location S3 spécifique.

**Paramètres:**
- `spark`: SparkSession active
- `schema_name`: Nom du schéma à créer
- `bucket`: Nom du bucket S3/MinIO

**Retour:** `True` si succès, `False` sinon

**Exemple:**
```python
create_schema_in_metastore(spark, "bronze", "bronze")
```

### Tests

Un script de test complet est disponible pour valider le fonctionnement :

```bash
# Depuis Jupyter
python /home/jovyan/jobs/utils/test_metastore_init.py
```

Le script de test vérifie :
1. ✅ Initialisation de tous les schémas
2. ✅ Initialisation d'un seul schéma (bronze)
3. ✅ Vérification de l'existence des schémas
4. ✅ Liste de tous les schémas dans le metastore

### Vérification dans Trino

Après l'exécution d'un job, vous pouvez vérifier que les schémas sont bien créés :

```bash
docker exec chu_trino trino --execute "SHOW SCHEMAS FROM minio;"
```

Résultat attendu :
```
"bronze"
"default"
"gold"
"information_schema"
"silver"
"warehouse"
```

### Troubleshooting

#### Le schéma n'est pas créé

**Vérification 1:** Le metastore est-il accessible ?
```python
spark.sql("SHOW DATABASES").show()
```

**Vérification 2:** Les credentials MinIO sont-elles correctes ?
```python
spark.conf.get("spark.hadoop.fs.s3a.access.key")  # doit être "minioadmin"
spark.conf.get("spark.hadoop.fs.s3a.secret.key")  # doit être "minioadmin123"
```

**Vérification 3:** Le bucket MinIO existe-t-il ?
```bash
docker exec chu_minio mc ls myminio/
```

#### Erreur "Database already exists"

C'est normal ! La fonction utilise `CREATE DATABASE IF NOT EXISTS`, donc si le schéma existe déjà, il ne sera pas recréé et aucune erreur ne sera levée.

### Avantages

✅ **Automatique** : Plus besoin de créer manuellement les schémas dans Trino
✅ **Idempotent** : Peut être exécuté plusieurs fois sans erreur
✅ **Centralisé** : Une seule configuration pour tous les jobs
✅ **Persistant** : Les schémas sont stockés dans le metastore (MariaDB)
✅ **Compatible** : Fonctionne avec Spark, Trino et tous les outils Hive-compatibles

### Jobs concernés

Les jobs suivants ont été modifiés pour utiliser cette fonctionnalité :

- ✅ `/home/jovyan/jobs/main_jobs/bronze_ingestion.py` → Initialise `bronze`
- ✅ `/home/jovyan/jobs/main_jobs/silver_transformation.py` → Initialise `silver`
- ✅ `/home/jovyan/jobs/main_jobs/gold_star_schema.py` → Initialise `gold`

### Contribution

Pour ajouter un nouveau schéma, modifier le dictionnaire `SCHEMAS` dans `metastore_init.py` :

```python
SCHEMAS = {
    # ... schémas existants ...
    "mon_nouveau_schema": {
        "bucket": "mon-bucket",
        "description": "Description de mon schéma"
    }
}
```

Puis l'utiliser :
```python
initialize_for_layer(spark, "mon_nouveau_schema")
```
