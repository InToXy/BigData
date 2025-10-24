# 🚀 TRINO - QUERY ENGINE POUR POWER BI

**Version:** Trino 435  
**Port:** 8090  
**Status:** ✅ Production Ready

---

## 📖 QU'EST-CE QUE TRINO ?

Trino (anciennement Presto SQL) est un moteur de requêtes SQL distribué ultra-rapide qui permet d'interroger vos données, où qu'elles soient :

- ✅ **MinIO/S3** (vos buckets bronze, silver, gold)
- ✅ **PostgreSQL** (votre base de données source)
- ✅ **Delta Lake** (format optimisé)
- ✅ **Et 60+ autres sources de données**

**Avantage principal pour PowerBI :** Une connexion SQL standard vers toutes vos données !

---

## 🏗️ ARCHITECTURE

```
┌─────────────────────────────────────────────────────────┐
│                       POWER BI                          │
│                    (via ODBC/JDBC)                      │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓ Port 8090
┌─────────────────────────────────────────────────────────┐
│                     TRINO SERVER                        │
│                  (Query Coordinator)                    │
├─────────────────────────────────────────────────────────┤
│  Catalogues:                                            │
│  ├─ minio (Hive) ────────→ MinIO S3                    │
│  ├─ postgresql ──────────→ PostgreSQL                  │
│  └─ deltalake ───────────→ Delta Lake                  │
└─────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
   ┌─────────┐      ┌─────────┐     ┌──────────┐
   │  MinIO  │      │PostgreSQL│     │Delta Lake│
   │ Buckets │      │   DB     │     │ Tables   │
   └─────────┘      └─────────┘     └──────────┘
```

---

## 🚀 DÉMARRAGE RAPIDE

### 1. Démarrer Trino

```bash
cd /home/alban/BigData/BigData

# Démarrer tous les services (y compris Trino)
docker-compose up -d

# Vérifier que Trino est démarré
docker ps | grep chu_trino

# Voir les logs
docker logs -f chu_trino
```

### 2. Initialiser les Tables Gold

```bash
# Attendre 30 secondes que Trino soit prêt
sleep 30

# Exécuter le script d'initialisation
./trino/init_trino_tables.sh
```

### 3. Tester la Connexion

```bash
# Script de test automatisé
./trino/test_trino_connection.sh

# Ou manuellement via CLI
docker exec -it chu_trino trino --server localhost:8080
```

### 4. Accéder à l'Interface Web

```
http://localhost:8090/ui
```

---

## 📊 UTILISATION

### Commandes CLI de Base

```bash
# Se connecter à Trino
docker exec -it chu_trino trino --server localhost:8080

# Une fois connecté:
SHOW CATALOGS;
SHOW SCHEMAS FROM minio;
USE minio.gold;
SHOW TABLES;
SELECT * FROM kpi_taux_hospitalisation_global;
```

### Requêtes SQL Exemples

```sql
-- Lister tous les catalogues
SHOW CATALOGS;

-- Voir les schémas dans MinIO
SHOW SCHEMAS FROM minio;

-- Utiliser le schéma gold
USE minio.gold;

-- Lister les tables
SHOW TABLES;

-- Interroger une table
SELECT * FROM kpi_taux_hospitalisation_global;

-- Top 10 diagnostics
SELECT 
    diagnostic_principal,
    nb_hospitalisations
FROM kpi_hospitalisation_par_diagnostic
ORDER BY nb_hospitalisations DESC
LIMIT 10;

-- Jointure entre catalogues (MinIO + PostgreSQL)
SELECT 
    g.diagnostic_principal,
    g.nb_patients_hospitalises,
    p.nom_etablissement
FROM minio.gold.kpi_hospitalisation_par_diagnostic g
LEFT JOIN postgresql.public.etablissement_sante p
    ON g.etablissement_id = p.id
LIMIT 100;
```

---

## 🔌 CONNEXION DEPUIS POWER BI

**Guide complet :** Voir `POWERBI_CONNECTION_GUIDE.md`

**Résumé rapide :**

1. **Installer le driver ODBC Trino** (Simba ou Starburst)
2. **Configurer une source ODBC :**
   - Hôte: `localhost`
   - Port: `8090`
   - Catalogue: `minio`
   - Schéma: `gold`
3. **Dans Power BI :**
   - Obtenir des données → ODBC
   - Sélectionner votre source configurée
   - Choisir les tables Gold

---

## 📁 STRUCTURE DES FICHIERS

```
trino/
├── README.md                        # Ce fichier
├── POWERBI_CONNECTION_GUIDE.md      # Guide détaillé PowerBI
│
├── etc/                             # Configuration Trino
│   ├── config.properties            # Config serveur
│   ├── jvm.config                   # Config mémoire JVM
│   ├── node.properties              # Config nœud
│   └── log.properties               # Config logs
│
├── catalog/                         # Connecteurs de données
│   ├── minio.properties             # Connexion MinIO/S3
│   ├── postgresql.properties        # Connexion PostgreSQL
│   └── deltalake.properties         # Connexion Delta Lake
│
├── init_trino_tables.sh             # Init tables Gold
└── test_trino_connection.sh         # Tests de connexion
```

---

## 🎯 CATALOGUES CONFIGURÉS

### 1. **minio** (Hive Connector)

**Accès à :**
- Bucket `bronze/` (données brutes)
- Bucket `silver/` (données nettoyées)
- Bucket `gold/` (KPIs) ⭐

**Utilisation :**
```sql
USE minio.gold;
SELECT * FROM kpi_taux_hospitalisation_global;
```

### 2. **postgresql** (PostgreSQL Connector)

**Accès à :**
- Base de données source `healthcare_data`
- Toutes les tables PostgreSQL originales

**Utilisation :**
```sql
USE postgresql.public;
SELECT * FROM etablissement_sante LIMIT 10;
```

### 3. **deltalake** (Delta Lake Connector)

**Accès à :**
- Bucket `gold-delta/` (Delta Lake optimisé)
- Tables avec versioning et ACID

**Utilisation :**
```sql
USE deltalake.default;
SHOW TABLES;
```

---

## 🔧 CONFIGURATION AVANCÉE

### Modification de la Mémoire

Éditer `trino/etc/jvm.config` :

```
-Xmx4G    # Mémoire max (augmenter si besoin)
-Xms2G    # Mémoire initiale
```

Puis redémarrer :
```bash
docker-compose restart trino
```

### Ajout d'un Nouveau Catalogue

1. Créer un fichier dans `trino/catalog/`
2. Exemple pour MySQL :

```properties
# trino/catalog/mysql.properties
connector.name=mysql
connection-url=jdbc:mysql://mysql_host:3306
connection-user=user
connection-password=password
```

3. Redémarrer Trino :
```bash
docker-compose restart trino
```

---

## 📈 PERFORMANCES

### Optimisations Activées

- ✅ **Adaptive Query Execution**
- ✅ **Predicate Pushdown** (filtrage côté source)
- ✅ **Column Pruning** (lecture colonnes nécessaires uniquement)
- ✅ **Query Parallelization**
- ✅ **S3 Connection Pooling** (100 connexions)

### Métriques

```sql
-- Voir les requêtes actives
SELECT * FROM system.runtime.queries;

-- Statistiques d'une requête
SELECT * FROM system.runtime.tasks WHERE query_id = 'xxx';
```

---

## 🐛 DÉPANNAGE

### Problème : Trino ne démarre pas

```bash
# Voir les logs
docker logs chu_trino

# Redémarrer
docker-compose restart trino

# Recréer le conteneur
docker-compose down
docker-compose up -d trino
```

### Problème : Catalogue 'minio' introuvable

```bash
# Vérifier la config
cat trino/catalog/minio.properties

# Vérifier que MinIO est accessible
curl http://localhost:9000

# Redémarrer Trino
docker-compose restart trino
```

### Problème : Table non trouvée

```bash
# Réinitialiser les tables
./trino/init_trino_tables.sh

# Ou manuellement
docker exec -it chu_trino trino --server localhost:8080
CREATE SCHEMA IF NOT EXISTS minio.gold WITH (location = 's3a://gold/');
```

### Problème : Erreur de connexion PowerBI

1. Vérifier que Trino est accessible : `http://localhost:8090/ui`
2. Tester avec le CLI : `docker exec -it chu_trino trino`
3. Vérifier le driver ODBC installé (64-bit)
4. Vérifier la configuration ODBC dans Windows

---

## 📊 MONITORING

### Interface Web

```
http://localhost:8090/ui
```

**Fonctionnalités :**
- 📈 Requêtes en cours
- 📊 Historique des requêtes
- 🔍 Détails d'exécution (plan de requête)
- 📉 Métriques de performance

### API REST

```bash
# Informations serveur
curl http://localhost:8090/v1/info | jq

# Liste des requêtes
curl http://localhost:8090/v1/query | jq

# Statistiques
curl http://localhost:8090/v1/stats | jq
```

---

## ✅ CHECKLIST DE PRODUCTION

```
☐ Services Docker démarrés (docker-compose up -d)
☐ Trino accessible (http://localhost:8090/ui)
☐ Tables Gold initialisées (init_trino_tables.sh)
☐ Tests de connexion réussis (test_trino_connection.sh)
☐ Driver ODBC installé (Windows)
☐ Source ODBC configurée
☐ Power BI connecté avec succès
☐ Dashboards créés et testés
```

---

## 🔗 RESSOURCES

### Documentation Officielle

- **Trino Docs:** https://trino.io/docs/current/
- **Hive Connector:** https://trino.io/docs/current/connector/hive.html
- **PostgreSQL Connector:** https://trino.io/docs/current/connector/postgresql.html
- **Delta Lake Connector:** https://trino.io/docs/current/connector/delta-lake.html

### Support

- **Logs:** `docker logs chu_trino`
- **Web UI:** http://localhost:8090/ui
- **CLI Test:** `docker exec -it chu_trino trino`

---

## 🎉 RÉSUMÉ

**Trino vous permet de :**

✅ Connecter Power BI à vos données Gold (MinIO/S3)  
✅ Requêter PostgreSQL et MinIO dans la même requête SQL  
✅ Utiliser un langage SQL standard (ANSI SQL)  
✅ Bénéficier de performances optimisées  
✅ Éviter de dupliquer les données  

**Prêt pour Power BI en 3 étapes :**
1. `docker-compose up -d trino`
2. `./trino/init_trino_tables.sh`
3. Connecter Power BI via ODBC

---

**Date de création:** 24 Octobre 2025  
**Version:** 1.0  
**Status:** ✅ Production Ready
