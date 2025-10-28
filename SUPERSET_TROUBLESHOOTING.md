# 🔧 SOLUTION : Connexion Superset ↔ Trino

## ✅ Problèmes Résolus

### 1. "Can't load plugin: sqlalchemy.dialects:trino"
**Cause** : Package `sqlalchemy-trino` incompatible  
**Solution** : Installé `trino[sqlalchemy]` et redémarré Superset

### 2. Le schéma "default" n'est pas proposé
**Cause** : URI avec schema dans le path  
**Solution** : URI modifiée pour utiliser seulement le catalogue

---

## ✅ Configuration Actuelle (Automatique)

La connexion Trino est **déjà configurée et fonctionnelle** :

```
Database ID: 2
Nom: Trino CHU Gold
URI: trino://trino@chu_trino:8080/minio
Expose in SQL Lab: ✅ Oui
Allow CTAS: ✅ Oui
```

**Schémas disponibles** :
- ✅ `default` (contient les 5 tables KPI)
- ℹ️ `information_schema`

**Tables dans `default`** :
- ✅ `kpi_patient_demographics` (10 lignes)
- ✅ `kpi_etablissement_performance` (69 lignes)
- ✅ `kpi_temporal_trends` (4 lignes)
- ✅ `kpi_deces_by_region` (2 lignes)
- ✅ `kpi_satisfaction_global` (2 lignes)

---

## 🚀 Utilisation dans Superset

### Option 1 : Via SQL Lab (RECOMMANDÉ)

1. Allez sur **http://localhost:8088** (admin/admin123)
2. Cliquez sur **SQL** → **SQL Lab**
3. Sélectionnez :
   - **Database** : `Trino CHU Gold`
   - **Schema** : `default` (devrait apparaître maintenant ✅)
4. Les 5 tables KPI apparaissent dans le panneau de gauche

**Si le schéma "default" n'apparaît toujours pas** :
- Cliquez sur le bouton de rafraîchissement 🔄 à côté du menu déroulant Schema
- OU redémarrez votre navigateur et reconnectez-vous

### Option 2 : Configuration Manuelle (si nécessaire)

Si vous devez reconfigurer manuellement :

1. **Settings** → **Database Connections**
2. Cliquez sur `Trino CHU Gold`
3. **Onglet Basic** :
   - **SQLALCHEMY URI** : `trino://trino@chu_trino:8080/minio`
   - ⚠️ **PAS** de `/default` à la fin
4. **Onglet Advanced** :
   - **SQL Lab** : ✅ Expose database in SQL Lab
   - **Performance** : ✅ Allow CREATE TABLE AS
   - **Security** : ⬜ Allow DML (laisser décoché)
5. **Test Connection** → Devrait afficher "Connection looks good!"
6. **Finish** pour sauvegarder

### Option 3 : Requête SQL Directe (Sans sélection de schema)

Vous pouvez spécifier le schéma directement dans vos requêtes :

```sql
-- Notation complète : catalogue.schema.table
SELECT * FROM minio.default.kpi_patient_demographics 
ORDER BY nb_patients DESC;

-- OU sélectionner le schéma par défaut
USE minio.default;

-- Puis requêter sans préfixe
SELECT * FROM kpi_patient_demographics;
```

---

## 🧪 Test de Validation

### Test 1 : Connexion Python
```bash
docker exec chu_superset python3 -c "
from trino.dbapi import connect
conn = connect(host='chu_trino', port=8080, user='trino', catalog='minio')
cursor = conn.cursor()
cursor.execute('SHOW SCHEMAS')
print([row[0] for row in cursor.fetchall()])
"
```
**Résultat attendu** : `['default', 'information_schema']`

### Test 2 : Liste des tables
```bash
docker exec chu_superset python3 -c "
from trino.dbapi import connect
conn = connect(host='chu_trino', port=8080, user='trino', catalog='minio', schema='default')
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
tables = [row[0] for row in cursor.fetchall()]
print(f'✅ {len(tables)} tables trouvées: {tables}')
"
```
**Résultat attendu** : `✅ 5 tables trouvées: ['kpi_deces_by_region', ...]`

### Test 3 : Requête de données
```bash
docker exec chu_superset python3 -c "
from trino.dbapi import connect
conn = connect(host='chu_trino', port=8080, user='trino', catalog='minio', schema='default')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM kpi_patient_demographics')
count = cursor.fetchone()[0]
print(f'✅ {count} lignes dans kpi_patient_demographics')
"
```
**Résultat attendu** : `✅ 10 lignes dans kpi_patient_demographics`

---

## 📊 Requêtes Prêtes à l'Emploi

### Dans SQL Lab de Superset

**Requête 1 : Démographie patients**
```sql
SELECT 
    sexe,
    tranche_age,
    nb_patients,
    ROUND(100.0 * nb_patients / SUM(nb_patients) OVER(), 2) as pourcentage
FROM default.kpi_patient_demographics 
ORDER BY nb_patients DESC;
```

**Requête 2 : Tendances trimestrielles**
```sql
SELECT 
    CONCAT('Q', CAST(trimestre AS VARCHAR), ' 2019') as periode,
    FORMAT_NUMBER(volume, 0) as nb_deces
FROM default.kpi_temporal_trends
ORDER BY trimestre;
```

**Requête 3 : Performance établissements**
```sql
SELECT 
    type_etablissement,
    COUNT(*) as nb_etablissements,
    SUM(nb_consultations) as total_consultations,
    SUM(nb_hospitalisations) as total_hospitalisations
FROM default.kpi_etablissement_performance
GROUP BY type_etablissement
ORDER BY nb_etablissements DESC;
```

**Requête 4 : Vue d'ensemble complète**
```sql
SELECT 
    'Patients' as kpi,
    CAST(SUM(nb_patients) AS VARCHAR) as valeur
FROM default.kpi_patient_demographics

UNION ALL

SELECT 
    'Décès 2019',
    FORMAT_NUMBER(SUM(volume), 0)
FROM default.kpi_temporal_trends

UNION ALL

SELECT 
    'Établissements',
    CAST(COUNT(DISTINCT code_etablissement) AS VARCHAR)
FROM default.kpi_etablissement_performance;
```

---

## 🔄 En Cas de Problème

### Rafraîchir les métadonnées
```bash
# Via CLI
docker exec chu_superset superset refresh-metadata

# Ou redémarrer Superset
docker restart chu_superset
sleep 10
```

### Vérifier les logs
```bash
# Logs Superset
docker logs chu_superset --tail 50

# Logs Trino
docker logs chu_trino --tail 50
```

### Recréer la connexion (dernier recours)
```bash
# Supprimer l'ancienne connexion
docker exec chu_superset_db psql -U superset -d superset -c "
DELETE FROM query WHERE database_id = 2;
DELETE FROM dbs WHERE id = 2;
"

# Recréer
docker exec chu_superset superset set-database-uri \
  -d "Trino CHU Gold" \
  -u "trino://trino@chu_trino:8080/minio"

# Redémarrer
docker restart chu_superset
```

---

## ✅ Checklist de Validation

- [x] Driver `trino[sqlalchemy]` installé
- [x] Connexion "Trino CHU Gold" créée (ID: 2)
- [x] URI correcte : `trino://trino@chu_trino:8080/minio` (sans /default)
- [x] Expose in SQL Lab activé
- [x] Test connexion Python réussi
- [ ] Schéma "default" visible dans SQL Lab
- [ ] Tables KPI accessibles
- [ ] Requête SQL exécutée avec succès

---

## 🎯 Prochaines Étapes

1. **Ouvrir Superset** : http://localhost:8088 (admin/admin123)
2. **Aller dans SQL Lab** : SQL → SQL Lab
3. **Sélectionner** : Database "Trino CHU Gold" → Schema "default"
4. **Exécuter une requête test** (copier-coller Requête 1 ci-dessus)
5. **Créer votre premier chart** : Bouton "Create chart" après l'exécution
6. **Créer un dashboard** : Assembler vos charts

---

## 📚 Documentation

- **Guide Quickstart** : `/home/alban/BigData/BigData/SUPERSET_QUICKSTART.md`
- **Guide Connexion** : `/home/alban/BigData/BigData/SUPERSET_GUIDE_CONNEXION.md`
- **Pipeline Complet** : `/home/alban/BigData/BigData/PIPELINE_COMPLET.md`

---

**🎉 Configuration terminée ! Vous pouvez maintenant visualiser vos KPIs !**

Si le schéma "default" n'apparaît toujours pas dans le dropdown après rafraîchissement du navigateur, utilisez la notation complète dans vos requêtes : `default.kpi_patient_demographics`
