# 📊 Guide : Connecter Superset à Trino pour accéder aux KPIs

## 🎯 Objectif
Visualiser les 5 KPIs Gold (87 lignes de données agrégées) dans Superset via Trino.

---

## 🔧 Étape 1 : Installation du driver Trino dans Superset

Le conteneur Superset doit avoir le driver Python pour Trino.

```bash
# Se connecter au conteneur Superset
docker exec -it chu_superset bash

# Installer le driver Trino
pip install trino

# Vérifier l'installation
pip list | grep -i trino

# Sortir du conteneur
exit
```

**OU** redémarrer Superset avec le driver pré-installé :

```bash
docker restart chu_superset

# Attendre 10 secondes
sleep 10

# Vérifier que Superset est prêt
docker logs chu_superset | tail -5
```

---

## 🌐 Étape 2 : Accéder à l'interface Superset

1. Ouvrez votre navigateur : **http://localhost:8088**
2. Connectez-vous :
   - **Username** : `admin`
   - **Password** : `admin123`

---

## 🔌 Étape 3 : Ajouter la connexion Trino

### Option A : Via l'interface graphique (recommandé)

1. Dans Superset, cliquez sur **Settings** (⚙️ en haut à droite) → **Database Connections**
2. Cliquez sur **+ Database** (bouton bleu en haut à droite)
3. Sélectionnez **Trino** dans la liste des connecteurs

4. **Configuration de base** :
   - **Display Name** : `Trino CHU Gold`
   - **SQLAlchemy URI** :
     ```
     trino://chu_trino:8090/minio/default
     ```

5. **Configuration avancée** (onglet "Advanced") :
   - **Expose database in SQL Lab** : ✅ Coché
   - **Allow CREATE TABLE AS** : ✅ Coché (optionnel)
   - **Allow DML** : ⬜ Décoché (sécurité)

6. Cliquez sur **Test Connection**
   - ✅ Si succès : "Connection looks good!"
   - ❌ Si échec : voir section Troubleshooting ci-dessous

7. Cliquez sur **Connect** pour finaliser

### Option B : Via SQL (alternative)

Si l'interface ne fonctionne pas, utilisez l'API :

```bash
docker exec chu_superset superset db upgrade

docker exec chu_superset bash -c "cat > /tmp/add_trino.py << 'EOF'
from superset import db
from superset.models.core import Database

# Créer la connexion Trino
trino_db = Database(
    database_name='Trino CHU Gold',
    sqlalchemy_uri='trino://chu_trino:8090/minio/default',
    expose_in_sqllab=True,
    allow_ctas=True,
    allow_dml=False
)

db.session.add(trino_db)
db.session.commit()
print('✅ Connexion Trino ajoutée avec succès')
EOF
"

docker exec chu_superset superset fab create-db-connection \
  --database_name "Trino CHU Gold" \
  --sqlalchemy_uri "trino://chu_trino:8090/minio/default"
```

---

## 📊 Étape 4 : Vérifier la connexion et explorer les données

### Dans SQL Lab

1. Allez dans **SQL** → **SQL Lab**
2. Sélectionnez :
   - **Database** : `Trino CHU Gold`
   - **Schema** : `default`
3. Les 5 tables KPI doivent apparaître dans le panneau de gauche :
   - ✅ `kpi_patient_demographics`
   - ✅ `kpi_etablissement_performance`
   - ✅ `kpi_temporal_trends`
   - ✅ `kpi_deces_by_region`
   - ✅ `kpi_satisfaction_global`

### Requête de test

```sql
-- Test 1 : Démographie patients
SELECT * FROM kpi_patient_demographics 
ORDER BY nb_patients DESC
LIMIT 10;
```

Résultat attendu : 10 lignes avec sexe, tranche_age, nb_patients

```sql
-- Test 2 : Tendances temporelles
SELECT 
    annee,
    trimestre,
    type_activite,
    volume
FROM kpi_temporal_trends
ORDER BY trimestre;
```

Résultat attendu : 4 lignes (trimestres 2019)

---

## 📈 Étape 5 : Créer vos premières visualisations

### Visualisation 1 : Démographie des patients (Barres empilées)

1. Allez dans **Charts** → **+ Chart**
2. Configuration :
   - **Dataset** : Sélectionner `kpi_patient_demographics`
   - **Chart Type** : `Bar Chart` (barres horizontales)
3. Paramètres :
   - **Query** :
     - **Metrics** : `SUM(nb_patients)`
     - **Group by** : `tranche_age`
     - **Breakdown Dimensions** : `sexe`
   - **Customize** :
     - **Color Scheme** : `supersetColors`
     - **Show Legend** : ✅
4. Cliquez sur **Update Chart**
5. Cliquez sur **Save** → Nom : `Démographie Patients par Âge et Sexe`

### Visualisation 2 : Tendances temporelles (Ligne)

1. **Charts** → **+ Chart**
2. Configuration :
   - **Dataset** : `kpi_temporal_trends`
   - **Chart Type** : `Line Chart`
3. Paramètres :
   - **Query** :
     - **Metrics** : `SUM(volume)`
     - **Dimensions** : `trimestre`
     - **Series** : `type_activite`
   - **Customize** :
     - **X Axis Label** : `Trimestre 2019`
     - **Y Axis Label** : `Nombre de décès`
4. **Save** → Nom : `Évolution Décès 2019`

### Visualisation 3 : Performance établissements (Table)

1. **Charts** → **+ Chart**
2. Configuration :
   - **Dataset** : `kpi_etablissement_performance`
   - **Chart Type** : `Table`
3. Paramètres :
   - **Query** :
     - **Columns** : `type_etablissement`, `nb_consultations`, `nb_hospitalisations`
     - **Metrics** : `COUNT(*)`
   - **Group by** : `type_etablissement`
4. **Save** → Nom : `Performance par Type Établissement`

### Visualisation 4 : Décès par région (Camembert)

1. **Charts** → **+ Chart**
2. Configuration :
   - **Dataset** : `kpi_deces_by_region`
   - **Chart Type** : `Pie Chart`
3. Paramètres :
   - **Metrics** : `SUM(total_deces)`
   - **Dimensions** : `region`
4. **Save** → Nom : `Répartition Décès par Région`

---

## 📋 Étape 6 : Créer un Dashboard

1. Allez dans **Dashboards** → **+ Dashboard**
2. Nommez-le : `KPIs CHU Data Warehouse`
3. Cliquez sur **Edit Dashboard**
4. Ajoutez vos charts :
   - Glissez-déposez depuis la liste de gauche
   - Organisez-les en grille (drag & drop)
5. Ajoutez des **Markdown** pour les titres de sections :
   ```markdown
   ## 📊 Analyse Démographique
   Répartition des patients par sexe et tranche d'âge
   ```
6. **Save** le dashboard

---

## 🔍 Étape 7 : Requêtes SQL avancées dans SQL Lab

### Top 5 tranches d'âge
```sql
SELECT 
    tranche_age,
    sexe,
    nb_patients,
    ROUND(100.0 * nb_patients / SUM(nb_patients) OVER(), 2) as pourcentage
FROM kpi_patient_demographics
ORDER BY nb_patients DESC
LIMIT 5;
```

### Variation trimestrielle décès
```sql
SELECT 
    trimestre,
    volume,
    volume - LAG(volume) OVER (ORDER BY trimestre) as variation,
    ROUND(100.0 * (volume - LAG(volume) OVER (ORDER BY trimestre)) / LAG(volume) OVER (ORDER BY trimestre), 2) as variation_pct
FROM kpi_temporal_trends
ORDER BY trimestre;
```

### Performance établissements (agrégation)
```sql
SELECT 
    type_etablissement,
    COUNT(*) as nb_etablissements,
    SUM(nb_consultations) as total_consultations,
    SUM(nb_hospitalisations) as total_hospitalisations
FROM kpi_etablissement_performance
GROUP BY type_etablissement
ORDER BY total_consultations DESC;
```

---

## 🐛 Troubleshooting

### Erreur : "Could not load database driver: trino"

**Cause** : Driver Trino non installé dans Superset

**Solution** :
```bash
docker exec chu_superset pip install trino sqlalchemy-trino
docker restart chu_superset
```

### Erreur : "Connection timeout"

**Cause** : Superset ne peut pas joindre le conteneur Trino

**Vérification** :
```bash
# Vérifier que Trino est accessible
docker exec chu_superset ping -c 2 chu_trino

# Vérifier que le port 8090 est ouvert
docker exec chu_superset nc -zv chu_trino 8090
```

**Solution** :
- Vérifier que les deux conteneurs sont sur le même réseau Docker : `bigdata_network`
```bash
docker inspect chu_superset | grep NetworkMode
docker inspect chu_trino | grep NetworkMode
```

### Erreur : "Schema 'default' does not exist"

**Cause** : Le schéma n'est pas créé dans Trino

**Solution** :
```bash
docker exec chu_trino trino --catalog minio --execute "
CREATE SCHEMA IF NOT EXISTS default
WITH (location = 's3a://gold/');
"
```

### Les tables n'apparaissent pas dans SQL Lab

**Cause** : Cache Superset obsolète

**Solution** :
1. Dans Superset : **Data** → **Databases**
2. Cliquez sur `Trino CHU Gold`
3. Onglet **Advanced** → Cliquez sur **Refresh Metadata**
4. Ou en CLI :
```bash
docker exec chu_superset superset refresh-metadata
```

### Erreur : "Authentication failed"

**Cause** : Trino configuré avec authentification

**Vérification** :
```bash
docker exec chu_trino cat /etc/trino/config.properties | grep authentication
```

**Solution** : Normalement Trino n'a pas d'auth, mais si configuré :
- URI : `trino://username@chu_trino:8090/minio/default`

---

## 📊 Datasets disponibles (87 lignes au total)

| Table | Lignes | Colonnes | Description |
|-------|--------|----------|-------------|
| `kpi_patient_demographics` | 10 | 3 | Sexe, tranche_age, nb_patients |
| `kpi_etablissement_performance` | 69 | 4 | Code, type, consultations, hospitalisations |
| `kpi_temporal_trends` | 4 | 4 | Année, trimestre, type_activite, volume |
| `kpi_deces_by_region` | 2 | 3 | Région, total_deces, age_moyen |
| `kpi_satisfaction_global` | 2 | 3 | Année, score_moyen, nb_reponses |

---

## ✅ Checklist de validation

- [ ] Driver Trino installé dans Superset
- [ ] Connexion "Trino CHU Gold" créée
- [ ] Test de connexion réussi (bouton "Test Connection")
- [ ] Les 5 tables KPI visibles dans SQL Lab
- [ ] Requête SQL de test exécutée avec succès
- [ ] Au moins 1 chart créé
- [ ] Au moins 1 dashboard créé

---

## 🎯 Exemple de Dashboard complet

Layout suggéré (4 sections) :

```
┌─────────────────────────────────────────────────┐
│  📊 KPIs CHU Data Warehouse - 2019              │
├──────────────────┬──────────────────────────────┤
│                  │                              │
│  Démographie     │   Performance                │
│  (Bar Chart)     │   Établissements (Table)     │
│                  │                              │
├──────────────────┴──────────────────────────────┤
│                                                  │
│  Tendances Temporelles Décès (Line Chart)       │
│                                                  │
├──────────────────┬──────────────────────────────┤
│                  │                              │
│  Décès Région    │   Satisfaction               │
│  (Pie Chart)     │   (Big Number)               │
│                  │                              │
└──────────────────┴──────────────────────────────┘
```

---

## 📚 Ressources supplémentaires

- **Documentation Superset** : https://superset.apache.org/docs/intro
- **Trino SQL** : https://trino.io/docs/current/sql.html
- **Votre fichier** : `/home/alban/BigData/BigData/PIPELINE_COMPLET.md`

---

**🎉 Vous êtes prêt à visualiser vos KPIs !**

Si vous rencontrez des problèmes, consultez la section Troubleshooting ou vérifiez les logs :
```bash
docker logs chu_superset | tail -50
docker logs chu_trino | tail -50
```
