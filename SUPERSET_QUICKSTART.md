# 🚀 GUIDE RAPIDE : Récupérer les KPIs dans Superset

## ✅ Étape 1 : Connexion Trino créée !

La connexion à Trino est **déjà configurée** dans Superset :
- **Nom** : `Trino CHU Gold`
- **URI** : `trino://trino@chu_trino:8080/minio/default`
- **Catalogue** : `minio`
- **Schéma** : `default`

---

## 🌐 Étape 2 : Accéder à Superset

1. Ouvrez votre navigateur : **http://localhost:8088**
2. Connectez-vous :
   - Username : `admin`
   - Password : `admin123`

---

## 📊 Étape 3 : Explorer les KPIs dans SQL Lab

### Accéder au SQL Lab
1. Cliquez sur **SQL** (en haut) → **SQL Lab**
2. Sélectionnez dans les dropdowns :
   - **Database** : `Trino CHU Gold`
   - **Schema** : `default`
3. Les 5 tables KPI apparaissent dans le panneau de gauche :
   - 📊 `kpi_patient_demographics` (10 lignes)
   - 🏥 `kpi_etablissement_performance` (69 lignes)
   - 📈 `kpi_temporal_trends` (4 lignes)
   - ⚰️ `kpi_deces_by_region` (2 lignes)
   - 😊 `kpi_satisfaction_global` (2 lignes)

### Requête test 1 : Démographie patients
```sql
SELECT 
    sexe,
    tranche_age,
    nb_patients
FROM kpi_patient_demographics 
ORDER BY nb_patients DESC;
```

**Résultat attendu** : 10 lignes montrant que les Femmes 75+ sont le groupe le plus important (17,080 patients)

### Requête test 2 : Tendances 2019
```sql
SELECT 
    CONCAT('Q', CAST(trimestre AS VARCHAR)) as trimestre,
    volume as nb_deces
FROM kpi_temporal_trends
ORDER BY trimestre;
```

**Résultat** : 
- Q1: 172,034 décès
- Q2: 146,523 décès
- Q3: 143,198 décès
- Q4: 158,853 décès

### Requête test 3 : Performance établissements
```sql
SELECT 
    type_etablissement,
    COUNT(*) as nb_etablissements
FROM kpi_etablissement_performance
GROUP BY type_etablissement
ORDER BY nb_etablissements DESC;
```

---

## 🎨 Étape 4 : Créer votre premier Chart

### Chart 1 : Démographie par Sexe et Âge (Bar Chart)

1. Cliquez sur **Charts** (menu du haut) → **+ Chart** (bouton bleu)
2. **Choose a dataset** :
   - Sélectionnez `Trino CHU Gold` dans la liste déroulante "Database"
   - Cliquez sur `kpi_patient_demographics`
   - Cliquez **Create new chart**
3. **Choose chart type** : `Bar Chart`
4. **Configuration** :
   
   **Onglet DATA** :
   - **X-AXIS** (Dimensions) : `tranche_age`
   - **METRICS** :
     - Cliquez sur `+ Add metric`
     - Sélectionnez `Simple` → `SUM(nb_patients)`
   - **BREAKDOWN DIMENSIONS** : `sexe`
   
   **Onglet CUSTOMIZE** :
   - **Chart Title** : `Répartition des Patients par Âge et Sexe`
   - **X Axis Label** : `Tranche d'âge`
   - **Y Axis Label** : `Nombre de patients`
   - **Color Scheme** : `supersetColors`
   - **Show Legend** : ✅ Coché

5. Cliquez sur **Update Chart** (bouton bleu en bas)
6. Cliquez sur **Save** → Nommez : `Demo Patients Demographie`

### Chart 2 : Évolution Décès 2019 (Line Chart)

1. **Charts** → **+ Chart**
2. Dataset : `kpi_temporal_trends`, Type : `Line Chart`
3. **Configuration DATA** :
   - **X-AXIS** : `trimestre`
   - **METRICS** : `SUM(volume)`
   - **Series** : `type_activite`
4. **CUSTOMIZE** :
   - **Chart Title** : `Évolution des Décès par Trimestre 2019`
   - **X Axis Title** : `Trimestre`
   - **Y Axis Title** : `Nombre de décès`
5. **Update Chart** → **Save** : `Demo Tendances Deces`

### Chart 3 : Répartition Décès (Pie Chart)

1. **Charts** → **+ Chart**
2. Dataset : `kpi_deces_by_region`, Type : `Pie Chart`
3. **Configuration DATA** :
   - **DIMENSIONS** : `region`
   - **METRIC** : `SUM(total_deces)`
4. **CUSTOMIZE** :
   - **Chart Title** : `Répartition des Décès par Région`
5. **Update Chart** → **Save** : `Demo Deces Regions`

---

## 📋 Étape 5 : Créer un Dashboard

1. Cliquez sur **Dashboards** → **+ Dashboard**
2. Nommez-le : `KPIs CHU 2019`
3. Cliquez sur **Edit Dashboard** (bouton crayon)
4. **Ajouter les charts** :
   - Dans le panneau de droite, cherchez vos charts
   - Glissez-déposez `Demo Patients Demographie` sur le canvas
   - Ajoutez `Demo Tendances Deces`
   - Ajoutez `Demo Deces Regions`
5. **Organiser** :
   - Redimensionnez les charts en tirant les coins
   - Déplacez-les pour un beau layout
6. **Ajouter un titre** :
   - Cliquez sur **Components** (panneau gauche)
   - Glissez **Text** (Markdown) en haut
   - Contenu :
     ```markdown
     # 📊 KPIs CHU Data Warehouse - Année 2019
     
     Dashboard de synthèse des indicateurs clés de performance.
     Source : Gold Layer (87 lignes agrégées)
     ```
7. Cliquez sur **Save** (en haut à droite)

---

## 🔍 Requêtes SQL Avancées

### Top 5 des tranches d'âge
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

### Variation trimestrielle
```sql
SELECT 
    trimestre,
    volume as nb_deces,
    volume - LAG(volume) OVER (ORDER BY trimestre) as variation
FROM kpi_temporal_trends
ORDER BY trimestre;
```

### Agrégation établissements
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

## 🎯 Datasets Disponibles

| Table | Lignes | Colonnes | Insight Principal |
|-------|--------|----------|-------------------|
| `kpi_patient_demographics` | 10 | sexe, tranche_age, nb_patients | Femmes 75+ = 17K patients |
| `kpi_etablissement_performance` | 69 | code, type, consultations, hospitalisations | 69 établissements distincts |
| `kpi_temporal_trends` | 4 | annee, trimestre, type_activite, volume | 620K décès en 2019 |
| `kpi_deces_by_region` | 2 | region, total_deces, age_moyen | 2 régions identifiées |
| `kpi_satisfaction_global` | 2 | annee, score_moyen, nb_reponses | Scores satisfaction |

---

## 🐛 Problème ?

### Les tables n'apparaissent pas
1. SQL Lab → Database `Trino CHU Gold`
2. Dans le menu **...** (3 points) → **Edit**
3. Onglet **Advanced** → Bouton **Refresh Metadata**

### Erreur de connexion
```bash
# Redémarrer Superset
docker restart chu_superset

# Attendre 10 secondes
sleep 10

# Vérifier les logs
docker logs chu_superset | tail -20
```

### Tester la connexion en CLI
```bash
docker exec chu_superset python3 -c "
from trino.dbapi import connect
conn = connect(host='chu_trino', port=8080, user='trino', catalog='minio', schema='default')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM kpi_patient_demographics')
print(f'Résultat : {cursor.fetchone()}')
"
```

---

## ✅ Checklist

- [x] Connexion Trino configurée dans Superset
- [x] Driver Python `trino` installé
- [ ] SQL Lab exploré avec requêtes test
- [ ] Au moins 1 chart créé
- [ ] Au moins 1 dashboard créé

---

**🎉 Vous êtes prêt à explorer vos KPIs !**

**Accès** : http://localhost:8088 (admin / admin123)

Pour plus de détails, consultez `/home/alban/BigData/BigData/SUPERSET_GUIDE_CONNEXION.md`
