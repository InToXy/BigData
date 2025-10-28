# Guide de connexion Superset → PostgreSQL → Gold Layer

## ✅ Configuration terminée

Les KPIs Gold sont maintenant chargés dans PostgreSQL et prêts pour Superset !

### 📊 Tables disponibles dans PostgreSQL

Les 7 tables KPI suivantes sont disponibles dans la base `healthcare_data` :

1. **kpi_deces_par_annee** (17 lignes)
   - Agrégation par année, sexe et catégorie d'âge
   - Colonnes : annee_deces, sexe, categorie_age, nombre_deces, age_moyen, pourcentage_annee

2. **kpi_deces_par_region** (99 lignes)
   - Répartition géographique par département
   - Colonnes : annee_deces, code_dept, nombre_deces, age_moyen, rang_departement

3. **kpi_demographic_summary** (2 lignes)
   - Statistiques démographiques globales par sexe
   - Colonnes : annee_deces, sexe, total_deces, age_moyen, age_median, age_q1, age_q3

4. **kpi_temporal_trends** (12 lignes)
   - Tendances temporelles mensuelles
   - Colonnes : annee, mois, trimestre, nombre_deces, age_moyen, annee_mois

5. **kpi_top_departements** (20 lignes)
   - Top 20 départements par nombre de décès
   - Colonnes : annee_deces, code_dept, nombre_deces, age_moyen, rang_departement

6. **kpi_distribution_age** (9 lignes)
   - Distribution par catégories d'âge avec pourcentages
   - Colonnes : annee_deces, categorie_age, nombre_deces, pourcentage

7. **kpi_synthese_globale** (1 ligne)
   - Vue d'ensemble annuelle
   - Colonnes : total_deces, age_moyen_global, age_median_global, nombre_lieux_deces, ratio_hommes_femmes

---

## 🔗 Connexion Superset à PostgreSQL

### Étape 1 : Accéder à Superset

1. Ouvrir votre navigateur
2. Aller sur : **http://localhost:8088**
3. Se connecter avec :
   - Username: `admin`
   - Password: `admin123`

### Étape 2 : Ajouter la connexion PostgreSQL

1. Cliquer sur **"Data"** dans le menu supérieur
2. Sélectionner **"Databases"**
3. Cliquer sur **"+ Database"** (bouton bleu en haut à droite)
4. Choisir **"PostgreSQL"** dans la liste

### Étape 3 : Configuration de la connexion

Utiliser les paramètres suivants :

**Méthode recommandée : URI de connexion avec IP**
```
postgresql://admin:admin123@172.18.0.3:5432/healthcare_data
```

**OU Méthode avec nom d'hôte (si résolution DNS fonctionne)**
```
postgresql://admin:admin123@chu_postgres:5432/healthcare_data
```

**OU Configuration manuelle :**
- **Host**: `172.18.0.3` (ou `chu_postgres`)
- **Port**: `5432`
- **Database**: `healthcare_data`
- **Username**: `admin`
- **Password**: `admin123`

### Étape 4 : Tester la connexion

1. Cliquer sur **"Test Connection"**
2. Si OK, cliquer sur **"Connect"**
3. Donner un nom à la connexion : **"Healthcare Gold Data"**

### Étape 5 : Exposer les tables

1. Aller dans **"Data" > "Datasets"**
2. Cliquer sur **"+ Dataset"**
3. Sélectionner :
   - **Database** : Healthcare Gold Data
   - **Schema** : public
   - **Table** : Sélectionner chaque table KPI une par une

Répéter pour chaque table :
- kpi_deces_par_annee
- kpi_deces_par_region
- kpi_demographic_summary
- kpi_temporal_trends
- kpi_top_departements
- kpi_distribution_age
- kpi_synthese_globale

---

## 📊 Suggestions de visualisations

### Visualisation 1 : Distribution par âge (Pie Chart)
- **Dataset** : kpi_distribution_age
- **Type** : Pie Chart
- **Dimension** : categorie_age
- **Métrique** : SUM(nombre_deces)

### Visualisation 2 : Décès par sexe et âge (Bar Chart)
- **Dataset** : kpi_deces_par_annee
- **Type** : Bar Chart
- **X-axis** : categorie_age
- **Metrics** : SUM(nombre_deces)
- **Group by** : sexe

### Visualisation 3 : Top 20 départements (Table)
- **Dataset** : kpi_top_departements
- **Type** : Table
- **Columns** : code_dept, nombre_deces, age_moyen, rang_departement
- **Sort** : rang_departement ASC

### Visualisation 4 : Tendances mensuelles (Line Chart)
- **Dataset** : kpi_temporal_trends
- **Type** : Line Chart
- **X-axis** : annee_mois
- **Metrics** : SUM(nombre_deces)

---

## 🎯 Requêtes SQL utiles pour vérification

### Vérifier les données chargées
```sql
-- Total des décès 2019
SELECT total_deces FROM kpi_synthese_globale;

-- Distribution par âge
SELECT categorie_age, nombre_deces, pourcentage 
FROM kpi_distribution_age 
ORDER BY nombre_deces DESC;

-- Top 5 départements
SELECT code_dept, nombre_deces, age_moyen, rang_departement
FROM kpi_top_departements
WHERE rang_departement <= 5
ORDER BY rang_departement;

-- Tendances mensuelles
SELECT annee_mois, nombre_deces, age_moyen
FROM kpi_temporal_trends
ORDER BY annee, mois;
```

---

## 🔧 Architecture complète

```
┌─────────────┐
│   MinIO     │ Bronze (210 MB) → Silver (20 MB) → Gold (23 KB Parquet)
│   (S3)      │                                              ↓
└─────────────┘                                              ↓
                                                             ↓
┌─────────────┐                                  ┌──────────────────┐
│   Spark     │ ──── gold_to_postgres.py ───→   │  PostgreSQL      │
│   Jobs      │                                  │  healthcare_data │
└─────────────┘                                  │  7 tables KPI    │
                                                 └──────────────────┘
                                                             ↓
                                                             ↓
                                                 ┌──────────────────┐
                                                 │    Superset      │
                                                 │  Dashboards +    │
                                                 │  Visualizations  │
                                                 └──────────────────┘
```

---

## ✅ Prochaines étapes

1. ✅ Données Bronze chargées (21 tables)
2. ✅ Modèle Silver créé (dimensions + faits)
3. ✅ KPIs Gold calculés (7 KPIs)
4. ✅ KPIs chargés dans PostgreSQL
5. ⏸️ Connexion Superset à PostgreSQL (à faire manuellement via l'interface)
6. ⏸️ Création des 4+ visualisations
7. ⏸️ Assemblage du dashboard final

---

## 📞 Support

Si problème de connexion :
- Vérifier que chu_postgres tourne : `docker ps | grep postgres`
- Tester la connexion : `docker exec chu_postgres psql -U admin -d healthcare_data -c "SELECT COUNT(*) FROM kpi_synthese_globale;"`
- Vérifier les tables : `docker exec chu_postgres psql -U admin -d healthcare_data -c "\dt"`
