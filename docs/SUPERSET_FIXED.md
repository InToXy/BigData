# ✅ PROBLÈME RÉSOLU : Superset Database Error

## 🔧 Qu'est-ce qui a été corrigé ?

Le problème "Failed to start remote query on a worker" venait de la configuration de la database qui avait le paramètre **`allow_run_async: True`**. Ce paramètre indique à Superset d'essayer d'exécuter les requêtes de manière distribuée (comme avec Trino/Presto), ce qui ne fonctionne pas avec PostgreSQL en mode direct.

### Solution appliquée :

✅ Reconfiguration complète de la connexion avec :
- `allow_run_async: False` ← **CRUCIAL**
- `cost_estimate_enabled: False`
- Connection timeout: 10s
- Backend: PostgreSQL (direct)

## 📊 Configuration actuelle

**Database** : Healthcare Gold Data (ID: 1)
**URI** : `postgresql://admin:admin123@172.18.0.3:5432/healthcare_data`
**Datasets exposés** : 7 tables KPI

## 🎯 Comment utiliser Superset maintenant

### 1. Accéder à SQL Lab (pour tester)

1. Ouvrir http://localhost:8088
2. Se connecter : admin / admin123
3. Menu : **SQL** → **SQL Lab**
4. Sélectionner :
   - **DATABASE** : Healthcare Gold Data
   - **SCHEMA** : public
5. Exécuter une requête test :

```sql
SELECT * FROM kpi_synthese_globale;
```

**Résultat attendu** :
- annee_deces: 2019
- total_deces: 620606
- age_moyen_global: 78.81

### 2. Créer une visualisation

1. Menu : **Charts** → **+ Chart**
2. **Choose a dataset** : Sélectionner un des 7 datasets :
   - kpi_deces_par_annee
   - kpi_deces_par_region
   - kpi_demographic_summary
   - kpi_distribution_age
   - kpi_synthese_globale
   - kpi_temporal_trends
   - kpi_top_departements

3. **Choose chart type** : Sélectionner le type de graphique
4. Configurer et **Save**

## 📈 Exemples de visualisations recommandées

### Chart 1 : Distribution par âge (Pie Chart)

- **Dataset** : kpi_distribution_age
- **Chart Type** : Pie Chart
- **Dimension** : categorie_age
- **Metric** : SUM(nombre_deces)
- **Title** : Distribution des décès par catégorie d'âge (2019)

### Chart 2 : Décès par sexe et âge (Bar Chart)

- **Dataset** : kpi_deces_par_annee  
- **Chart Type** : Bar Chart
- **X-Axis** : categorie_age
- **Metrics** : SUM(nombre_deces)
- **Group by** : sexe
- **Title** : Décès par sexe et tranche d'âge (2019)

### Chart 3 : Top 20 départements (Table)

- **Dataset** : kpi_top_departements
- **Chart Type** : Table
- **Columns** : 
  - rang_departement
  - code_dept
  - nombre_deces
  - age_moyen
- **Sort** : rang_departement ASC
- **Title** : Top 20 départements - Décès 2019

### Chart 4 : Tendances mensuelles (Line Chart)

- **Dataset** : kpi_temporal_trends
- **Chart Type** : Line Chart
- **X-Axis** : annee_mois
- **Metrics** : SUM(nombre_deces)
- **Title** : Évolution mensuelle des décès (2019)

### Chart 5 : Carte de France (Optionnel - si plugin geo installé)

- **Dataset** : kpi_deces_par_region
- **Chart Type** : Deck.gl Polygon (ou Country Map)
- **Region** : code_dept
- **Metric** : SUM(nombre_deces)

## ⚠️ Important : Table Schema

Si vous voyez encore l'erreur dans "Table Schema" :
- **Ignorez cette fonctionnalité** pour l'instant
- Utilisez plutôt **SQL Lab** ou créez directement des **Charts**
- Le schéma est correctement configuré dans les datasets

Pour vérifier les colonnes d'une table :
```sql
-- Dans SQL Lab
SELECT * FROM kpi_distribution_age LIMIT 1;
```

## 🎨 Créer un Dashboard

Une fois vos 4-5 charts créés :

1. Menu : **Dashboards** → **+ Dashboard**
2. **Nom** : Analyse Décès France 2019
3. **Drag & Drop** vos charts sur le dashboard
4. Ajuster la taille et position
5. **Save** le dashboard

## 📊 Données disponibles

### kpi_synthese_globale (1 ligne)
Vue d'ensemble 2019 : total, âge moyen, ratio H/F

### kpi_distribution_age (9 lignes)
Distribution par catégories d'âge avec pourcentages

### kpi_deces_par_annee (17 lignes)
Agrégation par année/sexe/catégorie d'âge

### kpi_deces_par_region (99 lignes)
Répartition par département avec classement

### kpi_demographic_summary (2 lignes)
Statistiques démographiques par sexe (médiane, quartiles)

### kpi_temporal_trends (12 lignes)
Tendances mensuelles 2019

### kpi_top_departements (20 lignes)
Top 20 départements par nombre de décès

## ✅ Checklist

- [x] Database configurée avec allow_run_async=False
- [x] 7 datasets exposés
- [x] Connection PostgreSQL validée
- [ ] Tester SQL Lab avec SELECT * FROM kpi_synthese_globale
- [ ] Créer Chart 1 : Pie Chart distribution âge
- [ ] Créer Chart 2 : Bar Chart sexe x âge
- [ ] Créer Chart 3 : Table top départements
- [ ] Créer Chart 4 : Line Chart tendances
- [ ] Créer Dashboard et assembler les charts

## 🆘 Si problème persiste

1. **Vider le cache du navigateur** (Ctrl+Shift+Delete)
2. **Redémarrer Superset** : `docker restart chu_superset`
3. **Attendre 10 secondes** que Superset redémarre
4. **Rafraîchir la page** (F5)

## 📞 Support

Vérifier que tout fonctionne :

```bash
# PostgreSQL accessible
docker exec chu_postgres psql -U admin -d healthcare_data -c "SELECT COUNT(*) FROM kpi_synthese_globale;"

# Superset en cours d'exécution
docker ps | grep superset

# Reconfigurer si besoin
python3 /home/alban/BigData/BigData/tools/fix_superset_connection.py
```

---

**🎯 Vous êtes maintenant prêt à créer vos visualisations !**
