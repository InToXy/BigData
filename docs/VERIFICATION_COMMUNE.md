# VÉRIFICATION COLONNE COMMUNE - GUIDE RAPIDE

## ✅ ÉTAT ACTUEL

### PostgreSQL (Source de données)
```
✅ Table: kpi_consultation_etablissement
✅ Total lignes: 372,655
✅ Lignes avec commune: 271,920 (73%)
✅ Lignes sans commune: 100,735 (27%)
```

### Superset (Dataset)
```
✅ Dataset ID: 8
✅ Colonnes synchronisées: 7
✅ Colonne 'commune' présente: OUI
```

## 🔍 VÉRIFICATION DANS SUPERSET

### Méthode 1: SQL Lab (RECOMMANDÉE)
1. Ouvrez Superset: **http://172.28.168.129:8088**
2. Connectez-vous: admin / admin123
3. Allez dans: **SQL Lab** > **SQL Editor**
4. Sélectionnez database: **Healthcare Gold Data**
5. Exécutez cette requête:
```sql
SELECT 
    raison_sociale_site, 
    commune, 
    nombre_consultations 
FROM kpi_consultation_etablissement 
WHERE commune IS NOT NULL 
ORDER BY nombre_consultations DESC 
LIMIT 20;
```

**Résultat attendu:**
- 20 lignes avec des noms de communes (Nantes, Paris, Montpellier, Lille, etc.)
- Si vous voyez NULL, c'est un problème de cache Superset

### Méthode 2: Vérifier le Dataset
1. Allez dans: **Data** > **Datasets**
2. Cherchez: **kpi_consultation_etablissement**
3. Cliquez sur le dataset
4. Onglet **Columns**
5. Vérifiez que la colonne **commune** apparaît dans la liste

**Colonnes attendues:**
- ✓ raison_sociale_site
- ✓ commune (← DOIT ÊTRE PRÉSENTE)
- ✓ nombre_consultations
- ✓ nombre_etablissements_distincts
- ✓ annee
- ✓ _gold_batch_id
- ✓ _gold_load_date

### Méthode 3: Créer un Chart de test
1. Allez dans: **Charts** > **+ Chart**
2. Choose Dataset: **kpi_consultation_etablissement**
3. Visualization Type: **Table**
4. Dimensions: 
   - Ajoutez **raison_sociale_site**
   - Ajoutez **commune** (← doit apparaître dans la liste)
5. Metrics: 
   - Ajoutez **nombre_consultations**
6. Filters:
   - commune IS NOT NULL
7. Cliquez **Update Chart**

**Résultat attendu:**
- Tableau avec 3 colonnes
- Colonne commune remplie avec des noms de villes

## 🔧 SI COMMUNE EST TOUJOURS NULL

### Solution A: Vider le cache Superset
```bash
docker exec chu_superset superset clear-cache
docker restart chu_superset
```

### Solution B: Resynchroniser manuellement
1. Data > Datasets > kpi_consultation_etablissement
2. Edit Dataset
3. Onglet **Columns**
4. Cliquez sur **Sync columns from source** (bouton en haut)
5. Sauvegardez

### Solution C: Recréer le dataset
1. Data > Datasets
2. Supprimez **kpi_consultation_etablissement**
3. Cliquez **+ Dataset**
4. Database: Healthcare Gold Data
5. Schema: public
6. Table: kpi_consultation_etablissement
7. Créez

## 📊 EXEMPLES DE DONNÉES

Top 15 établissements par nombre de consultations (avec commune):

| Raison Sociale                          | Commune               | Consultations |
|-----------------------------------------|-----------------------|---------------|
| LBM BIOLIANCE                           | Nantes                | 15            |
| INSTITUT PASTEUR                        | Paris                 | 12            |
| CENTRE HOSPITALIER                      | Papeete               | 12            |
| CABINET DU DR EMILIE AUBERT BRINGER     | Montpellier           | 11            |
| LABORATOIRE SECONDAIRE CERBALLIANCE HA  | Lille                 | 9             |
| CABINET DU DR LAURENT CHARRA            | Montpellier           | 9             |
| CENTRE HOSPITALIER DE PF                | Papeete               | 9             |
| CABINET DU DR MARIE-CAROLINE MAS        | Montpellier           | 8             |
| CABINET DU DR FRANCOIS KLEIN            | Montpellier           | 8             |
| CABINET DU DR ALAIN AIEM                | Charleville-Mézières  | 8             |

## 🎯 PROCHAINES ÉTAPES

Une fois la colonne commune visible dans Superset:

1. **Créer une visualisation géographique**
   - Chart Type: **Big Number** ou **Table**
   - Group by: commune
   - Metric: SUM(nombre_consultations)

2. **Créer un filtre par commune**
   - Dans vos charts, ajoutez commune comme filtre
   - Permet de filtrer par ville

3. **Créer un dashboard régional**
   - Combinez avec les données de région
   - Analyse par territoire

## 📝 COMMANDES DE DIAGNOSTIC

### Vérifier PostgreSQL
```bash
docker exec chu_postgres psql -U admin -d healthcare_data -c "
SELECT 
    COUNT(*) as total,
    COUNT(commune) as avec_commune,
    ROUND(100.0 * COUNT(commune) / COUNT(*), 2) as pourcentage
FROM kpi_consultation_etablissement;"
```

### Lister les communes disponibles
```bash
docker exec chu_postgres psql -U admin -d healthcare_data -c "
SELECT DISTINCT commune, COUNT(*) as nb_etablissements
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL
GROUP BY commune
ORDER BY nb_etablissements DESC
LIMIT 20;"
```

### Statistiques par commune
```bash
docker exec chu_postgres psql -U admin -d healthcare_data -c "
SELECT 
    commune,
    COUNT(*) as nb_etablissements,
    SUM(nombre_consultations) as total_consultations
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL
GROUP BY commune
ORDER BY total_consultations DESC
LIMIT 15;"
```

## ✅ CONFIRMATION

**Statut de la correction:**
- ✅ Bronze: commune présente (277,860 non-NULL sur 416,665)
- ✅ Silver: dim_etablissement avec commune
- ✅ Gold: kpi_consultation_etablissement avec commune
- ✅ PostgreSQL: 271,920 communes sur 372,655 lignes
- ✅ Superset: Dataset synchronisé avec colonne commune

**Le problème est résolu au niveau des données.**
Si vous voyez encore NULL dans Superset, c'est un problème de cache/synchronisation de l'interface.
