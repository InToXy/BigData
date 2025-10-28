# 🔧 Guide de résolution : Liste déroulante vide dans Superset

## ✅ Connexion validée

La connexion depuis Superset vers PostgreSQL fonctionne parfaitement :
- ✅ Driver psycopg2 installé dans Superset
- ✅ Connexion réseau OK (172.18.0.3:5432)
- ✅ Authentification OK (admin/admin123)
- ✅ Données accessibles (kpi_synthese_globale testé)

---

## 📌 Solution pour la liste déroulante vide

### Option 1 : Utiliser l'interface moderne de Superset

1. **Aller sur** : http://localhost:8088
2. **Se connecter** : admin / admin123
3. **Cliquer sur** le **+** en haut à droite
4. **Sélectionner** : **"Data" → "Connect database"**
5. **NE PAS utiliser la liste déroulante**, mais cliquer directement sur **"SUPPORTED DATABASES"**
6. **Chercher** "PostgreSQL" dans la liste ou scroller
7. **Cliquer sur PostgreSQL**

### Option 2 : Utiliser SQL Alchemy URI directement

Si l'interface moderne ne fonctionne pas :

1. **Aller dans** : **Settings** → **Database Connections**
2. **Cliquer sur** : **+ DATABASE**
3. **En bas de la modal**, chercher "**Or use a different database**"
4. Dans le champ **"SQLALCHEMY URI"**, coller :
   ```
   postgresql://admin:admin123@172.18.0.3:5432/healthcare_data
   ```
5. **Donner un nom** : `Healthcare Gold Data`
6. **Cliquer sur** "Test Connection"
7. **Si vert**, cliquer sur "Connect"

### Option 3 : Via l'ancien menu (Legacy)

1. **Aller sur** : **Data** (menu du haut)
2. **Databases** dans le sous-menu
3. **Cliquer sur l'icône "+"** (en haut à droite)
4. Dans la fenêtre qui s'ouvre :
   - **Database** : `Healthcare Gold Data`
   - **SQLAlchemy URI** : `postgresql://admin:admin123@172.18.0.3:5432/healthcare_data`
5. **Tester** puis **Sauvegarder**

---

## 🔗 URI de connexion à utiliser

### Recommandé (avec IP) :
```
postgresql://admin:admin123@172.18.0.3:5432/healthcare_data
```

### Alternative (avec hostname) :
```
postgresql://admin:admin123@chu_postgres:5432/healthcare_data
```

---

## 📊 Après connexion : Ajouter les Datasets

Une fois la base connectée :

1. **Aller dans** : **Data** → **Datasets**
2. **Cliquer sur** : **+ DATASET**
3. **Sélectionner** :
   - **DATABASE** : Healthcare Gold Data
   - **SCHEMA** : public
   - **TABLE** : Sélectionner une table KPI

4. **Répéter pour les 7 tables** :
   - kpi_deces_par_annee
   - kpi_deces_par_region
   - kpi_demographic_summary
   - kpi_distribution_age
   - kpi_synthese_globale
   - kpi_temporal_trends
   - kpi_top_departements

---

## 🎯 Vérification rapide via SQL Lab

Pour tester immédiatement :

1. **Aller dans** : **SQL** → **SQL Lab**
2. **Sélectionner** :
   - **DATABASE** : Healthcare Gold Data
   - **SCHEMA** : public
3. **Exécuter une requête test** :
   ```sql
   SELECT * FROM kpi_synthese_globale;
   ```
   
   Devrait retourner :
   - annee_deces: 2019
   - total_deces: 620606
   - age_moyen_global: ~78.81

4. **Autre test** :
   ```sql
   SELECT categorie_age, nombre_deces, pourcentage 
   FROM kpi_distribution_age 
   ORDER BY nombre_deces DESC 
   LIMIT 5;
   ```

---

## ❓ Si la liste reste vide

### Vérifier que Superset est à jour :

```bash
# Redémarrer Superset
docker restart chu_superset

# Attendre 10 secondes
sleep 10

# Vérifier les logs
docker logs chu_superset --tail 50
```

### Vider le cache du navigateur :

1. Ouvrir les DevTools (F12)
2. Onglet **Network**
3. Cocher **"Disable cache"**
4. Rafraîchir la page (Ctrl+F5 ou Cmd+Shift+R)

### Essayer un autre navigateur :

Si le problème persiste, essayer :
- Chrome/Chromium
- Firefox
- Edge

---

## 📸 Captures d'écran attendues

### Écran de connexion Database
Vous devriez voir :
- Un champ "SQLALCHEMY URI"
- Un bouton "Test Connection"
- Des options avancées (exposer dans SQL Lab, etc.)

### Écran SQL Lab
Vous devriez voir :
- Une liste déroulante "DATABASE" avec "Healthcare Gold Data"
- Une liste déroulante "SCHEMA" avec "public"
- Une zone de requête SQL

---

## ✅ Checklist de validation

- [ ] Superset accessible sur http://localhost:8088
- [ ] Connexion admin/admin123 fonctionne
- [ ] Database "Healthcare Gold Data" créée
- [ ] Test de connexion vert (successful)
- [ ] SQL Lab accessible
- [ ] Requête SELECT sur kpi_synthese_globale fonctionne
- [ ] 7 datasets exposés dans Data > Datasets
- [ ] Prêt à créer des charts !

---

## 🆘 Support supplémentaire

Si le problème persiste, vérifier :

```bash
# Superset est bien démarré
docker ps | grep superset

# PostgreSQL est accessible
docker exec chu_postgres psql -U admin -d healthcare_data -c "SELECT COUNT(*) FROM kpi_synthese_globale;"

# Les tables existent
docker exec chu_postgres psql -U admin -d healthcare_data -c "\dt"

# Test de connexion depuis Superset
docker exec chu_superset python3 /tmp/test_pg.py
```
