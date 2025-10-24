# 🔌 GUIDE DE CONNEXION POWER BI ➡️ TRINO ➡️ ZONE GOLD

**Date:** 24 Octobre 2025  
**Version Trino:** 435  
**Port:** 8090

---

## 📋 PRÉREQUIS

### 1. Services Démarrés

```bash
# Démarrer l'infrastructure
cd /home/alban/BigData/BigData
docker-compose up -d

# Vérifier que Trino est démarré
docker ps | grep chu_trino

# Attendre ~30 secondes puis initialiser les tables
./trino/init_trino_tables.sh
```

### 2. Driver ODBC Trino pour Windows

**Télécharger le driver:**
- **Recommandé:** Simba Trino ODBC Driver
  - URL: https://www.magnitude.com/drivers/trino-odbc-jdbc
  - Version: Latest (64-bit)
  
**Alternative:** Starburst ODBC Driver
- URL: https://www.starburst.io/platform/starburst-enterprise/drivers/

**Installation:**
1. Télécharger le fichier .msi (64-bit)
2. Double-cliquer et suivre l'assistant
3. Accepter les termes de licence
4. Installation standard

---

## 🔧 CONFIGURATION ODBC (Méthode Recommandée)

### Étape 1: Créer une source de données ODBC

1. **Ouvrir "ODBC Data Sources (64-bit)"**
   - Appuyer sur `Windows + R`
   - Taper `odbcad32.exe`
   - Sélectionner l'onglet "User DSN" ou "System DSN"

2. **Ajouter une nouvelle source**
   - Cliquer sur "Add..."
   - Sélectionner "Simba Trino ODBC Driver"
   - Cliquer sur "Finish"

3. **Configurer la connexion**

```
┌─────────────────────────────────────────┐
│  Simba Trino ODBC Driver Configuration │
├─────────────────────────────────────────┤
│                                         │
│  Data Source Name: CHU_Gold_Trino       │
│  Description: CHU Data Lake - Zone Gold │
│                                         │
│  Host: localhost                        │
│  Port: 8090                             │
│                                         │
│  Catalog: minio                         │
│  Schema: gold                           │
│                                         │
│  Authentication Type: No Authentication │
│   (ou Username Only avec votre nom)     │
│                                         │
│  SSL: Disabled ☐                        │
│                                         │
└─────────────────────────────────────────┘
```

4. **Tester la connexion**
   - Cliquer sur "Test"
   - Vous devez voir: "Connection Successful"

---

## 📊 CONNEXION DEPUIS POWER BI

### Méthode 1: Via ODBC (Recommandé)

1. **Ouvrir Power BI Desktop**

2. **Obtenir des données**
   - Onglet "Accueil" → "Obtenir des données"
   - Rechercher "ODBC"
   - Sélectionner "ODBC" → "Connecter"

3. **Sélectionner la source**
   - Dans la liste déroulante, choisir: `CHU_Gold_Trino`
   - Cliquer sur "OK"

4. **Naviguer dans les données**
   - Catalogue: `minio`
   - Schéma: `gold`
   - Tables: Vous verrez toutes les tables KPI

5. **Sélectionner les tables**
   ```
   ☑ kpi_taux_consultation_periode
   ☑ kpi_taux_hospitalisation_global
   ☑ kpi_hospitalisation_par_diagnostic
   ☑ kpi_hospitalisation_sexe_age
   ... (autres tables)
   ```

6. **Charger ou Transformer**
   - "Charger" : Import direct
   - "Transformer les données" : Ouvre Power Query Editor

---

### Méthode 2: Via Requête Directe (Advanced)

1. **Obtenir des données → Requête vide**

2. **Ouvrir l'éditeur avancé**

3. **Coller ce code M:**

```m
let
    Source = Odbc.DataSource(
        "dsn=CHU_Gold_Trino",
        [
            HierarchicalNavigation = true,
            Implementation = "2.0"
        ]
    ),
    minio_Database = Source{[Name="minio",Kind="Database"]}[Data],
    gold_Schema = minio_Database{[Name="gold",Kind="Schema"]}[Data]
in
    gold_Schema
```

---

### Méthode 3: Connexion DirectQuery (Temps Réel)

**Avantage:** Données toujours à jour, pas de rafraîchissement manuel

1. **Lors de la connexion ODBC:**
   - Sélectionner "DirectQuery" au lieu de "Import"

2. **Avantages/Inconvénients:**
   ```
   ✅ Données en temps réel
   ✅ Pas de limite de taille
   ✅ Moins d'utilisation mémoire PowerBI
   
   ❌ Performance dépend de Trino
   ❌ Fonctionnalités DAX limitées
   ```

---

## 🎯 TABLES DISPONIBLES POUR POWER BI

### Tables Gold (KPIs)

| Table | Lignes | Utilisation PowerBI |
|-------|--------|---------------------|
| `kpi_taux_hospitalisation_global` | 1 | **Carte/KPI principal** |
| `kpi_hospitalisation_par_diagnostic` | 768 | **Graphiques détaillés** |
| `kpi_hospitalisation_sexe_age` | 10 | **Analyse démographique** |
| `kpi_consultation_par_diagnostic` | ~50 | Graphiques consultations |
| `kpi_taux_consultation_periode` | ~5 | Tendances temporelles |
| `kpi_deces_par_region_2019` | ~15 | Cartes géographiques |
| `kpi_satisfaction_par_region_2020` | ~60 | Satisfaction patients |

---

## 📈 EXEMPLES DE REQUÊTES SQL (PowerBI Advanced)

### Requête 1: Top 10 Diagnostics

```sql
SELECT 
    diagnostic_principal,
    nb_patients_hospitalises,
    nb_hospitalisations,
    taux_hospitalisation
FROM minio.gold.kpi_hospitalisation_par_diagnostic
ORDER BY nb_hospitalisations DESC
LIMIT 10
```

### Requête 2: Hospitalisation par Tranche d'Âge et Sexe

```sql
SELECT 
    sexe,
    tranche_age,
    nb_patients_hospitalises,
    nb_hospitalisations,
    taux_hospitalisation
FROM minio.gold.kpi_hospitalisation_sexe_age
ORDER BY sexe, tranche_age
```

### Requête 3: Tendance Consultations

```sql
SELECT 
    periode_debut,
    periode_fin,
    nb_patients_distincts,
    nb_consultations_total,
    taux_consultation_moyen
FROM minio.gold.kpi_taux_consultation_periode
ORDER BY periode_debut
```

---

## 🎨 DASHBOARDS RECOMMANDÉS

### Dashboard 1: Vue Exécutive

**Éléments:**
- 📊 Carte KPI: Taux d'hospitalisation global
- 📈 Graphique en courbes: Évolution consultations
- 🗺️ Carte géographique: Décès par région
- ⭐ Jauge: Satisfaction moyenne

### Dashboard 2: Analyse Pathologies

**Éléments:**
- 📊 Top 20 diagnostics (barres horizontales)
- 🥧 Répartition par catégorie (pie chart)
- 📈 Tendance temporelle par diagnostic
- 📋 Table détaillée avec filtres

### Dashboard 3: Analyse Démographique

**Éléments:**
- 📊 Pyramide des âges
- 👥 Répartition par sexe
- 🎯 Taux hospitalisation par segment
- 📈 Évolution par tranche d'âge

---

## 🔍 VÉRIFICATION DE LA CONNEXION

### Test 1: Via Interface Web Trino

```bash
# Ouvrir dans navigateur
http://localhost:8090/ui

# Login: (laisser vide ou mettre votre nom)
# Explorer: Catalogues → minio → gold → Tables
```

### Test 2: Via CLI Trino

```bash
# Connexion au conteneur
docker exec -it chu_trino trino --server localhost:8080

# Commandes SQL
USE minio.gold;
SHOW TABLES;
SELECT * FROM kpi_taux_hospitalisation_global;
DESCRIBE kpi_hospitalisation_par_diagnostic;
```

### Test 3: Via ODBC Test

```bash
# Dans ODBC Data Source Administrator
# Sélectionner CHU_Gold_Trino → Configure → Test
# Doit afficher: "Connection successful"
```

---

## 🐛 DÉPANNAGE

### Problème 1: "Cannot connect to Trino"

**Solutions:**
```bash
# Vérifier que Trino est démarré
docker ps | grep chu_trino

# Vérifier les logs
docker logs chu_trino

# Redémarrer Trino
docker restart chu_trino
```

### Problème 2: "Catalog 'minio' does not exist"

**Solutions:**
```bash
# Vérifier la configuration
cat /home/alban/BigData/BigData/trino/catalog/minio.properties

# Recréer le conteneur
docker-compose down
docker-compose up -d trino
```

### Problème 3: "Table not found"

**Solutions:**
```bash
# Réexécuter le script d'initialisation
./trino/init_trino_tables.sh

# Ou manuellement dans Trino CLI
docker exec -it chu_trino trino --server localhost:8080
CREATE SCHEMA IF NOT EXISTS minio.gold WITH (location = 's3a://gold/');
```

### Problème 4: "Driver not found" dans PowerBI

**Solutions:**
1. Vérifier installation driver ODBC 64-bit
2. Redémarrer Power BI Desktop
3. Vérifier dans ODBC Administrator (64-bit)

---

## ⚡ OPTIMISATIONS POWERBI

### 1. Mode DirectQuery vs Import

**Import Mode:**
- ✅ Meilleure performance visualisation
- ✅ Toutes fonctionnalités DAX
- ❌ Nécessite rafraîchissement manuel
- **Recommandé pour:** Gold (1,563 lignes seulement)

**DirectQuery Mode:**
- ✅ Données toujours à jour
- ❌ Performance variable
- **Recommandé pour:** Bronze/Silver (gros volumes)

### 2. Optimisation des Requêtes

```sql
-- ✅ BON: Filtrage côté serveur
SELECT * 
FROM minio.gold.kpi_hospitalisation_par_diagnostic
WHERE nb_hospitalisations > 100

-- ❌ MAUVAIS: Récupérer tout puis filtrer dans PowerBI
SELECT * FROM minio.gold.kpi_hospitalisation_par_diagnostic
-- (puis filtrer dans PowerBI)
```

### 3. Rafraîchissement Planifié

**Configuration:**
1. Publier sur Power BI Service
2. Paramètres du dataset → Actualisation planifiée
3. Configurer la passerelle de données (si nécessaire)

---

## 📞 SUPPORT

### Logs Trino

```bash
# Voir les logs en temps réel
docker logs -f chu_trino

# Dernières 100 lignes
docker logs --tail 100 chu_trino
```

### Métriques Trino

```bash
# Interface Web
http://localhost:8090/ui

# API REST
curl http://localhost:8090/v1/info
curl http://localhost:8090/v1/query
```

---

## ✅ CHECKLIST DE MISE EN PRODUCTION

```
☐ Docker-compose démarré (docker-compose up -d)
☐ Trino accessible (http://localhost:8090/ui)
☐ Script init_trino_tables.sh exécuté
☐ Driver ODBC installé sur Windows
☐ Source ODBC configurée (CHU_Gold_Trino)
☐ Test de connexion ODBC réussi
☐ Power BI connecté et tables visibles
☐ Dashboard créé et testé
☐ Documentation partagée avec l'équipe
```

---

## 🎉 RÉSULTAT FINAL

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│              │  ODBC   │              │  SQL    │              │
│  POWER BI    │────────→│    TRINO     │────────→│  MinIO Gold  │
│  Desktop     │  8090   │  Query Engine│  S3A    │  (Parquet)   │
│              │         │              │         │              │
└──────────────┘         └──────────────┘         └──────────────┘
                                │
                                │ SQL
                                ↓
                         ┌──────────────┐
                         │              │
                         │  PostgreSQL  │
                         │  (Source)    │
                         │              │
                         └──────────────┘
```

**Vous pouvez maintenant créer des dashboards PowerBI connectés directement à vos données Gold !** 🚀

---

**Dernière mise à jour:** 24 Octobre 2025
