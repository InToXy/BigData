# 🚀 TRINO + POWER BI - GUIDE DE DÉMARRAGE RAPIDE

**Objectif:** Connecter Power BI à la zone Gold via Trino  
**Temps estimé:** 10-15 minutes  
**Date:** 24 Octobre 2025

---

## ⚡ DÉMARRAGE EN 4 ÉTAPES

### Étape 1: Démarrer l'Infrastructure (2 min)

```bash
cd /home/alban/BigData/BigData

# Démarrer tous les services
docker-compose up -d

# Vérifier que Trino est démarré
docker ps | grep chu_trino
```

**Résultat attendu:**
```
chu_trino    Up 30 seconds    0.0.0.0:8090->8080/tcp
```

---

### Étape 2: Initialiser les Tables (1 min)

```bash
# Attendre 30 secondes que Trino soit prêt
sleep 30

# Initialiser les tables Gold dans Trino
./trino/init_trino_tables.sh
```

**Résultat attendu:**
```
✅ Schéma gold créé
✅ 8 tables enregistrées
✅ Initialisation terminée
```

---

### Étape 3: Tester la Connexion (2 min)

```bash
# Test automatique
./trino/test_trino_connection.sh
```

**OU test manuel:**

```bash
# CLI Trino
docker exec -it chu_trino trino --server localhost:8080

# Dans Trino:
USE minio.gold;
SHOW TABLES;
SELECT * FROM kpi_taux_hospitalisation_global;
```

**Interface Web:**
```
http://localhost:8090/ui
```

---

### Étape 4: Connecter Power BI (10 min)

#### A. Installer le Driver ODBC (Windows)

1. **Télécharger:** Simba Trino ODBC Driver (64-bit)
   - URL: https://www.magnitude.com/drivers/trino-odbc-jdbc
   
2. **Installer:** Double-cliquer sur le .msi et suivre l'assistant

#### B. Configurer la Source ODBC

1. **Ouvrir:** ODBC Data Sources (64-bit)
   - `Windows + R` → taper `odbcad32.exe`

2. **Ajouter une source:**
   - Onglet "System DSN" → "Add..."
   - Sélectionner "Simba Trino ODBC Driver"

3. **Configuration:**
   ```
   Data Source Name: CHU_Gold_Trino
   Description: CHU Data Lake - Zone Gold
   
   Host: localhost
   Port: 8090
   
   Catalog: minio
   Schema: gold
   
   Authentication: No Authentication
   SSL: Disabled
   ```

4. **Tester:** Cliquer sur "Test" → "Connection Successful" ✅

#### C. Connecter Power BI

1. **Power BI Desktop** → "Obtenir des données" → "ODBC"

2. **Sélectionner:** `CHU_Gold_Trino`

3. **Naviguer:** 
   - Catalogue: `minio`
   - Schéma: `gold`
   - Tables: Sélectionner les tables KPI

4. **Charger:** Cliquer sur "Charger" ou "Transformer"

---

## 📊 TABLES DISPONIBLES

| Table | Lignes | Utilisation |
|-------|--------|-------------|
| `kpi_taux_hospitalisation_global` | 1 | KPI principal - Carte |
| `kpi_hospitalisation_par_diagnostic` | 768 | Graphiques détaillés |
| `kpi_hospitalisation_sexe_age` | 10 | Analyse démographique |
| `kpi_consultation_par_diagnostic` | ~50 | Consultations |
| `kpi_taux_consultation_periode` | ~5 | Tendances |
| `kpi_deces_par_region_2019` | ~15 | Mortalité |
| `kpi_satisfaction_par_region_2020` | ~60 | Satisfaction |

---

## 🎯 EXEMPLES DE REQUÊTES SQL

### Dashboard Exécutif

```sql
-- KPI Principal
SELECT 
    taux_hospitalisation,
    nb_patients_hospitalises,
    nb_hospitalisations_total
FROM minio.gold.kpi_taux_hospitalisation_global;
```

### Top 10 Pathologies

```sql
SELECT 
    diagnostic_principal,
    nb_hospitalisations,
    taux_hospitalisation
FROM minio.gold.kpi_hospitalisation_par_diagnostic
ORDER BY nb_hospitalisations DESC
LIMIT 10;
```

### Analyse Démographique

```sql
SELECT 
    sexe,
    tranche_age,
    nb_patients_hospitalises,
    taux_hospitalisation
FROM minio.gold.kpi_hospitalisation_sexe_age
ORDER BY sexe, tranche_age;
```

---

## 🔍 VÉRIFICATION

### Checklist Rapide

```
☑ Docker-compose démarré
☑ Trino accessible (http://localhost:8090/ui)
☑ Script init_trino_tables.sh exécuté
☑ Test de connexion réussi
☑ Driver ODBC installé
☑ Source ODBC configurée et testée
☑ Power BI connecté
```

### Commandes de Diagnostic

```bash
# Services actifs
docker ps

# Logs Trino
docker logs chu_trino | tail -50

# Test CLI
docker exec -it chu_trino trino --server localhost:8080

# Interface Web
http://localhost:8090/ui
```

---

## 🐛 PROBLÈMES COURANTS

### ❌ "Cannot connect to Trino"

```bash
# Redémarrer Trino
docker-compose restart trino

# Attendre 30 secondes
sleep 30

# Retester
./trino/test_trino_connection.sh
```

### ❌ "Catalog not found"

```bash
# Réinitialiser les tables
./trino/init_trino_tables.sh
```

### ❌ "Driver not found" (Power BI)

- Vérifier installation ODBC **64-bit**
- Redémarrer Power BI Desktop
- Vérifier dans ODBC Administrator (64-bit)

---

## 📚 DOCUMENTATION COMPLÈTE

- **Guide Power BI détaillé:** `trino/POWERBI_CONNECTION_GUIDE.md`
- **Documentation Trino:** `trino/README.md`
- **Tests:** `trino/test_trino_connection.sh`

---

## 🎉 ARCHITECTURE FINALE

```
┌──────────────┐
│  POWER BI    │  ← Dashboards et visualisations
│  Desktop     │
└──────┬───────┘
       │ ODBC (Port 8090)
       ↓
┌──────────────┐
│    TRINO     │  ← Moteur de requêtes SQL
│ Query Engine │
└──────┬───────┘
       │ S3A Protocol
       ↓
┌──────────────┐
│ MinIO Gold   │  ← Zone Gold (1,563 lignes)
│ (Parquet)    │     12 tables KPI
└──────────────┘
```

**Vous êtes prêt !** 🚀

---

## 🆘 SUPPORT

**Documentation:**
- `trino/README.md`
- `trino/POWERBI_CONNECTION_GUIDE.md`

**Tests:**
```bash
./trino/test_trino_connection.sh
```

**Logs:**
```bash
docker logs -f chu_trino
```

**Web UI:**
```
http://localhost:8090/ui
```

---

**Créé le:** 24 Octobre 2025  
**Status:** ✅ Production Ready
