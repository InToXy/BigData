# Tools - Scripts d'Administration

Ce dossier contient les scripts utilitaires pour la configuration et l'administration de la stack.

## 📋 Scripts Disponibles

### Superset Configuration

#### `fix_superset_connection.py`
- **Description**: Reconfigure la connexion Superset à PostgreSQL
- **Usage**: Résoudre les erreurs "Failed to start remote query on a worker"
- **Action**: 
  - Supprime l'ancienne base de données
  - Recrée avec `allow_run_async=False`
  - Réexpose tous les datasets
- **Prérequis**: Superset accessible sur http://localhost:8088
- **Exécution**: `python3 tools/fix_superset_connection.py`

#### `expose_new_kpis_superset.py`
- **Description**: Expose les 7 nouveaux KPIs métier dans Superset
- **KPIs exposés**:
  - kpi_consultation_etablissement
  - kpi_consultation_professionnel
  - kpi_hospitalisation_globale
  - kpi_hospitalisation_sexe_age
  - kpi_deces_region_2019
  - kpi_satisfaction_region
  - kpi_consultations_synthese
- **Authentification**: Bearer token + CSRF
- **Exécution**: `python3 tools/expose_new_kpis_superset.py`

## 🔧 Configuration Superset

### Credentials
- **URL**: http://localhost:8088
- **Username**: admin
- **Password**: admin123

### Database PostgreSQL
- **Name**: Healthcare Gold Data
- **Host**: 172.18.0.3:5432
- **Database**: healthcare_data
- **User**: admin
- **Password**: admin123
- **Configuration critique**: `allow_run_async=False` (évite les erreurs de worker)

## 📝 Fichiers Supprimés (Cleanup Oct 2025)

Les fichiers suivants ont été supprimés car obsolètes:
- configure_superset.py (remplacé par fix_superset_connection.py)
- create_hive_tables.sql (non utilisé)
- create_spark_tables.py (non utilisé)

## 📚 Documentation Associée

Voir `/docs/` pour les guides complets:
- SUPERSET_CONNECTION_GUIDE.md
- SUPERSET_TROUBLESHOOTING.md
- SUPERSET_FIXED.md
