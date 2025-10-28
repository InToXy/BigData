#!/usr/bin/env python3
"""
Reconfigurer Superset avec les bons paramètres PostgreSQL
"""
import requests
import json
import time

SUPERSET_URL = "http://localhost:8088"
SUPERSET_USER = "admin"
SUPERSET_PASSWORD = "admin123"
PG_URI = "postgresql://admin:admin123@172.18.0.3:5432/healthcare_data"
DATABASE_NAME = "Healthcare Gold Data"

print("🔧 Reconfiguration de Superset...\n")

session = requests.Session()

# Login
print("🔐 Connexion...")
login_url = f"{SUPERSET_URL}/api/v1/security/login"
login_data = {
    "username": SUPERSET_USER,
    "password": SUPERSET_PASSWORD,
    "provider": "db",
    "refresh": True
}

login_response = session.post(login_url, json=login_data)
access_token = login_response.json()["access_token"]
session.headers.update({"Authorization": f"Bearer {access_token}"})
print("   ✅ Connecté\n")

# CSRF
csrf_url = f"{SUPERSET_URL}/api/v1/security/csrf_token/"
csrf_response = session.get(csrf_url)
csrf_token = csrf_response.json()["result"]
session.headers.update({
    "X-CSRFToken": csrf_token,
    "Referer": SUPERSET_URL,
    "Content-Type": "application/json"
})

# Supprimer l'ancienne database
print("🗑️  Suppression de l'ancienne connexion...")
try:
    delete_url = f"{SUPERSET_URL}/api/v1/database/1"
    delete_response = session.delete(delete_url)
    if delete_response.status_code in [200, 404]:
        print("   ✅ Ancienne connexion supprimée\n")
    else:
        print(f"   ⚠️  Code: {delete_response.status_code}\n")
except Exception as e:
    print(f"   ⚠️  {e}\n")

time.sleep(1)

# Recréer avec les bons paramètres
print("💾 Création de la nouvelle connexion...")
database_url = f"{SUPERSET_URL}/api/v1/database/"
database_data = {
    "database_name": DATABASE_NAME,
    "sqlalchemy_uri": PG_URI,
    "expose_in_sqllab": True,
    "allow_run_async": False,  # IMPORTANT: Désactiver l'async
    "allow_ctas": False,
    "allow_cvas": False,
    "allow_dml": False,
    "force_ctas_schema": "",
    "cache_timeout": None,
    "encrypted_extra": "{}",
    "extra": json.dumps({
        "metadata_params": {},
        "engine_params": {
            "connect_args": {
                "connect_timeout": 10
            }
        },
        "metadata_cache_timeout": {},
        "schemas_allowed_for_csv_upload": [],
        "cost_estimate_enabled": False
    }),
    "server_cert": None,
    "impersonate_user": False,
    "allow_csv_upload": False,
    "is_managed_externally": False
}

try:
    db_response = session.post(database_url, json=database_data)
    if db_response.status_code == 201:
        database_id = db_response.json()["id"]
        print(f"   ✅ Database créée (ID: {database_id})\n")
    else:
        print(f"   ❌ Erreur: {db_response.status_code}")
        print(f"   {db_response.text[:300]}\n")
        exit(1)
except Exception as e:
    print(f"   ❌ Exception: {e}\n")
    exit(1)

# Exposer les datasets
print("📊 Exposition des datasets...")
dataset_url = f"{SUPERSET_URL}/api/v1/dataset/"

KPI_TABLES = [
    "kpi_deces_par_annee",
    "kpi_deces_par_region",
    "kpi_demographic_summary",
    "kpi_distribution_age",
    "kpi_synthese_globale",
    "kpi_temporal_trends",
    "kpi_top_departements"
]

for table_name in KPI_TABLES:
    dataset_data = {
        "database": database_id,
        "schema": "public",
        "table_name": table_name
    }
    
    try:
        dataset_response = session.post(dataset_url, json=dataset_data)
        if dataset_response.status_code in [201, 200]:
            print(f"   ✅ {table_name}")
        elif dataset_response.status_code == 422:
            print(f"   ⚠️  {table_name} (existe déjà)")
        else:
            print(f"   ❌ {table_name} - {dataset_response.status_code}")
    except Exception as e:
        print(f"   ❌ {table_name} - {e}")
    
    time.sleep(0.2)

print("""
\n✅ CONFIGURATION TERMINÉE

🎯 Paramètres importants :
   - allow_run_async: False (pas de workers distants)
   - Backend: PostgreSQL
   - URI: postgresql://admin:admin123@172.18.0.3:5432/healthcare_data

📊 Testez maintenant dans Superset :
   1. SQL Lab → Exécuter: SELECT * FROM kpi_synthese_globale;
   2. Charts → Créer vos visualisations
""")
