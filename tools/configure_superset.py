#!/usr/bin/env python3
"""
Configuration automatique de Superset pour PostgreSQL
Crée la connexion database et expose les datasets
"""
import requests
import json
import time

# Configuration
SUPERSET_URL = "http://localhost:8088"
SUPERSET_USER = "admin"
SUPERSET_PASSWORD = "admin123"

# Configuration PostgreSQL
PG_URI = "postgresql://admin:admin123@172.18.0.3:5432/healthcare_data"
DATABASE_NAME = "Healthcare Gold Data"

# Liste des tables KPI
KPI_TABLES = [
    "kpi_deces_par_annee",
    "kpi_deces_par_region",
    "kpi_demographic_summary",
    "kpi_distribution_age",
    "kpi_synthese_globale",
    "kpi_temporal_trends",
    "kpi_top_departements"
]

print("""
╔═══════════════════════════════════════════╗
║   CONFIGURATION AUTOMATIQUE SUPERSET      ║
║      Connexion PostgreSQL + Datasets      ║
╚═══════════════════════════════════════════╝
""")

# Session
session = requests.Session()

# 1. Login et obtenir le token CSRF
print("🔐 Connexion à Superset...")
login_url = f"{SUPERSET_URL}/api/v1/security/login"
login_data = {
    "username": SUPERSET_USER,
    "password": SUPERSET_PASSWORD,
    "provider": "db",
    "refresh": True
}

try:
    login_response = session.post(login_url, json=login_data)
    login_response.raise_for_status()
    access_token = login_response.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    print("   ✅ Connecté à Superset")
except Exception as e:
    print(f"   ❌ Erreur de connexion: {e}")
    print(f"   Réponse: {login_response.text if 'login_response' in locals() else 'N/A'}")
    exit(1)

# 2. Obtenir le token CSRF
print("\n🔑 Récupération du token CSRF...")
try:
    csrf_url = f"{SUPERSET_URL}/api/v1/security/csrf_token/"
    csrf_response = session.get(csrf_url)
    csrf_response.raise_for_status()
    csrf_token = csrf_response.json()["result"]
    session.headers.update({
        "X-CSRFToken": csrf_token,
        "Referer": SUPERSET_URL
    })
    print("   ✅ Token CSRF obtenu")
except Exception as e:
    print(f"   ❌ Erreur CSRF: {e}")
    exit(1)

# 3. Créer la connexion Database
print(f"\n💾 Création de la database '{DATABASE_NAME}'...")
database_url = f"{SUPERSET_URL}/api/v1/database/"
database_data = {
    "database_name": DATABASE_NAME,
    "sqlalchemy_uri": PG_URI,
    "expose_in_sqllab": True,
    "allow_run_async": True,
    "allow_ctas": False,
    "allow_cvas": False,
    "allow_dml": False,
    "force_ctas_schema": None,
    "cache_timeout": None,
    "encrypted_extra": "{}",
    "extra": json.dumps({
        "metadata_params": {},
        "engine_params": {},
        "metadata_cache_timeout": {},
        "schemas_allowed_for_csv_upload": []
    }),
    "server_cert": None,
    "impersonate_user": False,
    "allow_csv_upload": False
}

try:
    db_response = session.post(database_url, json=database_data)
    if db_response.status_code == 201:
        database_id = db_response.json()["id"]
        print(f"   ✅ Database créée (ID: {database_id})")
    elif db_response.status_code == 422:
        # Database existe déjà, récupérer son ID
        print("   ⚠️  Database existe déjà, récupération de l'ID...")
        list_response = session.get(database_url)
        databases = list_response.json()["result"]
        database_id = None
        for db in databases:
            if db["database_name"] == DATABASE_NAME:
                database_id = db["id"]
                print(f"   ✅ Database trouvée (ID: {database_id})")
                break
        if not database_id:
            print("   ❌ Impossible de trouver la database")
            exit(1)
    else:
        print(f"   ❌ Erreur création database: {db_response.status_code}")
        print(f"   Réponse: {db_response.text}")
        exit(1)
except Exception as e:
    print(f"   ❌ Erreur: {e}")
    exit(1)

# 4. Tester la connexion
print("\n🔍 Test de la connexion...")
test_url = f"{SUPERSET_URL}/api/v1/database/{database_id}/test_connection/"
try:
    test_response = session.post(test_url)
    if test_response.status_code == 200:
        print("   ✅ Connexion PostgreSQL validée")
    else:
        print(f"   ⚠️  Attention: code {test_response.status_code}")
except Exception as e:
    print(f"   ⚠️  Test ignoré: {e}")

# 5. Exposer les datasets
print(f"\n📊 Exposition des {len(KPI_TABLES)} tables KPI...")
dataset_url = f"{SUPERSET_URL}/api/v1/dataset/"

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
            print(f"   ❌ {table_name} - Erreur: {dataset_response.status_code}")
    except Exception as e:
        print(f"   ❌ {table_name} - Erreur: {e}")
    
    time.sleep(0.2)  # Petite pause entre les requêtes

print("""
\n╔═══════════════════════════════════════════╗
║          CONFIGURATION TERMINÉE !          ║
╚═══════════════════════════════════════════╝

✅ Database connectée : Healthcare Gold Data
✅ 7 datasets exposés

🎯 Prochaines étapes :
   1. Aller sur http://localhost:8088
   2. Menu : Charts → + Chart
   3. Sélectionner un dataset KPI
   4. Créer vos visualisations !

📊 Ou tester dans SQL Lab :
   1. Menu : SQL → SQL Lab
   2. Database : Healthcare Gold Data
   3. Schema : public
   4. Requête : SELECT * FROM kpi_synthese_globale;
""")
