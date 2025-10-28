#!/usr/bin/env python3
"""
Supprimer et recréer le dataset kpi_consultation_etablissement
pour forcer la prise en compte de la colonne commune
"""

import requests
import json
import time

# Configuration Superset
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"
DATABASE_ID = 1  # Healthcare Gold Data

print("""
╔═══════════════════════════════════════════╗
║   RECRÉATION COMPLÈTE DU DATASET          ║
║       kpi_consultation_etablissement      ║
╚═══════════════════════════════════════════╝
""")

# Session
session = requests.Session()

# 1. Login
print("🔐 Connexion à Superset...")
login_data = {
    "username": USERNAME,
    "password": PASSWORD,
    "provider": "db"
}

try:
    resp = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=login_data)
    resp.raise_for_status()
    token_data = resp.json()
    access_token = token_data.get("access_token")
    
    session.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-CSRFToken": session.cookies.get("csrf_access_token", "")
    })
    print("   ✅ Connecté\n")
except Exception as e:
    print(f"   ❌ Erreur connexion: {e}")
    exit(1)

# 2. Chercher et supprimer l'ancien dataset
print("🔍 Recherche de l'ancien dataset...")
try:
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": "kpi_consultation_etablissement"}]})}
    )
    resp.raise_for_status()
    datasets = resp.json()["result"]
    
    if datasets:
        old_dataset_id = datasets[0]["id"]
        print(f"   📋 Ancien dataset trouvé (ID: {old_dataset_id})")
        
        # Supprimer
        print("   🗑️  Suppression de l'ancien dataset...")
        resp = session.delete(f"{SUPERSET_URL}/api/v1/dataset/{old_dataset_id}")
        
        if resp.status_code in [200, 204]:
            print("   ✅ Dataset supprimé")
        else:
            print(f"   ⚠️  Status suppression: {resp.status_code}")
    else:
        print("   ℹ️  Aucun ancien dataset trouvé")
        
except Exception as e:
    print(f"   ⚠️  Erreur recherche/suppression: {e}")

time.sleep(2)

# 3. Créer le nouveau dataset
print("\n🆕 Création du nouveau dataset...")
new_dataset_payload = {
    "database": DATABASE_ID,
    "schema": "public",
    "table_name": "kpi_consultation_etablissement"
}

try:
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json=new_dataset_payload
    )
    
    if resp.status_code in [200, 201]:
        result = resp.json()
        new_dataset_id = result.get("id")
        print(f"   ✅ Dataset créé (ID: {new_dataset_id})")
    else:
        print(f"   ❌ Erreur création: {resp.status_code}")
        print(f"   Réponse: {resp.text[:200]}")
        exit(1)
        
except Exception as e:
    print(f"   ❌ Erreur création: {e}")
    exit(1)

time.sleep(2)

# 4. Vérifier les colonnes du nouveau dataset
print("\n✅ Vérification du nouveau dataset...")
try:
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/{new_dataset_id}")
    resp.raise_for_status()
    dataset_data = resp.json()["result"]
    
    columns = dataset_data.get("columns", [])
    column_names = [col["column_name"] for col in columns]
    
    print(f"   📊 Colonnes détectées ({len(columns)}):")
    for col_name in sorted(column_names):
        col_data = next((c for c in columns if c["column_name"] == col_name), {})
        col_type = col_data.get("type", "unknown")
        is_dttm = col_data.get("is_dttm", False)
        print(f"      ✓ {col_name:30} | Type: {col_type:15} | DateTime: {is_dttm}")
    
    if "commune" in column_names:
        print(f"\n   ✅✅✅ Colonne 'commune' DÉTECTÉE ! ✅✅✅")
        
        # Vérifier le type de la colonne commune
        commune_col = next((c for c in columns if c["column_name"] == "commune"), None)
        if commune_col:
            print(f"   📋 Détails colonne commune:")
            print(f"      - Type: {commune_col.get('type')}")
            print(f"      - Type générique: {commune_col.get('type_generic')}")
            print(f"      - Filtrable: {commune_col.get('filterable')}")
            print(f"      - Groupable: {commune_col.get('groupby')}")
    else:
        print(f"\n   ❌ Colonne 'commune' NON DÉTECTÉE")
        print("   ⚠️  Problème de synchronisation avec PostgreSQL")
    
except Exception as e:
    print(f"   ❌ Erreur vérification: {e}")

# 5. Test de requête sur le dataset
print(f"\n🔍 Test de requête sur les données...")
try:
    # Requête SQL directe via SQL Lab
    sql_lab_payload = {
        "database_id": DATABASE_ID,
        "sql": """
            SELECT 
                raison_sociale_site,
                commune,
                nombre_consultations
            FROM kpi_consultation_etablissement 
            WHERE commune IS NOT NULL 
            ORDER BY nombre_consultations DESC 
            LIMIT 5
        """,
        "schema": "public"
    }
    
    # Note: l'endpoint SQL peut varier selon la version de Superset
    # Essayons différents endpoints
    endpoints = [
        f"{SUPERSET_URL}/superset/sql_json/",
        f"{SUPERSET_URL}/api/v1/sqllab/execute/"
    ]
    
    for endpoint in endpoints:
        try:
            resp = session.post(endpoint, json=sql_lab_payload)
            if resp.status_code == 200:
                result = resp.json()
                data = result.get("data", [])
                if data:
                    print(f"   ✅ Requête SQL réussie via {endpoint.split('/')[-2]}")
                    print(f"   📋 Échantillon de données:")
                    for row in data[:3]:
                        print(f"      {row}")
                    break
        except:
            continue
    else:
        print("   ⚠️  Impossible de tester la requête SQL (endpoints non disponibles)")
        
except Exception as e:
    print(f"   ⚠️  Erreur test requête: {e}")

# 6. Instructions finales
print(f"""

╔═══════════════════════════════════════════╗
║             DATASET RECRÉÉ                ║
╚═══════════════════════════════════════════╝

✅ Dataset 'kpi_consultation_etablissement' recréé avec succès
📊 ID du nouveau dataset: {new_dataset_id}

🎯 VÉRIFICATION MANUELLE RECOMMANDÉE:

1. Ouvrez Superset: http://172.28.168.129:8088
   Login: admin / admin123

2. Allez dans SQL Lab > SQL Editor
   Database: Healthcare Gold Data
   
   Testez cette requête:
   ════════════════════════════════════════
   SELECT 
       raison_sociale_site,
       commune,
       nombre_consultations
   FROM kpi_consultation_etablissement 
   WHERE commune IS NOT NULL 
   ORDER BY nombre_consultations DESC 
   LIMIT 10;
   ════════════════════════════════════════
   
   ✅ ATTENDU: Vous devriez voir Nantes, Paris, Montpellier, etc.
   ❌ SI NULL: Problème de connexion PostgreSQL

3. Créez un chart de test:
   Charts > + Chart
   Dataset: kpi_consultation_etablissement
   Viz: Table
   Columns: raison_sociale_site, commune, nombre_consultations
   Filters: commune IS NOT NULL
   
   ✅ La colonne commune doit être dans la liste des dimensions

4. Si commune est toujours NULL dans Superset:
   - Vérifiez la connexion database: Data > Databases > Healthcare Gold Data > Test Connection
   - Vérifiez les permissions: Settings > Database Connections
   - Essayez de recréer la connexion database

📝 NOTE: Les données sont CONFIRMÉES dans PostgreSQL (271,920 communes non-NULL).
Si Superset montre NULL, c'est un problème de configuration Superset, pas de données.
""")

print("\n✅ TERMINÉ")
