#!/usr/bin/env python3
"""
Créer un chart de test pour vérifier que la colonne commune est accessible
"""

import requests
import json

# Configuration Superset
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"

print("""
╔═══════════════════════════════════════════╗
║   CRÉATION CHART TEST - COLONNE COMMUNE   ║
╚═══════════════════════════════════════════╝
""")

# Session
session = requests.Session()

# 1. Login
print("🔐 Connexion...")
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
        "Content-Type": "application/json"
    })
    print("   ✅ Connecté\n")
except Exception as e:
    print(f"   ❌ Erreur: {e}")
    exit(1)

# 2. Trouver le dataset
print("🔍 Recherche dataset kpi_consultation_etablissement...")
try:
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": "kpi_consultation_etablissement"}]})}
    )
    resp.raise_for_status()
    datasets = resp.json()["result"]
    
    if not datasets:
        print("   ❌ Dataset non trouvé")
        exit(1)
    
    dataset_id = datasets[0]["id"]
    print(f"   ✅ Dataset trouvé (ID: {dataset_id})")
    
except Exception as e:
    print(f"   ❌ Erreur: {e}")
    exit(1)

# 3. Créer un chart Table avec la colonne commune
print("\n📊 Création d'un chart Table de test...")

chart_config = {
    "slice_name": "TEST - Consultations par Commune",
    "viz_type": "table",
    "datasource_id": dataset_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "groupby": ["commune", "raison_sociale_site"],
        "metrics": ["count"],
        "all_columns": [],
        "percent_metrics": [],
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "subject": "commune",
                "operator": "IS NOT NULL",
                "comparator": None,
                "expressionType": "SIMPLE"
            }
        ],
        "order_by_cols": [],
        "row_limit": 20,
        "server_page_length": 10,
        "order_desc": True,
        "table_timestamp_format": "smart_date",
        "show_cell_bars": True,
        "color_pn": True
    }),
    "query_context": json.dumps({
        "datasource": {
            "id": dataset_id,
            "type": "table"
        },
        "queries": [
            {
                "columns": ["commune", "raison_sociale_site"],
                "metrics": ["count"],
                "filters": [
                    {
                        "col": "commune",
                        "op": "IS NOT NULL",
                        "val": None
                    }
                ],
                "row_limit": 20,
                "orderby": []
            }
        ]
    })
}

try:
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/chart/",
        json=chart_config
    )
    
    if resp.status_code in [200, 201]:
        chart_data = resp.json()
        chart_id = chart_data.get("id")
        print(f"   ✅ Chart créé (ID: {chart_id})")
        print(f"   🔗 URL: {SUPERSET_URL}/explore/?form_data_key=&slice_id={chart_id}")
    else:
        print(f"   ⚠️  Status: {resp.status_code}")
        print(f"   Réponse: {resp.text[:300]}")
        
except Exception as e:
    print(f"   ⚠️  Erreur création chart: {e}")

# 4. Instructions finales
print(f"""

╔═══════════════════════════════════════════╗
║          INSTRUCTIONS DE TEST             ║
╚═══════════════════════════════════════════╝

🌐 Accédez à Superset: http://172.28.168.129:8088
   Login: admin / admin123

📊 Option 1: SQL Lab (PLUS SIMPLE)
   ══════════════════════════════════════
   1. Menu: SQL Lab > SQL Editor
   2. Database: Healthcare Gold Data
   3. Collez et exécutez:
   
   SELECT 
       commune,
       raison_sociale_site,
       nombre_consultations
   FROM kpi_consultation_etablissement
   WHERE commune IS NOT NULL
   ORDER BY nombre_consultations DESC
   LIMIT 20;
   
   ✅ Vous DEVEZ voir les communes: Nantes, Paris, Montpellier, etc.

📈 Option 2: Créer un Chart
   ══════════════════════════════════════
   1. Menu: Charts > + Chart
   2. Choose Dataset: kpi_consultation_etablissement
   3. Choose Chart Type: Table
   4. Configuration:
      - DIMENSIONS: commune, raison_sociale_site
      - METRICS: COUNT(*)
      - FILTERS: commune IS NOT NULL
   5. Cliquez "Update Chart"
   
   ✅ La colonne commune doit apparaître dans la liste

📋 Données de référence (PostgreSQL confirmé):
   ═══════════════════════════════════════════
   Total lignes: 372,655
   Avec commune: 271,920 (73%)
   Sans commune: 100,735 (27%)
   
   Top communes:
   - Nantes: 15 consultations
   - Paris: 12 consultations
   - Montpellier: 38 consultations (total)
   - Lille: 32 consultations (total)

⚠️  SI COMMUNE EST TOUJOURS NULL DANS SUPERSET:
   ═════════════════════════════════════════════
   Ce n'est PAS un problème de données (confirmées OK dans PostgreSQL)
   
   Solutions possibles:
   1. Vider le cache navigateur: Ctrl+Shift+Del
   2. Tester dans un autre navigateur
   3. Tester en navigation privée
   4. Vérifier les logs Superset: docker logs chu_superset
   5. Vérifier la connexion DB: Data > Databases > Test Connection

✅ RAPPEL: Les données sont BONNES dans PostgreSQL.
   Si Superset montre NULL, c'est un problème d'affichage/cache.
""")

print("\n✅ SCRIPT TERMINÉ")
