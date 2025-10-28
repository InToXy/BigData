#!/usr/bin/env python3
"""
Script pour rafraîchir le schéma du dataset kpi_consultation_etablissement
et vérifier que la colonne commune est bien présente
"""

import requests
import json

# Configuration Superset
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"

print("""
╔═══════════════════════════════════════════╗
║  RAFRAÎCHIR kpi_consultation_etablissement║
║         Mise à jour schéma commune        ║
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
        "Content-Type": "application/json"
    })
    print("   ✅ Connecté\n")
except Exception as e:
    print(f"   ❌ Erreur connexion: {e}")
    exit(1)

# 2. Chercher le dataset kpi_consultation_etablissement
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
    
    dataset = datasets[0]
    dataset_id = dataset["id"]
    print(f"   ✅ Dataset trouvé (ID: {dataset_id})")
    print(f"   📋 Colonnes actuelles: {len(dataset.get('columns', []))}")
    
    # Afficher les colonnes
    for col in dataset.get('columns', [])[:10]:
        print(f"      - {col.get('column_name')}")
    
except Exception as e:
    print(f"   ❌ Erreur recherche: {e}")
    exit(1)

# 3. Rafraîchir le schéma
print(f"\n🔄 Rafraîchissement du schéma...")
try:
    resp = session.put(
        f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}/refresh"
    )
    resp.raise_for_status()
    print("   ✅ Schéma rafraîchi")
except Exception as e:
    print(f"   ⚠️  Endpoint refresh non disponible: {e}")
    print("   Tentative alternative...")
    
    # Alternative: GET le dataset pour récupérer les colonnes actuelles
    try:
        resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}")
        resp.raise_for_status()
        current_data = resp.json()["result"]
        
        # Forcer la mise à jour
        update_payload = {
            "table_name": "kpi_consultation_etablissement"
        }
        
        resp = session.put(
            f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}",
            json=update_payload
        )
        resp.raise_for_status()
        print("   ✅ Dataset mis à jour")
    except Exception as e2:
        print(f"   ❌ Erreur mise à jour: {e2}")

# 4. Récupérer les colonnes mises à jour
print(f"\n✅ Vérification post-rafraîchissement...")
try:
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}")
    resp.raise_for_status()
    updated_dataset = resp.json()["result"]
    
    columns = updated_dataset.get("columns", [])
    column_names = [col["column_name"] for col in columns]
    
    print(f"   📊 Colonnes disponibles ({len(columns)}):")
    for col_name in sorted(column_names):
        print(f"      - {col_name}")
    
    if "commune" in column_names:
        print(f"\n   ✅ Colonne 'commune' PRÉSENTE !")
    else:
        print(f"\n   ⚠️  Colonne 'commune' ABSENTE")
        print("   💡 Solution: Dans Superset UI, aller dans:")
        print("      Data > Datasets > kpi_consultation_etablissement")
        print("      Onglet 'Columns' > Click 'Sync columns from source'")
    
except Exception as e:
    print(f"   ❌ Erreur vérification: {e}")

print("\n✅ TERMINÉ")
print("🌐 Accédez à Superset: http://172.28.168.129:8088")
print("📊 Vérifiez le dataset: Data > Datasets > kpi_consultation_etablissement")
