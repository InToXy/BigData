#!/usr/bin/env python3
"""
Force la synchronisation complète des colonnes du dataset kpi_consultation_etablissement
"""

import requests
import json

# Configuration Superset
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"

print("""
╔═══════════════════════════════════════════╗
║    SYNCHRONISATION FORCÉE DES COLONNES    ║
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
        "Content-Type": "application/json"
    })
    print("   ✅ Connecté\n")
except Exception as e:
    print(f"   ❌ Erreur connexion: {e}")
    exit(1)

# 2. Chercher le dataset
print("🔍 Recherche dataset...")
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
    
except Exception as e:
    print(f"   ❌ Erreur recherche: {e}")
    exit(1)

# 3. Supprimer toutes les anciennes colonnes
print(f"\n🗑️  Nettoyage des anciennes colonnes...")
try:
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}")
    resp.raise_for_status()
    current_data = resp.json()["result"]
    
    old_columns = current_data.get("columns", [])
    print(f"   Anciennes colonnes trouvées: {len(old_columns)}")
    
    for col in old_columns:
        col_id = col["id"]
        try:
            resp = session.delete(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}/column/{col_id}")
            print(f"   🗑️  Supprimé: {col['column_name']}")
        except Exception as e:
            print(f"   ⚠️  Impossible de supprimer {col['column_name']}: {e}")
    
except Exception as e:
    print(f"   ⚠️  Erreur nettoyage: {e}")

# 4. Forcer la synchronisation depuis la source
print(f"\n🔄 Synchronisation forcée depuis PostgreSQL...")
try:
    # Utiliser l'endpoint pour rafraîchir les métadonnées
    resp = session.put(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}/refresh")
    
    if resp.status_code in [200, 201]:
        print("   ✅ Synchronisation réussie via /refresh")
    else:
        # Alternative: recréer le dataset
        print("   ⚠️  Endpoint /refresh non disponible, utilisation alternative...")
        
        # Forcer une mise à jour du dataset
        update_payload = {
            "table_name": "kpi_consultation_etablissement",
            "schema": "public"
        }
        
        resp = session.put(
            f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}",
            json=update_payload
        )
        resp.raise_for_status()
        print("   ✅ Dataset mis à jour")
        
except Exception as e:
    print(f"   ⚠️  Erreur synchronisation: {e}")

# 5. Vérifier les colonnes synchronisées
print(f"\n✅ Vérification des colonnes...")
try:
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}")
    resp.raise_for_status()
    updated_dataset = resp.json()["result"]
    
    columns = updated_dataset.get("columns", [])
    column_names = [col["column_name"] for col in columns]
    
    print(f"\n   📊 Colonnes synchronisées ({len(columns)}):")
    for col_name in sorted(column_names):
        print(f"      ✓ {col_name}")
    
    if "commune" in column_names:
        print(f"\n   ✅✅✅ Colonne 'commune' PRÉSENTE ! ✅✅✅")
    else:
        print(f"\n   ❌ Colonne 'commune' toujours ABSENTE")
        print("\n   💡 Solution manuelle requise:")
        print("      1. Ouvrez Superset: http://172.28.168.129:8088")
        print("      2. Allez dans: Data > Datasets")
        print("      3. Cliquez sur 'kpi_consultation_etablissement'")
        print("      4. Onglet 'Columns'")
        print("      5. Cliquez sur 'Sync columns from source'")
        print("      6. Sauvegardez")
    
except Exception as e:
    print(f"   ❌ Erreur vérification: {e}")

# 6. Tester une requête SQL directe
print(f"\n🔍 Test requête SQL directe...")
try:
    # Créer une requête SQL pour vérifier
    sql_payload = {
        "database_id": 1,
        "sql": "SELECT raison_sociale_site, commune, nombre_consultations FROM kpi_consultation_etablissement WHERE commune IS NOT NULL LIMIT 5",
        "schema": "public"
    }
    
    resp = session.post(
        f"{SUPERSET_URL}/superset/sql_json/",
        json=sql_payload
    )
    
    if resp.status_code == 200:
        result = resp.json()
        print("   ✅ Requête SQL réussie:")
        print(f"   📋 Résultat: {result.get('data', [])[:2]}")
    else:
        print(f"   ⚠️  Requête SQL status: {resp.status_code}")
        
except Exception as e:
    print(f"   ⚠️  Erreur requête SQL: {e}")

print("\n✅ TERMINÉ")
print("\n🌐 Accédez à Superset: http://172.28.168.129:8088")
print("📊 Dataset: Data > Datasets > kpi_consultation_etablissement")
