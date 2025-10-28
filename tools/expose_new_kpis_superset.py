#!/usr/bin/env python3
"""
Script pour exposer les 7 nouveaux KPIs métier dans Superset
En complément des 7 KPIs déjà exposés
"""

import requests
import json
import time

# Configuration Superset
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"
DATABASE_ID = 1  # ID de la base "Healthcare Gold Data"

# Les 7 nouveaux KPIs métier à exposer
NEW_KPIS = [
    {
        "table_name": "kpi_consultation_etablissement",
        "schema": "public",
        "description": "Taux de consultation par établissement - Nombre de consultations par raison sociale et commune"
    },
    {
        "table_name": "kpi_consultation_professionnel",
        "schema": "public",
        "description": "Taux de consultation par professionnel - Consultations par profession et catégorie"
    },
    {
        "table_name": "kpi_hospitalisation_globale",
        "schema": "public",
        "description": "Taux global d'hospitalisation - Statistiques globales des hospitalisations"
    },
    {
        "table_name": "kpi_hospitalisation_sexe_age",
        "schema": "public",
        "description": "Hospitalisation par démographie - Taux par sexe et catégorie d'âge"
    },
    {
        "table_name": "kpi_deces_region_2019",
        "schema": "public",
        "description": "Décès par région 2019 - Nombre de décès par code département pour l'année 2019"
    },
    {
        "table_name": "kpi_satisfaction_region",
        "schema": "public",
        "description": "Satisfaction par région - Taux de satisfaction moyen par région"
    },
    {
        "table_name": "kpi_consultations_synthese",
        "schema": "public",
        "description": "Synthèse des consultations - Vue d'ensemble des consultations par période"
    }
]

def get_csrf_token(session):
    """Récupérer le token CSRF"""
    response = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    if response.status_code == 200:
        return response.json().get("result")
    return None

def login():
    """Se connecter à Superset et retourner la session"""
    session = requests.Session()
    
    # Login
    login_data = {
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }
    
    response = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json=login_data
    )
    
    if response.status_code == 200:
        # Récupérer le token d'accès
        access_token = response.json().get("access_token")
        if access_token:
            session.headers.update({
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            })
        
        # Récupérer le token CSRF
        csrf_token = get_csrf_token(session)
        if csrf_token:
            session.headers.update({
                "X-CSRFToken": csrf_token,
                "Referer": SUPERSET_URL
            })
        return session
    else:
        raise Exception(f"Login failed: {response.text}")

def expose_dataset(session, table_info):
    """Exposer une table comme dataset"""
    dataset_data = {
        "database": DATABASE_ID,
        "schema": table_info["schema"],
        "table_name": table_info["table_name"]
    }
    
    response = session.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json=dataset_data
    )
    
    if response.status_code == 201:
        dataset_id = response.json().get("id")
        print(f"   ✅ Dataset créé (ID: {dataset_id})")
        return dataset_id
    elif response.status_code == 422:
        # Dataset existe déjà
        print(f"   ℹ️  Dataset existe déjà")
        return None
    else:
        print(f"   ❌ Erreur: {response.status_code} - {response.text}")
        return None

def main():
    print("""
╔═══════════════════════════════════════════╗
║   EXPOSITION NOUVEAUX KPIs - SUPERSET     ║
║         7 KPIs Métier                     ║
╚═══════════════════════════════════════════╝
""")
    
    try:
        # Se connecter
        print("🔐 Connexion à Superset...")
        session = login()
        print("   ✅ Connecté\n")
        
        # Exposer chaque KPI
        success_count = 0
        for kpi in NEW_KPIS:
            print(f"📊 {kpi['table_name']}")
            print(f"   {kpi['description']}")
            
            dataset_id = expose_dataset(session, kpi)
            if dataset_id or dataset_id is None:
                success_count += 1
            
            time.sleep(0.5)  # Pause entre les requêtes
        
        print()
        print("=" * 50)
        print(f"✅ {success_count}/{len(NEW_KPIS)} KPIs exposés dans Superset")
        print()
        print("📋 Vérification:")
        print(f"   Ouvrez: {SUPERSET_URL}/tablemodelview/list/")
        print("   Vous devriez voir 14 datasets au total:")
        print("   - 7 KPIs de base (déjà exposés)")
        print("   - 7 KPIs métier (nouveaux)")
        print()
        print("🎨 Prochaines étapes:")
        print("   1. Créer des visualisations pour les nouveaux KPIs")
        print("   2. Assembler un dashboard 'Analyse Métier Santé 2019'")
        
    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
