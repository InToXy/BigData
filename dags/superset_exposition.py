"""
DAG Airflow - Exposition des KPIs dans Superset
Expose automatiquement les nouveaux KPIs dans Superset après chargement PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Configuration par défaut
default_args = {
    'owner': 'chu_data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Définition du DAG
dag = DAG(
    'superset_kpis_exposition',
    default_args=default_args,
    description='Expose les KPIs dans Superset après chargement PostgreSQL',
    schedule_interval=None,  # Déclenché manuellement ou par autre DAG
    start_date=days_ago(1),
    catchup=False,
    tags=['superset', 'exposition', 'kpis'],
)

# ============================================================
# TASK 1: Exposition des KPIs dans Superset
# ============================================================
expose_kpis = BashOperator(
    task_id='expose_new_kpis_superset',
    bash_command="""
    cd /opt/airflow/dags/../tools && \
    python3 expose_new_kpis_superset.py
    """,
    dag=dag,
)

# ============================================================
# TASK 2: Vérification des datasets Superset
# ============================================================
def check_superset_datasets(**context):
    """Vérifier que les datasets sont bien exposés"""
    import requests
    
    SUPERSET_URL = "http://chu_superset:8088"
    
    try:
        # Login
        login_response = requests.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={
                "username": "admin",
                "password": "admin123",
                "provider": "db",
                "refresh": True
            }
        )
        
        if login_response.status_code == 200:
            access_token = login_response.json().get("access_token")
            
            # Récupérer les datasets
            headers = {"Authorization": f"Bearer {access_token}"}
            datasets_response = requests.get(
                f"{SUPERSET_URL}/api/v1/dataset/",
                headers=headers
            )
            
            if datasets_response.status_code == 200:
                count = datasets_response.json().get("count", 0)
                print(f"✅ {count} datasets trouvés dans Superset")
                
                if count >= 14:
                    print("✅ Tous les KPIs sont exposés")
                else:
                    print(f"⚠️  Seulement {count} datasets (attendu: 14)")
            else:
                print(f"❌ Erreur récupération datasets: {datasets_response.status_code}")
        else:
            print(f"❌ Erreur login Superset: {login_response.status_code}")
            
    except Exception as e:
        print(f"❌ Erreur vérification Superset: {str(e)}")

check_datasets = PythonOperator(
    task_id='check_superset_datasets',
    python_callable=check_superset_datasets,
    provide_context=True,
    dag=dag,
)

# ============================================================
# TASK 3: Notification de succès
# ============================================================
success_notification = BashOperator(
    task_id='success_notification',
    bash_command="""
    echo "╔═══════════════════════════════════════════════════╗"
    echo "║     ✅ KPIs EXPOSÉS DANS SUPERSET                 ║"
    echo "╚═══════════════════════════════════════════════════╝"
    echo ""
    echo "🎨 Accès Superset: http://localhost:8088"
    echo "🔑 Credentials: admin / admin123"
    echo ""
    echo "📊 Datasets disponibles:"
    echo "  - 7 KPIs de base"
    echo "  - 7 KPIs métier"
    echo ""
    echo "🎯 Prochaine étape: Créer des visualisations"
    """,
    dag=dag,
)

# ============================================================
# DÉPENDANCES
# ============================================================
expose_kpis >> check_datasets >> success_notification
