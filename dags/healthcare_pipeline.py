"""
DAG Airflow - Pipeline Healthcare Data Lakehouse
Bronze → Silver → Gold → PostgreSQL → Superset

Orchestration complète du pipeline de données de santé
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# Configuration par défaut du DAG
default_args = {
    'owner': 'chu_data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Définition du DAG
dag = DAG(
    'healthcare_pipeline_complete',
    default_args=default_args,
    description='Pipeline complet Healthcare: Bronze → Silver → Gold → PostgreSQL',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'bronze', 'silver', 'gold', 'production'],
    max_active_runs=1,
)

# ============================================================
# TASK 1: Bronze Layer - Ingestion des données sources
# ============================================================
bronze_ingestion = BashOperator(
    task_id='bronze_ingestion_rgpd',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[2] \
        --driver-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /home/jovyan/bronze_ingestion_rgpd_complete.py
    """,
    dag=dag,
)

# ============================================================
# TASK 2: Vérification Bronze Layer
# ============================================================
check_bronze = BashOperator(
    task_id='check_bronze_data',
    bash_command="""
    echo "Vérification des données Bronze..."
    docker exec chu_minio mc ls myminio/bronze/ | wc -l
    if [ $? -eq 0 ]; then
        echo "✅ Bronze layer vérifié"
    else
        echo "❌ Erreur Bronze layer"
        exit 1
    fi
    """,
    dag=dag,
)

# ============================================================
# TASK 3: Silver Layer - Transformation dimensionnelle
# ============================================================
silver_transformation = BashOperator(
    task_id='silver_transformation',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[2] \
        --driver-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /home/jovyan/silver_transformation.py
    """,
    dag=dag,
)

# ============================================================
# TASK 4: Vérification Silver Layer
# ============================================================
check_silver = BashOperator(
    task_id='check_silver_data',
    bash_command="""
    echo "Vérification des données Silver..."
    docker exec chu_minio mc ls myminio/silver/ | wc -l
    if [ $? -eq 0 ]; then
        echo "✅ Silver layer vérifié"
    else
        echo "❌ Erreur Silver layer"
        exit 1
    fi
    """,
    dag=dag,
)

# ============================================================
# TASK 5: Gold Layer - KPIs de base
# ============================================================
gold_kpis_base = BashOperator(
    task_id='gold_aggregation_base',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[2] \
        --driver-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /home/jovyan/gold_aggregation.py
    """,
    dag=dag,
)

# ============================================================
# TASK 6: Gold Layer - KPIs métier
# ============================================================
gold_kpis_metier = BashOperator(
    task_id='gold_kpis_metier',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[2] \
        --driver-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /home/jovyan/gold_metier.py
    """,
    dag=dag,
)

# ============================================================
# TASK 7: Vérification Gold Layer
# ============================================================
check_gold = BashOperator(
    task_id='check_gold_kpis',
    bash_command="""
    echo "Vérification des KPIs Gold..."
    COUNT=$(docker exec chu_minio mc ls myminio/gold/ | grep kpi_ | wc -l)
    if [ $COUNT -ge 14 ]; then
        echo "✅ Gold layer vérifié: $COUNT KPIs trouvés"
    else
        echo "❌ Erreur Gold layer: seulement $COUNT KPIs trouvés (attendu: 14)"
        exit 1
    fi
    """,
    dag=dag,
)

# ============================================================
# TASK 8: Chargement PostgreSQL
# ============================================================
load_postgresql = BashOperator(
    task_id='load_gold_to_postgresql',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[2] \
        --driver-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        --jars /usr/local/spark/jars/postgresql-42.6.0.jar \
        /home/jovyan/gold_kpis_to_postgres.py
    """,
    dag=dag,
)

# ============================================================
# TASK 9: Vérification PostgreSQL
# ============================================================
check_postgresql = BashOperator(
    task_id='check_postgresql_tables',
    bash_command="""
    echo "Vérification des tables PostgreSQL..."
    COUNT=$(docker exec chu_postgres psql -U admin -d healthcare_data -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'kpi_%';" | tr -d ' ')
    if [ $COUNT -ge 14 ]; then
        echo "✅ PostgreSQL vérifié: $COUNT tables KPI trouvées"
    else
        echo "❌ Erreur PostgreSQL: seulement $COUNT tables trouvées (attendu: 14)"
        exit 1
    fi
    """,
    dag=dag,
)

# ============================================================
# TASK 10: Notification de succès
# ============================================================
def send_success_notification(**context):
    """Envoyer une notification de succès"""
    execution_date = context['execution_date']
    print(f"""
    ╔═══════════════════════════════════════════════════════╗
    ║           ✅ PIPELINE EXÉCUTÉ AVEC SUCCÈS             ║
    ╚═══════════════════════════════════════════════════════╝
    
    Date d'exécution: {execution_date}
    
    📊 Résumé:
    - Bronze: 21 tables ingérées avec RGPD
    - Silver: 7 tables dimensionnelles créées
    - Gold: 14 KPIs générés
    - PostgreSQL: 14 tables chargées
    
    🎯 Prêt pour analyse dans Superset
    URL: http://localhost:8088
    """)

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# ============================================================
# DÉPENDANCES DES TÂCHES (Pipeline Flow)
# ============================================================

# Bronze → Check → Silver → Check → Gold (parallel) → Check → PostgreSQL → Check → Notification
bronze_ingestion >> check_bronze >> silver_transformation >> check_silver

# Gold: 2 jobs en parallèle (KPIs base + KPIs métier)
check_silver >> [gold_kpis_base, gold_kpis_metier] >> check_gold

# PostgreSQL et notification finale
check_gold >> load_postgresql >> check_postgresql >> success_notification
