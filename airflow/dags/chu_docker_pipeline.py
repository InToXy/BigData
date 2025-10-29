
"""
DAG Airflow PRODUCTION FINAL CHU
Solution robuste utilisant DockerOperator pour exécuter les notebooks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Configuration par défaut du DAG
default_args = {
    'owner': 'chu-bigdata',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'chu_docker_pipeline',
    default_args=default_args,
    description='Pipeline ETL CHU utilisant DockerOperator',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    max_active_runs=1,
    tags=['chu', 'etl', 'production', 'docker']
)

start_task = DummyOperator(
    task_id='start_production_pipeline',
    dag=dag
)

# Tâches pour chaque couche (Bronze, Silver, Gold)
bronze_task = DockerOperator(
    task_id='bronze_ingestion_docker',
    image='jupyter/pyspark-notebook:latest',
    command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 /home/jovyan/jobs/main_jobs/bronze_ingestion.py',
    mounts=[
        Mount(
            source='/home/matheo/BigData/spark_jobs',
            target='/home/jovyan/jobs',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/jars',
            target='/home/jovyan/jars',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/data',
            target='/data',
            type='bind'
        )
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='bigdata_network',
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag
)

silver_task = DockerOperator(
    task_id='silver_transformation_docker',
    image='jupyter/pyspark-notebook:latest',
    command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 /home/jovyan/jobs/main_jobs/silver_transformation.py',
    mounts=[
        Mount(
            source='/home/matheo/BigData/spark_jobs',
            target='/home/jovyan/jobs',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/jars',
            target='/home/jovyan/jars',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/data',
            target='/data',
            type='bind'
        )
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='bigdata_network',
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag
)

gold_task = DockerOperator(
    task_id='gold_star_schema_docker',
    image='jupyter/pyspark-notebook:latest',
    command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 /home/jovyan/jobs/main_jobs/gold_star_schema.py',
    mounts=[
        Mount(
            source='/home/matheo/BigData/spark_jobs',
            target='/home/jovyan/jobs',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/jars',
            target='/home/jovyan/jars',
            type='bind'
        ),
        Mount(
            source='/home/matheo/BigData/data',
            target='/data',
            type='bind'
        )
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='bigdata_network',
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_production_pipeline',
    dag=dag
)

# Définition des dépendances
start_task >> bronze_task >> silver_task >> gold_task >> end_task
