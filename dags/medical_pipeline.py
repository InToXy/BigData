from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from utils.hdfs_check import create_hdfs_check_task

with DAG(
    dag_id='medical_data_pipeline',
    start_date=datetime(2025, 10, 15),
    schedule_interval=None,  # This DAG will be triggered manually
    catchup=False,
    tags=['medical', 'spark', 'pipeline'],
) as dag:
    
    check_hdfs = create_hdfs_check_task(dag)

    bronze_ingestion = SparkSubmitOperator(
        task_id='bronze_ingestion',
        application='/opt/spark/jobs/bronze_ingestion.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.python.version': '3',
            'spark.driver.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED',
            'spark.executor.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
        },
        application_args=['--source-path', '/data/source']
    )


    silver_transformation = SparkSubmitOperator(
        task_id='silver_transformation',
        application='/opt/spark/jobs/silver_transformation.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.python.version': '3',
            'spark.driver.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED',
            'spark.executor.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
        }
    )

    gold_aggregation = SparkSubmitOperator(
        task_id='gold_aggregation',
        application='/opt/spark/jobs/gold_aggregation.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.python.version': '3',
            'spark.driver.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED',
            'spark.executor.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
        }
    )

    check_hdfs >> bronze_ingestion >> silver_transformation >> gold_aggregation