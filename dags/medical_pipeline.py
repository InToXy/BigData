from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='medical_data_pipeline',
    start_date=datetime(2025, 10, 15),
    schedule_interval=None,  # This DAG will be triggered manually
    catchup=False,
    tags=['medical', 'spark', 'pipeline'],
) as dag:

    bronze_ingestion = SparkSubmitOperator(
        task_id='bronze_ingestion',
        application='/opt/spark/jobs/bronze_ingestion.py',
        conn_id='spark_default',
        application_args=['--source-path', '/data/source'],
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.python.version': '3',
            'spark.driver.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED',
            'spark.executor.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
        }
    )

    silver_transformation = SparkSubmitOperator(
        task_id='silver_transformation',
        application='/opt/spark/jobs/silver_transformation.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
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
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.python.version': '3',
            'spark.driver.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED',
            'spark.executor.extraJavaOptions': '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
        }
    )

    bronze_ingestion >> silver_transformation >> gold_aggregation