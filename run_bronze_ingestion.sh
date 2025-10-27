#!/bin/bash

# Script pour exécuter bronze_ingestion.py dans le conteneur Jupyter
# avec les dépendances Spark correctement configurées

echo "📦 Copie du script bronze_ingestion.py dans le conteneur..."
docker cp /home/alban/BigData/BigData/spark_jobs/main_jobs/bronze_ingestion.py chu_jupyter:/home/jovyan/work/

echo ""
echo "🚀 Exécution du pipeline Bronze..."
echo ""

docker exec chu_jupyter bash -c "
cd /home/jovyan/work && \
spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar,/home/jovyan/jars/postgresql-42.6.0.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
  bronze_ingestion.py
"

echo ""
echo "✅ Terminé !"
