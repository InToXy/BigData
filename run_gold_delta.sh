#!/bin/bash

# Script pour exécuter gold_aggregation_delta.py dans le conteneur Jupyter
# avec Delta Lake et les dépendances S3A

echo "📦 Vérification du bucket gold-delta..."
docker exec chu_minio mc ls myminio/gold-delta >/dev/null 2>&1 || {
    echo "🔧 Création du bucket gold-delta..."
    docker exec chu_minio mc mb myminio/gold-delta
}

echo ""
echo "🚀 Exécution du pipeline Gold Delta..."
echo ""

docker exec chu_jupyter bash -c "
cd /home/jovyan/jobs/main_jobs && \
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  gold_aggregation_delta.py
"

echo ""
echo "✅ Terminé !"
echo ""
echo "📊 Pour consulter les résultats dans MinIO:"
echo "   http://localhost:9001 (minioadmin / minioadmin123)"
echo "   Bucket: gold-delta"
