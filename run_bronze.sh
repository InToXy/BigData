#!/bin/bash
set -e

CONTAINER="chu_jupyter"
echo "🚀 Lancement ingestion Bronze..."

# Config MinIO
docker exec $CONTAINER mc alias set myminio http://minio:9000 minioadmin minioadmin123 2>/dev/null || true
docker exec $CONTAINER mc mb myminio/bronze 2>/dev/null || true

# Lance le job
docker exec $CONTAINER spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/jobs/main_jobs/bronze_ingestion.py

echo "✅ Terminé! Tables Bronze:"
docker exec $CONTAINER mc ls myminio/bronze/
