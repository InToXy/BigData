#!/bin/bash
# Script de vérification du contenu du bucket Bronze

CONTAINER="chu_jupyter"

echo "🔍 Vérification du bucket Bronze..."
echo ""

docker exec $CONTAINER spark-submit \
    --master local[*] \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/work/verify_bronze.py
