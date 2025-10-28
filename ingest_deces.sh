#!/bin/bash

echo "╔══════════════════════════════════════════════════╗"
echo "║    INGESTION DONNÉES DÉCÈS 2019 - BRONZE        ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Vérifier que le conteneur Jupyter est démarré
if ! docker ps | grep -q chu_jupyter; then
    echo "❌ Conteneur chu_jupyter non démarré"
    echo "➡️  Lancez: docker-compose up -d chu_jupyter"
    exit 1
fi

echo "✅ Conteneur Jupyter actif"
echo ""

# Vérifier la connectivité PostgreSQL
echo "🔍 Vérification PostgreSQL..."
if ! docker exec chu_postgres_data psql -U admin -d healthcare_data -c "SELECT COUNT(*) FROM deces WHERE EXTRACT(YEAR FROM date_deces) = 2019;" > /dev/null 2>&1; then
    echo "❌ Impossible de se connecter à PostgreSQL"
    exit 1
fi

DECES_COUNT=$(docker exec chu_postgres_data psql -U admin -d healthcare_data -t -c "SELECT COUNT(*) FROM deces WHERE EXTRACT(YEAR FROM date_deces) = 2019;")
echo "✅ PostgreSQL accessible - $DECES_COUNT décès en 2019"
echo ""

# Vérifier MinIO
echo "🔍 Vérification MinIO..."
if ! docker exec chu_jupyter curl -s http://minio:9000 > /dev/null; then
    echo "❌ MinIO non accessible"
    exit 1
fi
echo "✅ MinIO accessible"
echo ""

# Exécuter le job Spark
echo "🚀 Lancement du job d'ingestion..."
echo ""

docker exec chu_jupyter spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.shuffle.partitions=8 \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar,/home/jovyan/jars/postgresql-42.6.0.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \
    /home/jovyan/jobs/main_jobs/ingest_deces.py

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "╔══════════════════════════════════════════════════╗"
    echo "║         ✅ INGESTION RÉUSSIE                     ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""
    echo "📊 Vérification dans MinIO:"
    docker exec chu_jupyter aws --endpoint-url http://minio:9000 s3 ls s3://bronze/deces_2019/ --recursive --human-readable --summarize
    echo ""
    echo "➡️  Vous pouvez maintenant relancer le job Silver:"
    echo "    ./run_silver.sh"
else
    echo "╔══════════════════════════════════════════════════╗"
    echo "║         ❌ INGESTION ÉCHOUÉE                     ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""
    echo "🔍 Vérifiez les logs ci-dessus pour plus de détails"
    exit 1
fi
