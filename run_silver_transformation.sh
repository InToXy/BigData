#!/bin/bash

# Script pour exécuter silver_transformation.py dans le conteneur Jupyter
# avec les dépendances S3A

echo "🔍 Vérification du bucket bronze..."
BRONZE_COUNT=$(docker exec chu_minio mc ls myminio/bronze/ 2>/dev/null | wc -l)

if [ "$BRONZE_COUNT" -eq 0 ]; then
    echo "⚠️  ATTENTION: Le bucket bronze est vide !"
    echo "   Vous devez d'abord exécuter le pipeline Bronze:"
    echo "   ./run_bronze_ingestion.sh"
    echo ""
    read -p "Voulez-vous exécuter le pipeline Bronze maintenant? (o/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Oo]$ ]]; then
        echo "🚀 Exécution du pipeline Bronze..."
        ./run_bronze_ingestion.sh
        echo ""
        echo "✅ Pipeline Bronze terminé, démarrage du pipeline Silver..."
    else
        echo "❌ Annulé - Le pipeline Silver nécessite les données Bronze"
        exit 1
    fi
fi

echo ""
echo "🚀 Exécution du pipeline Silver..."
echo ""

docker exec chu_jupyter bash -c "
cd /home/jovyan/jobs/main_jobs && \
spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  silver_transformation.py
"

echo ""
echo "✅ Terminé !"
echo ""
echo "📊 Pour consulter les résultats dans MinIO:"
echo "   http://localhost:9001 (minioadmin / minioadmin123)"
echo "   Bucket: silver"
echo ""
echo "📌 Prochaine étape:"
echo "   ./run_gold_delta.sh     # Pour générer les KPIs (format Delta Lake)"
echo "   OU"
echo "   Exécuter gold_aggregation.py pour KPIs au format Parquet standard"
