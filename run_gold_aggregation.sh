#!/bin/bash

# Script pour exécuter gold_aggregation.py (version Parquet standard)
# dans le conteneur Jupyter avec les dépendances S3A

echo "🔍 Vérification du bucket silver..."
# Vérifier que le bucket contient des fichiers Parquet, pas juste des dossiers vides
PARQUET_COUNT=$(docker exec chu_minio mc ls -r myminio/silver/ 2>/dev/null | grep -c "\.parquet$" || echo "0")

if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo "⚠️  ATTENTION: Aucun fichier Parquet trouvé dans le bucket silver !"
    echo "   Vous devez d'abord exécuter le pipeline Silver:"
    echo "   ./run_silver_transformation.sh"
    exit 1
fi

echo "✅ Bucket silver contient $PARQUET_COUNT fichiers Parquet"
echo ""
echo "📦 Vérification du bucket gold..."
docker exec chu_minio mc ls myminio/gold >/dev/null 2>&1 || {
    echo "🔧 Création du bucket gold..."
    docker exec chu_minio mc mb myminio/gold
}

echo ""
echo "🚀 Exécution du pipeline Gold (Parquet standard)..."
echo ""

docker exec chu_jupyter bash -c "
cd /home/jovyan/jobs/main_jobs && \
spark-submit \
  --master local[*] \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  gold_aggregation.py
"

echo ""
echo "✅ Terminé !"
echo ""
echo "📊 Pour consulter les résultats dans MinIO:"
echo "   http://localhost:9001 (minioadmin / minioadmin123)"
echo "   Bucket: gold"
echo ""
echo "💡 Note: Cette version utilise le format Parquet standard"
echo "   Pour Delta Lake avec Time Travel, utilisez: ./run_gold_delta.sh"
