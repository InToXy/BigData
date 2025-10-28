#!/bin/bash
###############################################################################
# run_silver.sh
# Exécute la transformation Silver (Bronze → Star Schema)
###############################################################################

set -e

CONTAINER="chu_jupyter"
SCRIPT="/home/jovyan/jobs/main_jobs/silver_transformation_clean.py"

echo "======================================================================="
echo "🔵 TRANSFORMATION SILVER - CHU DATA WAREHOUSE"
echo "======================================================================="
echo "📅 Début: $(date)"
echo ""

# Vérifier que le conteneur est en cours d'exécution
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "❌ Erreur: Le conteneur ${CONTAINER} n'est pas en cours d'exécution"
    echo "   Lancer d'abord: docker-compose up -d"
    exit 1
fi

echo "✅ Conteneur ${CONTAINER} actif"
echo ""

# Vérifier que Bronze existe
echo "🔍 Vérification du bucket Bronze..."
docker exec ${CONTAINER} bash -c "
  python3 -c '
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(\"check\").config(\"spark.hadoop.fs.s3a.endpoint\", \"http://minio:9000\").config(\"spark.hadoop.fs.s3a.access.key\", \"minioadmin\").config(\"spark.hadoop.fs.s3a.secret.key\", \"minioadmin123\").config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\").config(\"spark.hadoop.fs.s3a.connection.ssl.enabled\", \"false\").config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\").getOrCreate()
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(\"s3a://bronze\"), hadoop_conf)
path = sc._jvm.org.apache.hadoop.fs.Path(\"s3a://bronze/\")
if fs.exists(path):
    print(\"✅ Bronze bucket exists\")
else:
    print(\"❌ Bronze bucket not found\")
    exit(1)
'
" || {
    echo ""
    echo "❌ Le bucket Bronze n'existe pas ou est vide"
    echo "   Lancer d'abord: ./run_bronze.sh"
    exit 1
}

echo ""
echo "======================================================================="
echo "🚀 Lancement de la transformation Silver"
echo "======================================================================="
echo ""

# Exécution du job Spark
docker exec ${CONTAINER} spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=8 \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  ${SCRIPT}

EXIT_CODE=$?

echo ""
echo "======================================================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ TRANSFORMATION SILVER RÉUSSIE"
    echo "======================================================================="
    echo "📅 Fin: $(date)"
    echo ""
    echo "📋 Tables Silver créées:"
    echo "   - dim_patient"
    echo "   - dim_etablissement"
    echo "   - dim_temps"
    echo "   - fact_consultation"
    echo "   - fact_hospitalisation"
    echo "   - fact_deces"
    echo "   - metrique_satisfaction"
    echo ""
    echo "➡️  Prochaine étape: ./run_gold.sh"
else
    echo "❌ ERREUR TRANSFORMATION SILVER"
    echo "======================================================================="
    echo "Code erreur: ${EXIT_CODE}"
    echo ""
    echo "Pour débugger:"
    echo "  docker logs ${CONTAINER}"
fi
echo "======================================================================="

exit $EXIT_CODE
