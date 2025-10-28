#!/bin/bash
###############################################################################
# run_gold.sh
# Exécute l'agrégation Gold (Silver → KPIs)
###############################################################################

set -e

CONTAINER="chu_jupyter"
SCRIPT="/home/jovyan/jobs/main_jobs/gold_aggregation_clean.py"

echo "======================================================================="
echo "🏆 AGRÉGATION GOLD - KPIs CHU DATA WAREHOUSE"
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

# Vérifier que Silver existe
echo "🔍 Vérification du bucket Silver..."
docker exec ${CONTAINER} bash -c "
  python3 -c '
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(\"check\").config(\"spark.hadoop.fs.s3a.endpoint\", \"http://minio:9000\").config(\"spark.hadoop.fs.s3a.access.key\", \"minioadmin\").config(\"spark.hadoop.fs.s3a.secret.key\", \"minioadmin123\").config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\").config(\"spark.hadoop.fs.s3a.connection.ssl.enabled\", \"false\").config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\").getOrCreate()
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(\"s3a://silver\"), hadoop_conf)
path = sc._jvm.org.apache.hadoop.fs.Path(\"s3a://silver/\")
if fs.exists(path):
    print(\"✅ Silver bucket exists\")
else:
    print(\"❌ Silver bucket not found\")
    exit(1)
'
" || {
    echo ""
    echo "❌ Le bucket Silver n'existe pas ou est vide"
    echo "   Lancer d'abord: ./run_silver.sh"
    exit 1
}

echo ""
echo "======================================================================="
echo "🚀 Lancement de l'agrégation Gold"
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
    echo "✅ AGRÉGATION GOLD RÉUSSIE"
    echo "======================================================================="
    echo "📅 Fin: $(date)"
    echo ""
    echo "🎯 KPIs créés (8 tables):"
    echo "   1. kpi_consultation_rate - Taux consultations par période"
    echo "   2. kpi_hospitalisation_metrics - Métriques hospitalisation"
    echo "   3. kpi_deces_by_region - Décès par région/démographie"
    echo "   4. kpi_satisfaction_global - Satisfaction patients agrégée"
    echo "   5. kpi_activite_mensuelle - Activité mensuelle globale"
    echo "   6. kpi_patient_demographics - Démographie patients"
    echo "   7. kpi_etablissement_performance - Performance établissements"
    echo "   8. kpi_temporal_trends - Tendances temporelles"
    echo ""
    echo "➡️  Prochaine étape: Visualisation Superset (http://localhost:8088)"
    echo "    Login: admin / admin123"
else
    echo "❌ ERREUR AGRÉGATION GOLD"
    echo "======================================================================="
    echo "Code erreur: ${EXIT_CODE}"
    echo ""
    echo "Pour débugger:"
    echo "  docker logs ${CONTAINER}"
fi
echo "======================================================================="

exit $EXIT_CODE
