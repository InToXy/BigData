#!/bin/bash
###############################################################################
# check_environment.sh
# Vérifie que tous les composants sont prêts avant d'exécuter le pipeline
###############################################################################

set -e

echo "======================================================================="
echo "🔍 VÉRIFICATION DE L'ENVIRONNEMENT CHU DATA WAREHOUSE"
echo "======================================================================="
echo ""

# Couleurs pour output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Compteurs
PASSED=0
FAILED=0

# Fonction de test
check_service() {
    SERVICE=$1
    if docker ps --format '{{.Names}}' | grep -q "^${SERVICE}$"; then
        echo -e "${GREEN}✅${NC} ${SERVICE} - Running"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}❌${NC} ${SERVICE} - Not running"
        ((FAILED++))
        return 1
    fi
}

check_port() {
    PORT=$1
    SERVICE=$2
    if nc -z localhost ${PORT} 2>/dev/null; then
        echo -e "${GREEN}✅${NC} Port ${PORT} (${SERVICE}) - Accessible"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}❌${NC} Port ${PORT} (${SERVICE}) - Not accessible"
        ((FAILED++))
        return 1
    fi
}

check_file() {
    FILE=$1
    DESC=$2
    if [ -f "${FILE}" ]; then
        echo -e "${GREEN}✅${NC} ${DESC} - Exists"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}❌${NC} ${DESC} - Missing"
        ((FAILED++))
        return 1
    fi
}

# 1. CONTENEURS DOCKER
echo "📦 1. Vérification des conteneurs Docker"
echo "─────────────────────────────────────────"
check_service "chu_minio"
check_service "chu_postgres_data"
check_service "chu_jupyter"
check_service "chu_hive_metastore"
check_service "chu_trino"
check_service "chu_superset"

echo ""

# 2. PORTS RÉSEAU
echo "🌐 2. Vérification des ports réseau"
echo "─────────────────────────────────────────"
check_port 9000 "MinIO API"
check_port 9001 "MinIO Console"
check_port 5432 "PostgreSQL"
check_port 8888 "Jupyter Lab"
check_port 9083 "Hive Metastore"
check_port 8090 "Trino"
check_port 8088 "Superset"

echo ""

# 3. SCRIPTS D'EXÉCUTION
echo "📜 3. Vérification des scripts"
echo "─────────────────────────────────────────"
check_file "./run_bronze.sh" "Script Bronze"
check_file "./run_silver.sh" "Script Silver"
check_file "./run_gold.sh" "Script Gold"
check_file "./run_all_pipeline.sh" "Script Pipeline Complet"
check_file "./setup_trino.sh" "Script Setup Trino"

echo ""

# 4. JOBS SPARK
echo "⚡ 4. Vérification des jobs Spark"
echo "─────────────────────────────────────────"
check_file "./spark_jobs/main_jobs/bronze_ingestion.py" "Job Bronze"
check_file "./spark_jobs/main_jobs/silver_transformation_clean.py" "Job Silver"
check_file "./spark_jobs/main_jobs/gold_aggregation_clean.py" "Job Gold"

echo ""

# 5. CONFIGURATION TRINO
echo "🔧 5. Vérification configuration Trino"
echo "─────────────────────────────────────────"
check_file "./trino/catalog/hive.properties" "Catalogue Hive"
check_file "./trino/catalog/iceberg.properties" "Catalogue Iceberg"
check_file "./trino/setup_trino_gold.sql" "Script SQL Trino"

echo ""

# 6. BUCKETS MINIO
echo "🪣 6. Vérification des buckets MinIO"
echo "─────────────────────────────────────────"

if docker ps --format '{{.Names}}' | grep -q "^chu_minio$"; then
    for BUCKET in bronze silver gold; do
        if docker exec chu_minio mc ls myminio/${BUCKET} >/dev/null 2>&1; then
            echo -e "${GREEN}✅${NC} Bucket ${BUCKET} - Exists"
            ((PASSED++))
        else
            echo -e "${YELLOW}⚠️${NC}  Bucket ${BUCKET} - Missing (sera créé automatiquement)"
        fi
    done
else
    echo -e "${RED}❌${NC} MinIO non accessible pour vérifier les buckets"
    ((FAILED+=3))
fi

echo ""

# 7. CONNECTIVITÉ SPARK -> MINIO
echo "🔗 7. Test connectivité Spark → MinIO"
echo "─────────────────────────────────────────"

if docker ps --format '{{.Names}}' | grep -q "^chu_jupyter$"; then
    if docker exec chu_jupyter python3 -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('test').config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000').config('spark.hadoop.fs.s3a.access.key', 'minioadmin').config('spark.hadoop.fs.s3a.secret.key', 'minioadmin123').config('spark.hadoop.fs.s3a.path.style.access', 'true').config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false').config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem').getOrCreate()
    print('OK')
    spark.stop()
except Exception as e:
    print(f'ERROR: {e}')
    exit(1)
" 2>/dev/null | grep -q "OK"; then
        echo -e "${GREEN}✅${NC} Spark peut accéder à MinIO"
        ((PASSED++))
    else
        echo -e "${RED}❌${NC} Spark ne peut pas accéder à MinIO"
        ((FAILED++))
    fi
else
    echo -e "${RED}❌${NC} Conteneur Jupyter non disponible"
    ((FAILED++))
fi

echo ""

# 8. JARS HADOOP
echo "📚 8. Vérification des JARs Hadoop/AWS"
echo "─────────────────────────────────────────"

if docker exec chu_jupyter bash -c "[ -f /home/jovyan/jars/hadoop-aws-3.3.4.jar ]" 2>/dev/null; then
    echo -e "${GREEN}✅${NC} hadoop-aws-3.3.4.jar - Present"
    ((PASSED++))
else
    echo -e "${RED}❌${NC} hadoop-aws-3.3.4.jar - Missing"
    ((FAILED++))
fi

if docker exec chu_jupyter bash -c "[ -f /home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar ]" 2>/dev/null; then
    echo -e "${GREEN}✅${NC} aws-java-sdk-bundle-1.12.262.jar - Present"
    ((PASSED++))
else
    echo -e "${RED}❌${NC} aws-java-sdk-bundle-1.12.262.jar - Missing"
    ((FAILED++))
fi

echo ""

# RÉSUMÉ
echo "======================================================================="
echo "📊 RÉSUMÉ DE LA VÉRIFICATION"
echo "======================================================================="
echo ""
echo -e "Tests réussis:  ${GREEN}${PASSED}${NC}"
echo -e "Tests échoués:  ${RED}${FAILED}${NC}"
echo ""

if [ ${FAILED} -eq 0 ]; then
    echo -e "${GREEN}✅ ENVIRONNEMENT PRÊT!${NC}"
    echo ""
    echo "======================================================================="
    echo "🚀 PROCHAINES ÉTAPES"
    echo "======================================================================="
    echo ""
    echo "1. Exécuter le pipeline complet:"
    echo "   chmod +x run_all_pipeline.sh"
    echo "   ./run_all_pipeline.sh"
    echo ""
    echo "2. Configurer Trino:"
    echo "   chmod +x setup_trino.sh"
    echo "   ./setup_trino.sh"
    echo ""
    echo "3. Accéder aux services:"
    echo "   - MinIO:    http://localhost:9001 (minioadmin/minioadmin123)"
    echo "   - Jupyter:  http://localhost:8888 (token: admin123)"
    echo "   - Trino:    http://localhost:8090"
    echo "   - Superset: http://localhost:8088 (admin/admin123)"
    echo ""
    echo "======================================================================="
    exit 0
else
    echo -e "${RED}❌ PROBLÈMES DÉTECTÉS${NC}"
    echo ""
    echo "======================================================================="
    echo "🔧 ACTIONS CORRECTIVES"
    echo "======================================================================="
    echo ""
    
    if ! docker ps --format '{{.Names}}' | grep -q "chu_minio\|chu_postgres_data\|chu_jupyter"; then
        echo "1. Démarrer les conteneurs:"
        echo "   docker-compose up -d"
        echo ""
    fi
    
    if [ ! -f "./run_bronze.sh" ] || [ ! -f "./run_silver.sh" ] || [ ! -f "./run_gold.sh" ]; then
        echo "2. Scripts manquants - ils devraient avoir été créés"
        echo "   Vérifier le contenu du répertoire"
        echo ""
    fi
    
    if ! docker exec chu_jupyter bash -c "[ -f /home/jovyan/jars/hadoop-aws-3.3.4.jar ]" 2>/dev/null; then
        echo "3. JARs Hadoop manquants:"
        echo "   Télécharger les JARs dans ./jars/"
        echo "   - hadoop-aws-3.3.4.jar"
        echo "   - aws-java-sdk-bundle-1.12.262.jar"
        echo ""
    fi
    
    echo "Après corrections, relancer:"
    echo "   ./check_environment.sh"
    echo ""
    echo "======================================================================="
    exit 1
fi
