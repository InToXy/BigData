#!/bin/bash#!/bin/bash#!/bin/bash

#########################################################################

# Script d'exécution de l'ingestion Bronze pour CHU#########################################################################

#########################################################################

# Script d'exécution de l'ingestion Bronze pour CHU (Cloud Healthcare Unit)# Script pour exécuter bronze_ingestion.py dans le conteneur Jupyter

set -e

########################################################################## avec les dépendances Spark correctement configurées

# Couleurs

GREEN='\033[0;32m'

YELLOW='\033[1;33m'

RED='\033[0;31m'set -e  # Stop on errorecho "📦 Copie du script bronze_ingestion.py dans le conteneur..."

BLUE='\033[0;34m'

NC='\033[0m'docker cp /home/alban/BigData/BigData/spark_jobs/main_jobs/bronze_ingestion.py chu_jupyter:/home/jovyan/work/



echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"# Couleurs pour le terminal

echo -e "${BLUE}║   🏥 CHU - Pipeline Bronze Ingestion                      ║${NC}"

echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"GREEN='\033[0;32m'echo ""

echo ""

YELLOW='\033[1;33m'echo "🚀 Exécution du pipeline Bronze..."

CONTAINER_NAME="${CONTAINER_NAME:-chu_jupyter}"

SCRIPT_PATH="/home/jovyan/jobs/main_jobs/bronze_ingestion.py"RED='\033[0;31m'echo ""

JARS_DIR="/home/jovyan/jars"

BLUE='\033[0;34m'

# Vérifier container

echo -e "${YELLOW}🔍 Vérification du container...${NC}"NC='\033[0m' # No Colordocker exec chu_jupyter bash -c "

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then

    echo -e "${RED}❌ Container ${CONTAINER_NAME} non actif!${NC}"cd /home/jovyan/work && \

    exit 1

fiecho -e "${BLUE}╔════════════════════════════════════════════════════════════════════╗${NC}"spark-submit \

echo -e "${GREEN}✅ Container actif${NC}"

echo -e "${BLUE}║   🏥 CHU - Cloud Healthcare Unit - Pipeline Bronze                ║${NC}"  --master local[*] \

# Vérifier MinIO

echo -e "${YELLOW}🔍 Vérification MinIO...${NC}"echo -e "${BLUE}║   Ingestion des données de santé vers la zone Bronze              ║${NC}"  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar,/home/jovyan/jars/postgresql-42.6.0.jar \

if ! docker exec ${CONTAINER_NAME} bash -c "mc alias list | grep -q myminio" 2>/dev/null; then

    echo -e "${YELLOW}⚙️  Configuration MinIO...${NC}"echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════╝${NC}"  --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \

    docker exec ${CONTAINER_NAME} bash -c "mc alias set myminio http://minio:9000 minioadmin minioadmin123"

fiecho ""  --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar:/home/jovyan/jars/postgresql-42.6.0.jar \

echo -e "${GREEN}✅ MinIO OK${NC}"

  bronze_ingestion.py

# Créer bucket Bronze

echo -e "${YELLOW}🪣 Vérification bucket Bronze...${NC}"# Variables de configuration"

if ! docker exec ${CONTAINER_NAME} bash -c "mc ls myminio/bronze 2>/dev/null" >/dev/null 2>&1; then

    docker exec ${CONTAINER_NAME} bash -c "mc mb myminio/bronze"CONTAINER_NAME="${CONTAINER_NAME:-chu_jupyter}"

    echo -e "${GREEN}✅ Bucket Bronze créé${NC}"

elseSCRIPT_PATH="/home/jovyan/jobs/main_jobs/bronze_ingestion.py"echo ""

    echo -e "${GREEN}✅ Bucket Bronze existe${NC}"

fiJARS_DIR="/home/jovyan/jars"echo "✅ Terminé !"



# Vérifier données sources

echo -e "${YELLOW}🔍 Vérification données sources...${NC}"echo -e "${YELLOW}📋 Configuration:${NC}"

SOURCES=$(docker exec ${CONTAINER_NAME} bash -c "find /data/source -type f \( -name '*.csv' -o -name '*.xlsx' \) 2>/dev/null | wc -l" || echo "0")echo -e "   Container: ${CONTAINER_NAME}"

if [ "$SOURCES" -eq "0" ]; thenecho -e "   Script: ${SCRIPT_PATH}"

    echo -e "${RED}❌ Aucune donnée source trouvée!${NC}"echo -e "   JARs: ${JARS_DIR}"

    exit 1echo ""

fi

echo -e "${GREEN}✅ ${SOURCES} fichiers sources détectés${NC}"# Vérifier que le container existe et est en cours d'exécution

echo ""echo -e "${YELLOW}🔍 Vérification du container...${NC}"

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then

# Lancement    echo -e "${RED}❌ Le container ${CONTAINER_NAME} n'est pas en cours d'exécution!${NC}"

echo -e "${GREEN}🚀 Lancement du job Bronze...${NC}"    echo -e "${YELLOW}💡 Conseil: Lancez 'sudo docker-compose up -d' pour démarrer les services${NC}"

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"    exit 1

fi

START_TIME=$(date +%s)echo -e "${GREEN}✅ Container actif${NC}"

echo ""

docker exec ${CONTAINER_NAME} spark-submit \

    --master local[*] \# Vérifier la connexion MinIO

    --driver-memory 2g \echo -e "${YELLOW}🔍 Vérification MinIO...${NC}"

    --executor-memory 2g \if ! docker exec ${CONTAINER_NAME} bash -c "mc alias list | grep -q myminio" 2>/dev/null; then

    --conf spark.sql.shuffle.partitions=8 \    echo -e "${YELLOW}⚠️  Configuration de MinIO client...${NC}"

    --conf spark.sql.adaptive.enabled=true \    docker exec ${CONTAINER_NAME} bash -c "

    --jars ${JARS_DIR}/hadoop-aws-3.3.4.jar,${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \        mc alias set myminio http://minio:9000 minioadmin minioadmin123

    --conf spark.driver.extraClassPath=${JARS_DIR}/hadoop-aws-3.3.4.jar:${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \    " || {

    --conf spark.executor.extraClassPath=${JARS_DIR}/hadoop-aws-3.3.4.jar:${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \        echo -e "${RED}❌ Impossible de configurer MinIO${NC}"

    ${SCRIPT_PATH}        exit 1

    }

EXIT_CODE=$?fi

END_TIME=$(date +%s)echo -e "${GREEN}✅ MinIO accessible${NC}"

DURATION=$((END_TIME - START_TIME))echo ""



echo ""# Créer le bucket Bronze s'il n'existe pas

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"echo -e "${YELLOW}🪣 Vérification du bucket Bronze...${NC}"

if ! docker exec ${CONTAINER_NAME} bash -c "mc ls myminio/bronze 2>/dev/null" >/dev/null 2>&1; then

if [ $EXIT_CODE -eq 0 ]; then    echo -e "${YELLOW}⚙️  Création du bucket Bronze...${NC}"

    echo -e "${GREEN}✅ Job terminé avec succès!${NC}"    docker exec ${CONTAINER_NAME} bash -c "mc mb myminio/bronze" || {

    echo -e "${GREEN}⏱️  Durée: ${DURATION}s${NC}"        echo -e "${RED}❌ Impossible de créer le bucket${NC}"

    echo ""        exit 1

    echo -e "${YELLOW}📊 Tables Bronze créées:${NC}"    }

    docker exec ${CONTAINER_NAME} mc ls myminio/bronze/ || true    echo -e "${GREEN}✅ Bucket Bronze créé${NC}"

    echo ""else

    echo -e "${GREEN}🎯 Zone Bronze prête pour Silver!${NC}"    echo -e "${GREEN}✅ Bucket Bronze existe déjà${NC}"

elsefi

    echo -e "${RED}❌ Job échoué (code: $EXIT_CODE)${NC}"echo ""

    exit $EXIT_CODE

fi# Vérifier que les données sources sont montées

echo -e "${YELLOW}🔍 Vérification des données sources...${NC}"
SOURCES_CHECK=$(docker exec ${CONTAINER_NAME} bash -c "
    if [ -d /data/source ]; then
        find /data/source -type f \( -name '*.csv' -o -name '*.xlsx' \) | wc -l
    else
        echo '0'
    fi
")

if [ "$SOURCES_CHECK" -eq "0" ]; then
    echo -e "${RED}❌ Aucune donnée source trouvée dans /data/source!${NC}"
    echo -e "${YELLOW}💡 Conseil: Vérifiez que le volume est monté correctement dans docker-compose.yml${NC}"
    exit 1
fi
echo -e "${GREEN}✅ ${SOURCES_CHECK} fichiers sources détectés${NC}"
echo ""

# Vérifier PostgreSQL
echo -e "${YELLOW}🔍 Vérification PostgreSQL...${NC}"
if ! docker exec ${CONTAINER_NAME} bash -c "
    nc -z chu_postgres_data 5432
" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  PostgreSQL non accessible (normal si aucune table PostgreSQL)${NC}"
else
    echo -e "${GREEN}✅ PostgreSQL accessible${NC}"
fi
echo ""

# Afficher les tables qui seront ingérées
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   📊 Tables à ingérer dans Bronze:                                ║${NC}"
echo -e "${BLUE}╠════════════════════════════════════════════════════════════════════╣${NC}"
echo -e "${BLUE}║   1. 💾 PostgreSQL:                                               ║${NC}"
echo -e "${BLUE}║      - patients                                                    ║${NC}"
echo -e "${BLUE}║      - consultations                                               ║${NC}"
echo -e "${BLUE}║      - deces (filtré 2019)                                        ║${NC}"
echo -e "${BLUE}║                                                                    ║${NC}"
echo -e "${BLUE}║   2. 📁 CSV Files:                                                 ║${NC}"
echo -e "${BLUE}║      - etablissement_sante.csv (~417K lignes)                     ║${NC}"
echo -e "${BLUE}║      - professionnel_sante.csv                                     ║${NC}"
echo -e "${BLUE}║      - activite_professionnel_sante.csv                           ║${NC}"
echo -e "${BLUE}║      - Hospitalisations.csv (~2.5K lignes)                        ║${NC}"
echo -e "${BLUE}║      - Satisfaction (multiples années)                             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Options d'exécution
echo -e "${YELLOW}⚙️  Configuration Spark:${NC}"
echo -e "   Driver Memory: 2g (LOW_RESOURCE_MODE)"
echo -e "   Executor Memory: 2g"
echo -e "   Shuffle Partitions: 8"
echo -e "   Compression: Snappy"
echo ""

# Confirmation avant exécution
read -p "$(echo -e ${YELLOW}Voulez-vous lancer l\'ingestion Bronze? [y/N]: ${NC})" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}❌ Annulé par l'utilisateur${NC}"
    exit 0
fi
echo ""

# Lancement du job
echo -e "${GREEN}🚀 Lancement du job d'ingestion Bronze...${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════════${NC}"
echo ""

START_TIME=$(date +%s)

# Exécuter le job avec spark-submit
docker exec ${CONTAINER_NAME} spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.adaptive.enabled=true \
    --jars ${JARS_DIR}/hadoop-aws-3.3.4.jar,${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=${JARS_DIR}/hadoop-aws-3.3.4.jar:${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=${JARS_DIR}/hadoop-aws-3.3.4.jar:${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    ${SCRIPT_PATH}

EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════════════${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Job terminé avec succès!${NC}"
    echo -e "${GREEN}⏱️  Durée: ${DURATION}s${NC}"
    echo ""
    
    # Afficher le contenu du bucket Bronze
    echo -e "${YELLOW}📊 Contenu du bucket Bronze:${NC}"
    docker exec ${CONTAINER_NAME} mc ls myminio/bronze/ || true
    echo ""
    
    # Compter les tables créées
    TABLE_COUNT=$(docker exec ${CONTAINER_NAME} mc ls myminio/bronze/ | wc -l)
    echo -e "${GREEN}🎯 ${TABLE_COUNT} tables créées dans Bronze${NC}"
    echo ""
    
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   ✅ Zone Bronze prête pour la transformation Silver!             ║${NC}"
    echo -e "${BLUE}║                                                                    ║${NC}"
    echo -e "${BLUE}║   Prochaines étapes:                                               ║${NC}"
    echo -e "${BLUE}║   1. Vérifier les données: mc ls myminio/bronze/                  ║${NC}"
    echo -e "${BLUE}║   2. Lancer la transformation Silver                              ║${NC}"
    echo -e "${BLUE}║   3. Créer les KPIs dans Gold                                     ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════╝${NC}"
else
    echo -e "${RED}❌ Job échoué (code: $EXIT_CODE)${NC}"
    echo -e "${RED}⏱️  Durée avant échec: ${DURATION}s${NC}"
    echo ""
    echo -e "${YELLOW}💡 Conseils de dépannage:${NC}"
    echo -e "   1. Vérifiez les logs ci-dessus pour les erreurs"
    echo -e "   2. Vérifiez que PostgreSQL contient des données"
    echo -e "   3. Vérifiez que les fichiers CSV sont accessibles"
    echo -e "   4. Consultez les logs Spark pour plus de détails"
    exit $EXIT_CODE
fi
