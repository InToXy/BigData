#!/bin/bash
# Script global d'analyse de performance Bronze + Silver

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║   🚀 ANALYSE DE PERFORMANCE COMPLÈTE - DATA LAKE CHU           ║"
echo "║   📊 Bronze Layer + Silver Layer                                ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Couleurs pour l'affichage
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Fonction de vérification MinIO
check_minio() {
    echo -e "${BLUE}🔍 Vérification de MinIO...${NC}"
    if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9000/minio/health/live | grep -q "200"; then
        echo -e "${GREEN}   ✅ MinIO est accessible${NC}"
        return 0
    else
        echo -e "${RED}   ❌ MinIO n'est pas accessible sur http://127.0.0.1:9000${NC}"
        echo -e "${YELLOW}   💡 Lancez: cd /home/alban/BigData/BigData && docker-compose up -d chu_minio${NC}"
        return 1
    fi
}

# Vérifier MinIO
check_minio
if [ $? -ne 0 ]; then
    exit 1
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo -e "${BLUE}📦 PARTIE 1/2 - ANALYSE DU BUCKET BRONZE${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

cd bucket_bronze
if [ -f generer_tout.sh ]; then
    ./generer_tout.sh
    BRONZE_STATUS=$?
else
    echo -e "${RED}❌ Script generer_tout.sh introuvable dans bucket_bronze${NC}"
    BRONZE_STATUS=1
fi

cd ..

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo -e "${BLUE}📦 PARTIE 2/2 - ANALYSE DU BUCKET SILVER${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

cd bucket_silver
if [ -f generer_tout.sh ]; then
    ./generer_tout.sh
    SILVER_STATUS=$?
else
    echo -e "${RED}❌ Script generer_tout.sh introuvable dans bucket_silver${NC}"
    SILVER_STATUS=1
fi

cd ..

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                    📊 RÉSUMÉ DE L'ANALYSE                       ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

if [ $BRONZE_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ Bronze Layer : Analyse terminée avec succès${NC}"
    echo "   📁 Dossier : bucket_bronze/"
    echo "   📊 Graphiques : 9 fichiers PNG"
    echo "   📄 Rapport : rapport_performance.html"
else
    echo -e "${RED}❌ Bronze Layer : Erreur lors de l'analyse${NC}"
fi

echo ""

if [ $SILVER_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ Silver Layer : Analyse terminée avec succès${NC}"
    echo "   📁 Dossier : bucket_silver/"
    echo "   📊 Graphiques : 9 fichiers PNG"
    echo "   📄 Rapport : rapport_performance.html"
else
    echo -e "${RED}❌ Silver Layer : Erreur lors de l'analyse${NC}"
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"

# Calculer le statut final
if [ $BRONZE_STATUS -eq 0 ] && [ $SILVER_STATUS -eq 0 ]; then
    echo -e "${GREEN}🎉 Toutes les analyses sont terminées avec succès !${NC}"
    echo ""
    echo "📂 Visualiser les rapports :"
    echo "   Bronze : file:///home/alban/BigData/BigData/graphes/bucket_bronze/rapport_performance.html"
    echo "   Silver : file:///home/alban/BigData/BigData/graphes/bucket_silver/rapport_performance.html"
    echo ""
    echo "══════════════════════════════════════════════════════════════════"
    exit 0
elif [ $BRONZE_STATUS -eq 0 ] || [ $SILVER_STATUS -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Analyse partielle terminée${NC}"
    echo ""
    if [ $BRONZE_STATUS -eq 0 ]; then
        echo "   Bronze : file:///home/alban/BigData/BigData/graphes/bucket_bronze/rapport_performance.html"
    fi
    if [ $SILVER_STATUS -eq 0 ]; then
        echo "   Silver : file:///home/alban/BigData/BigData/graphes/bucket_silver/rapport_performance.html"
    fi
    echo ""
    echo "══════════════════════════════════════════════════════════════════"
    exit 1
else
    echo -e "${RED}❌ Toutes les analyses ont échoué${NC}"
    echo ""
    echo "💡 Vérifiez que :"
    echo "   1. MinIO est démarré (docker ps | grep minio)"
    echo "   2. Les buckets bronze et silver existent"
    echo "   3. Les données ont été ingérées"
    echo ""
    echo "══════════════════════════════════════════════════════════════════"
    exit 1
fi
