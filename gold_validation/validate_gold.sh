#!/bin/bash

# Script de validation des tables Gold - Wrapper interactif
# Permet d'exécuter la validation via Jupyter (RECOMMANDÉ) ou Trino

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALIDATION_SCRIPT="validate_gold_tables.py"
TRINO_SCRIPT="quick_check_trino.sh"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}║           🔍 VALIDATION DES TABLES GOLD - CHU DATA LAKE          ║${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Vérifier que les scripts existent
if [ ! -f "$SCRIPT_DIR/$VALIDATION_SCRIPT" ]; then
    echo -e "${RED}❌ Erreur: Script Python introuvable: $VALIDATION_SCRIPT${NC}"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/$TRINO_SCRIPT" ]; then
    echo -e "${RED}❌ Erreur: Script Trino introuvable: $TRINO_SCRIPT${NC}"
    exit 1
fi

# Fonction d'aide
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -d, --detailed     Affiche les détails de chaque table"
    echo "  -e, --export       Exporte les résultats en CSV"
    echo "  -s, --sample N     Nombre de lignes d'échantillon (défaut: 5)"
    echo "  -h, --help         Affiche cette aide"
    echo ""
    echo "Exemples:"
    echo "  $0                           # Validation basique"
    echo "  $0 --detailed                # Validation détaillée"
    echo "  $0 --detailed --export       # Validation détaillée avec export CSV"
    echo "  $0 --detailed --sample 10    # 10 lignes d'échantillon par table"
    echo ""
    exit 0
}

# Vérifier les arguments
PYTHON_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--detailed)
            PYTHON_ARGS="$PYTHON_ARGS --detailed"
            shift
            ;;
        -e|--export)
            PYTHON_ARGS="$PYTHON_ARGS --export-csv"
            shift
            ;;
        -s|--sample)
            PYTHON_ARGS="$PYTHON_ARGS --sample-size $2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}❌ Option inconnue: $1${NC}"
            echo "Utilisez --help pour voir les options disponibles"
            exit 1
            ;;
    esac
done

# Méthode d'exécution
echo -e "${YELLOW}📍 Méthode d'exécution:${NC}"
echo ""
echo "Choisissez la méthode d'exécution:"
echo "  1) Jupyter Spark (chu_jupyter) - ⚡ RECOMMANDÉ - Analyse complète"
echo "  2) Trino SQL (chu_trino) - 🚀 RAPIDE - Vérification basique"
echo ""
read -p "Votre choix [1-2]: " choice

case $choice in
    1)
        echo ""
        echo -e "${GREEN}🚀 Exécution via Spark dans Jupyter...${NC}"
        echo ""
        
        # Vérifier que le container existe
        if ! docker ps | grep -q chu_jupyter; then
            echo -e "${RED}❌ Erreur: Container chu_jupyter n'est pas démarré${NC}"
            echo "Démarrez-le avec: cd BigData && docker-compose up -d jupyter"
            exit 1
        fi
        
        # Copier le script dans le container
        echo "📦 Copie du script de validation..."
        docker cp "$SCRIPT_DIR/$VALIDATION_SCRIPT" chu_jupyter:/home/jovyan/work/validate_gold_tables.py
        
        # Exécuter avec spark-submit
        echo "⚙️  Exécution de la validation..."
        docker exec chu_jupyter bash -c "cd /home/jovyan/work && spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 validate_gold_tables.py $PYTHON_ARGS"
        VALIDATION_EXIT=$?
        
        # Récupérer les exports CSV si demandés
        if echo "$PYTHON_ARGS" | grep -q "export-csv"; then
            echo ""
            echo "📥 Récupération des rapports CSV..."
            docker cp chu_jupyter:/home/jovyan/work/gold_stats_*.csv "$SCRIPT_DIR/" 2>/dev/null || true
            docker cp chu_jupyter:/home/jovyan/work/gold_quality_*.csv "$SCRIPT_DIR/" 2>/dev/null || true
            
            if ls "$SCRIPT_DIR"/gold_*.csv 1> /dev/null 2>&1; then
                echo -e "${GREEN}✅ Rapports CSV sauvegardés dans: $SCRIPT_DIR/${NC}"
                ls -lh "$SCRIPT_DIR"/gold_*.csv
            fi
        fi
        
        exit $VALIDATION_EXIT
        ;;
    
    2)
        echo ""
        echo -e "${GREEN}🚀 Exécution via Trino (vérification rapide)...${NC}"
        echo ""
        
        # Vérifier que Trino est démarré
        if ! docker ps | grep -q chu_trino; then
            echo -e "${RED}❌ Erreur: Container chu_trino n'est pas démarré${NC}"
            echo "Démarrez-le avec:"
            echo "  docker run -d --name chu_trino --network bigdata_network \\"
            echo "    -p 8090:8080 \\"
            echo "    -v /home/alban/BigData/BigData/trino/etc:/etc/trino \\"
            echo "    -v /home/alban/BigData/BigData/trino/catalog:/etc/trino/catalog \\"
            echo "    trinodb/trino:435"
            exit 1
        fi
        
        # Exécuter le script Trino
        exec "$SCRIPT_DIR/$TRINO_SCRIPT" "$@"
        ;;
    
    *)
        echo -e "${RED}❌ Choix invalide${NC}"
        exit 1
        ;;
esac
