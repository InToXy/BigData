#!/bin/bash
# Vérification rapide des tables Gold via Trino
# Plus léger que le script PySpark, utilise le moteur SQL Trino

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║        🔍 VÉRIFICATION RAPIDE TABLES GOLD (via Trino)            ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Vérifier que Trino est démarré
if ! docker ps | grep -q chu_trino; then
    echo -e "${RED}❌ Erreur: Container Trino (chu_trino) n'est pas démarré${NC}"
    echo "Démarrez-le avec: docker-compose up -d trino"
    exit 1
fi

echo -e "${GREEN}✅ Container Trino détecté${NC}"
echo ""

# Liste des tables à vérifier
TABLES=(
    "kpi_taux_consultation_periode"
    "kpi_consultation_par_diagnostic"
    "kpi_taux_hospitalisation_global"
    "kpi_hospitalisation_par_diagnostic"
    "kpi_hospitalisation_sexe_age"
    "kpi_consultation_par_professionnel"
    "kpi_deces_par_region_2019"
    "kpi_satisfaction_par_region_2020"
)

# Fonction pour exécuter une requête Trino
run_query() {
    docker exec chu_trino trino --server localhost:8080 --catalog minio --schema gold --output-format CSV_UNQUOTED --execute "$1" 2>/dev/null
}

# Vérifier l'accès au catalogue
echo -e "${BLUE}🔍 Vérification de l'accès au catalogue minio.gold...${NC}"
if ! run_query "SELECT 1" > /dev/null 2>&1; then
    echo -e "${RED}❌ Impossible d'accéder au catalogue minio.gold${NC}"
    echo "Exécutez: ./trino/init_trino_tables.sh"
    exit 1
fi
echo -e "${GREEN}✅ Accès au catalogue OK${NC}"
echo ""

# En-tête du rapport
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                    RAPPORT DE VÉRIFICATION                       ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

printf "%-45s %12s %10s %8s\n" "TABLE" "LIGNES" "COLONNES" "STATUS"
printf "%-45s %12s %10s %8s\n" "$(printf '%.0s─' {1..45})" "$(printf '%.0s─' {1..12})" "$(printf '%.0s─' {1..10})" "$(printf '%.0s─' {1..8})"

TOTAL_ROWS=0
TOTAL_COLS=0
TABLES_OK=0
TABLES_MISSING=0

# Vérifier chaque table
for table in "${TABLES[@]}"; do
    # Compter les lignes
    row_count=$(run_query "SELECT COUNT(*) FROM $table" 2>/dev/null | tail -1)
    
    if [ $? -eq 0 ] && [ ! -z "$row_count" ]; then
        # Compter les colonnes
        col_count=$(run_query "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'gold' AND table_name = '$table'" 2>/dev/null | tail -1)
        
        if [ -z "$col_count" ] || [ "$col_count" == "0" ]; then
            # Fallback: compter via DESCRIBE
            col_count=$(run_query "DESCRIBE $table" 2>/dev/null | wc -l)
        fi
        
        TOTAL_ROWS=$((TOTAL_ROWS + row_count))
        TOTAL_COLS=$((TOTAL_COLS + col_count))
        TABLES_OK=$((TABLES_OK + 1))
        
        # Statut
        if [ "$row_count" -eq 0 ]; then
            status="${YELLOW}⚠️ VIDE${NC}"
        else
            status="${GREEN}✅ OK${NC}"
        fi
        
        printf "%-45s %12s %10s " "$table" "$row_count" "$col_count"
        echo -e "$status"
    else
        TABLES_MISSING=$((TABLES_MISSING + 1))
        printf "%-45s %12s %10s " "$table" "N/A" "N/A"
        echo -e "${RED}❌ ABSENT${NC}"
    fi
done

# Synthèse
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                          SYNTHÈSE                                ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Statistiques globales:"
echo "   Tables attendues:    ${#TABLES[@]}"
echo "   Tables existantes:   $TABLES_OK"
echo "   Tables manquantes:   $TABLES_MISSING"
echo "   Lignes totales:      $TOTAL_ROWS"
echo "   Colonnes totales:    $TOTAL_COLS"
echo ""

if [ $TABLES_OK -eq ${#TABLES[@]} ]; then
    if [ $TOTAL_ROWS -gt 0 ]; then
        echo -e "${GREEN}✅ VALIDATION RÉUSSIE - Toutes les tables sont présentes et peuplées${NC}"
        AVG_ROWS=$((TOTAL_ROWS / TABLES_OK))
        echo "   Moyenne lignes/table: $AVG_ROWS"
        EXIT_CODE=0
    else
        echo -e "${YELLOW}⚠️  ATTENTION - Tables présentes mais vides${NC}"
        EXIT_CODE=2
    fi
else
    echo -e "${RED}❌ ÉCHEC - $TABLES_MISSING table(s) manquante(s)${NC}"
    EXIT_CODE=1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"

# Afficher les 5 premières lignes d'une table exemple (si demandé)
if [ "$1" == "--sample" ]; then
    echo ""
    echo "📄 Échantillon de données (kpi_taux_hospitalisation_global):"
    echo ""
    run_query "SELECT * FROM kpi_taux_hospitalisation_global LIMIT 5"
fi

exit $EXIT_CODE
