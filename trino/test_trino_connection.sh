#!/bin/bash
# Script de test de la connexion Trino
# Vérifie que Trino peut accéder aux données Gold

echo "🧪 TEST DE CONNEXION TRINO → ZONE GOLD"
echo "======================================"
echo ""

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction de test
test_query() {
    local description=$1
    local query=$2
    
    echo -n "  🔍 $description... "
    
    result=$(docker exec chu_trino trino --server localhost:8080 --execute "$query" 2>&1)
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ OK${NC}"
        return 0
    else
        echo -e "${RED}❌ ÉCHEC${NC}"
        echo "     Erreur: $result"
        return 1
    fi
}

# Vérifier que le conteneur Trino est démarré
echo "1️⃣ Vérification du conteneur Trino..."
if docker ps | grep -q chu_trino; then
    echo -e "   ${GREEN}✅ Conteneur chu_trino est en cours d'exécution${NC}"
else
    echo -e "   ${RED}❌ Conteneur chu_trino n'est pas démarré${NC}"
    echo "   Exécuter: docker-compose up -d trino"
    exit 1
fi

echo ""
echo "2️⃣ Test de connectivité Trino..."
test_query "Connexion au serveur Trino" "SELECT 1 as test"

echo ""
echo "3️⃣ Vérification des catalogues..."
test_query "Catalogue MinIO" "SHOW SCHEMAS FROM minio"
test_query "Catalogue PostgreSQL" "SHOW SCHEMAS FROM postgresql"
test_query "Catalogue Delta Lake" "SHOW SCHEMAS FROM deltalake"

echo ""
echo "4️⃣ Vérification du schéma Gold..."
test_query "Schéma minio.gold existe" "SHOW TABLES FROM minio.gold"

echo ""
echo "5️⃣ Test des tables Gold..."

# Liste des tables attendues
TABLES=(
    "kpi_taux_consultation_periode"
    "kpi_taux_hospitalisation_global"
    "kpi_hospitalisation_par_diagnostic"
)

for table in "${TABLES[@]}"; do
    test_query "Table $table accessible" "SELECT COUNT(*) FROM minio.gold.$table"
done

echo ""
echo "6️⃣ Test de requêtes complexes..."

test_query "Top 5 diagnostics" \
    "SELECT diagnostic_principal, nb_hospitalisations FROM minio.gold.kpi_hospitalisation_par_diagnostic ORDER BY nb_hospitalisations DESC LIMIT 5"

test_query "Statistiques hospitalisation" \
    "SELECT taux_hospitalisation FROM minio.gold.kpi_taux_hospitalisation_global LIMIT 1"

echo ""
echo "7️⃣ Informations système..."

echo "  📊 Version Trino:"
docker exec chu_trino trino --version 2>/dev/null | head -1

echo ""
echo "  📊 Catalogues disponibles:"
docker exec chu_trino trino --server localhost:8080 --execute "SHOW CATALOGS" 2>/dev/null | sed 's/^/     /'

echo ""
echo "  📊 Tables dans minio.gold:"
docker exec chu_trino trino --server localhost:8080 --execute "SHOW TABLES FROM minio.gold" 2>/dev/null | sed 's/^/     /'

echo ""
echo "======================================"
echo -e "${GREEN}✅ TESTS TERMINÉS${NC}"
echo ""
echo "🌐 Accès Web UI: http://localhost:8090/ui"
echo "🔌 Connexion PowerBI:"
echo "   - Hôte: localhost"
echo "   - Port: 8090"
echo "   - Catalogue: minio"
echo "   - Schéma: gold"
echo ""
