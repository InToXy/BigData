#!/bin/bash
###############################################################################
# setup_trino.sh
# Configure Trino pour accéder aux données Gold
###############################################################################

set -e

echo "======================================================================="
echo "🔍 CONFIGURATION TRINO - CHU DATA WAREHOUSE"
echo "======================================================================="
echo ""

# Vérifier que Trino est lancé
if ! docker ps --format '{{.Names}}' | grep -q "^chu_trino$"; then
    echo "❌ Le conteneur chu_trino n'est pas en cours d'exécution"
    echo "   Lancer: docker-compose up -d trino"
    exit 1
fi

echo "✅ Trino en cours d'exécution"
echo ""

# Attendre que Trino soit prêt
echo "⏳ Attente du démarrage de Trino (peut prendre 30-60 secondes)..."
for i in {1..30}; do
    if docker exec chu_trino curl -s http://localhost:8080/v1/info > /dev/null 2>&1; then
        echo "✅ Trino est prêt!"
        break
    fi
    echo "   Tentative $i/30..."
    sleep 2
done

echo ""
echo "======================================================================="
echo "🔧 Création du schéma et des tables Gold dans Trino"
echo "======================================================================="
echo ""

# Copier le script SQL dans le conteneur
docker cp ./trino/setup_trino_gold.sql chu_trino:/tmp/setup_trino_gold.sql

# Exécuter le script SQL
docker exec chu_trino trino --file /tmp/setup_trino_gold.sql

echo ""
echo "======================================================================="
echo "✅ CONFIGURATION TRINO TERMINÉE"
echo "======================================================================="
echo ""
echo "📊 Schéma créé: hive.chu_gold"
echo ""
echo "🎯 Tables KPI disponibles:"
echo "   - kpi_consultation_rate"
echo "   - kpi_hospitalisation_metrics"
echo "   - kpi_deces_by_region"
echo "   - kpi_satisfaction_global"
echo "   - kpi_activite_mensuelle"
echo "   - kpi_patient_demographics"
echo "   - kpi_etablissement_performance"
echo "   - kpi_temporal_trends"
echo ""
echo "📈 Vues créées:"
echo "   - v_consultations_recentes"
echo "   - v_activite_annuelle"
echo "   - v_dashboard_executif"
echo ""
echo "======================================================================="
echo "🔗 TESTER TRINO"
echo "======================================================================="
echo ""
echo "1. Interface Web:"
echo "   http://localhost:8090"
echo ""
echo "2. Client CLI:"
echo "   docker exec -it chu_trino trino"
echo "   USE hive.chu_gold;"
echo "   SHOW TABLES;"
echo "   SELECT * FROM kpi_consultation_rate LIMIT 10;"
echo ""
echo "3. Connexion Superset:"
echo "   URI: trino://trino@chu_trino:8080/hive/chu_gold"
echo ""
echo "======================================================================="

exit 0
