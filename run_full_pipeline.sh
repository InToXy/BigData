#!/bin/bash

# Script maître pour exécuter l'ensemble du pipeline Data Lake
# Bronze → Silver → Gold (Delta Lake)

set -e  # Arrêter en cas d'erreur

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     PIPELINE DATA LAKE COMPLET - Bronze → Silver → Gold       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Fonction pour afficher le temps écoulé
START_TIME=$(date +%s)

function show_elapsed() {
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    MINUTES=$((ELAPSED / 60))
    SECONDS=$((ELAPSED % 60))
    echo "⏱️  Temps écoulé: ${MINUTES}m ${SECONDS}s"
}

# Fonction pour vérifier si un bucket contient des données
function check_bucket() {
    local bucket=$1
    local count=$(docker exec chu_minio mc ls myminio/$bucket/ 2>/dev/null | wc -l)
    echo $count
}

echo "═══════════════════════════════════════════════════════════════"
echo "ÉTAPE 1/3 : PIPELINE BRONZE (Ingestion des données brutes)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

BRONZE_COUNT=$(check_bucket "bronze")
if [ "$BRONZE_COUNT" -gt 0 ]; then
    echo "ℹ️  Le bucket Bronze contient déjà $BRONZE_COUNT tables"
    read -p "Voulez-vous re-exécuter le pipeline Bronze? (o/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Oo]$ ]]; then
        echo "⏭️  Pipeline Bronze ignoré"
    else
        echo "🚀 Exécution du pipeline Bronze..."
        ./run_bronze_ingestion.sh
        show_elapsed
    fi
else
    echo "🚀 Exécution du pipeline Bronze..."
    ./run_bronze_ingestion.sh
    show_elapsed
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "ÉTAPE 2/3 : PIPELINE SILVER (Transformation dimensionnelle)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

SILVER_COUNT=$(check_bucket "silver")
if [ "$SILVER_COUNT" -gt 0 ]; then
    echo "ℹ️  Le bucket Silver contient déjà $SILVER_COUNT tables"
    read -p "Voulez-vous re-exécuter le pipeline Silver? (o/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Oo]$ ]]; then
        echo "⏭️  Pipeline Silver ignoré"
    else
        echo "🚀 Exécution du pipeline Silver..."
        ./run_silver_transformation.sh
        show_elapsed
    fi
else
    echo "🚀 Exécution du pipeline Silver..."
    ./run_silver_transformation.sh
    show_elapsed
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "ÉTAPE 3/3 : PIPELINE GOLD (Agrégation des KPIs - Delta Lake)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

GOLD_COUNT=$(check_bucket "gold-delta")
if [ "$GOLD_COUNT" -gt 0 ]; then
    echo "ℹ️  Le bucket Gold-Delta contient déjà $GOLD_COUNT KPIs"
    read -p "Voulez-vous re-exécuter le pipeline Gold? (o/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Oo]$ ]]; then
        echo "⏭️  Pipeline Gold ignoré"
    else
        echo "🚀 Exécution du pipeline Gold Delta..."
        ./run_gold_delta.sh
        show_elapsed
    fi
else
    echo "🚀 Exécution du pipeline Gold Delta..."
    ./run_gold_delta.sh
    show_elapsed
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║               ✅ PIPELINE COMPLET TERMINÉ !                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

show_elapsed

echo ""
echo "📊 RÉSUMÉ DES DONNÉES:"
echo "   - Bronze: $(check_bucket bronze) tables"
echo "   - Silver: $(check_bucket silver) tables"
echo "   - Gold:   $(check_bucket gold-delta) KPIs"
echo ""
echo "🌐 Interface MinIO: http://localhost:9001"
echo "   Login: minioadmin / minioadmin123"
echo ""
echo "💡 Prochaines étapes:"
echo "   - Consulter les KPIs dans MinIO"
echo "   - Créer des dashboards (Superset, PowerBI, etc.)"
echo "   - Configurer l'orchestration Airflow"
echo ""
