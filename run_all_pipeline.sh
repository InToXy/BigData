#!/bin/bash
###############################################################################
# run_all_pipeline.sh
# Exécute le pipeline complet Bronze → Silver → Gold
###############################################################################

set -e

echo "======================================================================="
echo "🏥 PIPELINE COMPLET CHU DATA WAREHOUSE"
echo "======================================================================="
echo "📅 Début: $(date)"
echo ""
echo "Étapes:"
echo "  1. Bronze - Ingestion données brutes"
echo "  2. Silver - Transformation schéma en étoile"
echo "  3. Gold - Agrégation KPIs"
echo ""
echo "======================================================================="

# Étape 1: Bronze
echo ""
echo "🟤 ÉTAPE 1/3: BRONZE INGESTION"
echo "======================================================================="
if [ -f "./run_bronze.sh" ]; then
    chmod +x ./run_bronze.sh
    ./run_bronze.sh
    if [ $? -ne 0 ]; then
        echo "❌ Échec Bronze - Arrêt du pipeline"
        exit 1
    fi
else
    echo "⚠️  run_bronze.sh non trouvé, passage à Silver..."
fi

# Étape 2: Silver
echo ""
echo "🔵 ÉTAPE 2/3: SILVER TRANSFORMATION"
echo "======================================================================="
if [ -f "./run_silver.sh" ]; then
    chmod +x ./run_silver.sh
    ./run_silver.sh
    if [ $? -ne 0 ]; then
        echo "❌ Échec Silver - Arrêt du pipeline"
        exit 1
    fi
else
    echo "❌ run_silver.sh non trouvé"
    exit 1
fi

# Étape 3: Gold
echo ""
echo "🏆 ÉTAPE 3/3: GOLD AGGREGATION"
echo "======================================================================="
if [ -f "./run_gold.sh" ]; then
    chmod +x ./run_gold.sh
    ./run_gold.sh
    if [ $? -ne 0 ]; then
        echo "❌ Échec Gold - Pipeline incomplet"
        exit 1
    fi
else
    echo "❌ run_gold.sh non trouvé"
    exit 1
fi

# Résumé final
echo ""
echo "======================================================================="
echo "✅ PIPELINE COMPLET TERMINÉ AVEC SUCCÈS"
echo "======================================================================="
echo "📅 Fin: $(date)"
echo ""
echo "📊 Architecture créée:"
echo ""
echo "  🟤 BRONZE (s3a://bronze/)"
echo "     - Tables brutes normalisées: 15+ tables"
echo "     - Volume: ~5.4M lignes"
echo ""
echo "  🔵 SILVER (s3a://silver/)"
echo "     - Dimensions: dim_patient, dim_etablissement, dim_temps"
echo "     - Faits: fact_consultation, fact_hospitalisation, fact_deces"
echo "     - Métriques: metrique_satisfaction"
echo ""
echo "  🏆 GOLD (s3a://gold/)"
echo "     - 8 KPIs métiers prêts pour visualisation"
echo ""
echo "======================================================================="
echo "🔗 ACCÈS AUX SERVICES"
echo "======================================================================="
echo ""
echo "  📊 Superset (Dashboards):"
echo "     URL: http://localhost:8088"
echo "     Login: admin / admin123"
echo ""
echo "  🔍 Trino (Query Engine):"
echo "     URL: http://localhost:8090"
echo "     Pour se connecter: docker exec -it chu_trino trino"
echo ""
echo "  📓 Jupyter Lab:"
echo "     URL: http://localhost:8888"
echo "     Token: admin123"
echo ""
echo "  🗄️  MinIO (Data Lake):"
echo "     URL: http://localhost:9001"
echo "     Login: minioadmin / minioadmin123"
echo ""
echo "======================================================================="
echo "📚 PROCHAINES ÉTAPES"
echo "======================================================================="
echo ""
echo "  1. Créer les tables Hive pour Trino:"
echo "     docker exec -it chu_trino trino"
echo "     CREATE SCHEMA hive.chu_gold WITH (location = 's3a://gold/');"
echo ""
echo "  2. Connecter Superset à Trino:"
echo "     - Aller sur http://localhost:8088"
echo "     - Data > Databases > + Database"
echo "     - SQLAlchemy URI: trino://trino@chu_trino:8080/hive"
echo ""
echo "  3. Créer des dashboards dans Superset à partir des KPIs Gold"
echo ""
echo "======================================================================="

exit 0
