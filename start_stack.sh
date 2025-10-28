#!/bin/bash

echo "╔═══════════════════════════════════════════════════════╗"
echo "║     🚀 DÉMARRAGE STACK HEALTHCARE + AIRFLOW           ║"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

# Vérifier si Docker est actif
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker n'est pas démarré. Veuillez lancer Docker Desktop."
    exit 1
fi

echo "📦 Démarrage des conteneurs..."
docker-compose up -d

echo ""
echo "⏳ Attente de l'initialisation des services..."
sleep 10

echo ""
echo "🔍 Vérification des conteneurs..."
echo ""

# Fonction pour vérifier un conteneur
check_container() {
    local name=$1
    local port=$2
    
    if docker ps | grep -q "$name"; then
        echo "  ✅ $name - En cours d'exécution"
        if [ ! -z "$port" ]; then
            echo "     → http://localhost:$port"
        fi
    else
        echo "  ❌ $name - Non démarré"
    fi
}

# Vérifier chaque service
check_container "chu_minio" "9001"
check_container "chu_postgres" "5432"
check_container "chu_jupyter" "8888"
check_container "chu_airflow_webserver" "8080"
check_container "chu_airflow_scheduler" ""
check_container "chu_superset" "8088"
check_container "chu_trino" "8082"

echo ""
echo "╔═══════════════════════════════════════════════════════╗"
echo "║                 🎯 ACCÈS AUX SERVICES                 ║"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""
echo "  🌀 Airflow    : http://localhost:8081"
echo "                  User: admin / Pass: admin123"
echo ""
echo "  📊 Superset   : http://localhost:8088"
echo "                  User: admin / Pass: admin123"
echo ""
echo "  📦 MinIO      : http://localhost:9001"
echo "                  User: minioadmin / Pass: minioadmin123"
echo ""
echo "  📓 Jupyter    : http://localhost:8888"
echo ""
echo "  🔍 Trino      : http://localhost:8082"
echo ""
echo "╔═══════════════════════════════════════════════════════╗"
echo "║              📋 PROCHAINES ÉTAPES                     ║"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""
echo "1. Accéder à Airflow: http://localhost:8081"
echo "2. Se connecter avec admin/admin123"
echo "3. Activer le DAG 'healthcare_pipeline_complete'"
echo "4. Cliquer sur 'Trigger DAG' pour lancer le pipeline"
echo "5. Suivre l'exécution dans la Graph View"
echo ""
echo "📚 Documentation: dags/README_AIRFLOW.md"
echo ""
