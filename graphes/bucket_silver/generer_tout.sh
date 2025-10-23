#!/bin/bash
# Script d'automatisation complète - Analyse de performance Silver Layer

echo "🚀 Démarrage de l'analyse de performance complète - SILVER LAYER"
echo "=================================================================="
echo ""

# Vérifier que MinIO est accessible
echo "1️⃣  Vérification de la connexion MinIO..."
if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9000/minio/health/live | grep -q "200"; then
    echo "   ✅ MinIO est accessible"
else
    echo "   ❌ MinIO n'est pas accessible sur http://127.0.0.1:9000"
    echo "   💡 Lancez d'abord 'docker-compose up -d' depuis le répertoire BigData"
    exit 1
fi

echo ""
echo "2️⃣  Lancement de l'analyse de performance..."
python3 performance_minio.py

if [ $? -eq 0 ]; then
    echo ""
    echo "3️⃣  Génération du rapport HTML..."
    python3 generer_rapport.py
    
    echo ""
    echo "=================================================================="
    echo "✅ Analyse complète terminée avec succès!"
    echo ""
    echo "📁 Fichiers générés:"
    echo "   • 9 graphiques PNG (1_*.png à 8_*.png)"
    echo "   • 1 rapport HTML interactif (rapport_performance.html)"
    echo ""
    echo "🌐 Pour visualiser le rapport:"
    echo "   file:///home/alban/BigData/BigData/graphes/bucket_silver/rapport_performance.html"
    echo ""
    echo "=================================================================="
else
    echo ""
    echo "❌ Erreur lors de l'analyse de performance"
    exit 1
fi
