#!/bin/bash
# Script de lancement rapide pour les tests d'ingestion

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║           🧪 TESTS D'INGESTION - BUCKET BRONZE                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Vérifier que MinIO est accessible
echo "🔍 Vérification de MinIO..."
if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9000/minio/health/live | grep -q "200"; then
    echo "   ✅ MinIO est accessible"
else
    echo "   ❌ MinIO n'est pas accessible"
    echo "   💡 Lancez: docker-compose up -d chu_minio"
    exit 1
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "📊 Lancement du test de la table 'consultations'"
echo "══════════════════════════════════════════════════════════════════"
echo ""

python3 test_consultation_bronze.py

echo ""
echo "══════════════════════════════════════════════════════════════════"

if [ $? -eq 0 ]; then
    echo "✅ Test terminé avec succès"
else
    echo "❌ Le test a échoué"
    exit 1
fi
