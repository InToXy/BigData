#!/bin/bash

echo "╔══════════════════════════════════════════════════╗"
echo "║       RÉINITIALISATION SUPERSET                  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# 1. Créer le réseau si nécessaire
docker network inspect bigdata_network >/dev/null 2>&1 || docker network create bigdata_network

# 2. S'assurer que la DB est démarrée
if ! docker ps | grep -q chu_superset_db; then
    echo "❌ Base de données Superset non démarrée"
    exit 1
fi

echo "✅ Base de données Superset active"
echo ""

# 3. Init Superset
echo "📦 Initialisation de Superset..."
docker run --rm \
    --name chu_superset_init \
    --network bigdata_network \
    -e SQLALCHEMY_DATABASE_URI="postgresql://superset:superset123@chu_superset_db:5432/superset" \
    -e SUPERSET_SECRET_KEY='thisISaSECRET_1234567890abcdefghijklmnopqrstuvwxyz' \
    apache/superset:3.0.0 \
    sh -c "
        echo 'Migration de la base de données...'
        superset db upgrade
        
        echo 'Création de l utilisateur admin...'
        superset fab create-admin \
            --username admin \
            --firstname Admin \
            --lastname User \
            --email admin@chu.com \
            --password admin123
        
        echo 'Initialisation Superset...'
        superset init
        
        echo '✅ Initialisation terminée'
    "

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Initialisation réussie"
    echo ""
    
    # 4. Démarrer Superset
    echo "🚀 Démarrage de Superset..."
    docker run -d \
        --name chu_superset \
        --network bigdata_network \
        -p 8088:8088 \
        -e SQLALCHEMY_DATABASE_URI="postgresql://superset:superset123@chu_superset_db:5432/superset" \
        -e SUPERSET_SECRET_KEY='thisISaSECRET_1234567890abcdefghijklmnopqrstuvwxyz' \
        -e SUPERSET_ENV=production \
        -e SUPERSET_PORT=8088 \
        --restart unless-stopped \
        apache/superset:3.0.0
    
    echo ""
    echo "⏳ Attente du démarrage (30s)..."
    sleep 30
    
    # 5. Vérifier le statut
    if docker ps | grep -q chu_superset; then
        echo ""
        echo "╔══════════════════════════════════════════════════╗"
        echo "║         ✅ SUPERSET DÉMARRÉ                      ║"
        echo "╚══════════════════════════════════════════════════╝"
        echo ""
        echo "🌐 URL:  http://localhost:8088"
        echo "👤 Login: admin"
        echo "🔑 Pass:  admin123"
        echo ""
        echo "📊 Logs: docker logs -f chu_superset"
    else
        echo "❌ Échec du démarrage"
        docker logs chu_superset --tail 50
        exit 1
    fi
else
    echo "❌ Échec de l'initialisation"
    exit 1
fi
