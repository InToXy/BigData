#!/bin/bash

echo "╔══════════════════════════════════════════════════╗"
echo "║    INITIALISATION COMPLÈTE SUPERSET             ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Arrêter Superset
echo "🛑 Arrêt de Superset..."
docker stop chu_superset chu_superset_init 2>/dev/null
docker rm chu_superset chu_superset_init 2>/dev/null

# Nettoyer la base
echo "🗑️  Nettoyage de la base de données..."
docker exec chu_superset_db psql -U superset -d superset -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO superset;"

echo ""
echo "🚀 Redémarrage de Superset avec initialisation..."

# Démarrer Superset avec init
docker run -d \
  --name chu_superset_init \
  --network bigdata_network \
  -e SUPERSET_SECRET_KEY='CHU_secret_key_2024_super_secure' \
  -e DATABASE_DIALECT=postgresql \
  -e DATABASE_USER=superset \
  -e DATABASE_PASSWORD=superset \
  -e DATABASE_HOST=chu_superset_db \
  -e DATABASE_PORT=5432 \
  -e DATABASE_DB=superset \
  apache/superset:3.0.0 \
  bash -c "
    superset db upgrade && \
    superset fab create-admin \
      --username admin \
      --firstname Admin \
      --lastname User \
      --email admin@superset.com \
      --password admin123 && \
    superset init
  "

echo "⏳ Attente de l'initialisation (30 secondes)..."
sleep 30

# Vérifier les logs
echo ""
echo "📋 Logs d'initialisation:"
docker logs chu_superset_init 2>&1 | tail -20

# Arrêter le conteneur d'init
docker stop chu_superset_init 2>/dev/null
docker rm chu_superset_init 2>/dev/null

# Démarrer Superset normal
echo ""
echo "🚀 Démarrage de Superset..."
docker run -d \
  --name chu_superset \
  --network bigdata_network \
  -p 8088:8088 \
  -e SUPERSET_SECRET_KEY='CHU_secret_key_2024_super_secure' \
  -e DATABASE_DIALECT=postgresql \
  -e DATABASE_USER=superset \
  -e DATABASE_PASSWORD=superset \
  -e DATABASE_HOST=chu_superset_db \
  -e DATABASE_PORT=5432 \
  -e DATABASE_DB=superset \
  apache/superset:3.0.0

echo "⏳ Attente du démarrage (10 secondes)..."
sleep 10

# Vérifier l'état
echo ""
echo "🔍 Vérification de Superset..."
if docker ps | grep -q chu_superset; then
    echo "✅ Superset démarré"
    
    # Vérifier l'utilisateur
    echo ""
    echo "👤 Vérification de l'utilisateur admin..."
    docker exec chu_superset_db psql -U superset -d superset -c "SELECT username, email, active FROM ab_user WHERE username = 'admin';" 2>/dev/null
    
    echo ""
    echo "╔══════════════════════════════════════════════════╗"
    echo "║         ✅ SUPERSET PRÊT                         ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""
    echo "🌐 Accès:"
    echo "   URL:      http://localhost:8088"
    echo "   Username: admin"
    echo "   Password: admin123"
    echo ""
    echo "📊 Prochaines étapes:"
    echo "   1. Ouvrir http://localhost:8088"
    echo "   2. Se connecter avec admin / admin123"
    echo "   3. Data > Databases > + Database"
    echo "   4. Sélectionner: Trino"
    echo "   5. URI: trino://trino@chu_trino:8080/hive/chu_gold"
    echo ""
else
    echo "❌ Échec du démarrage"
    echo "📋 Logs:"
    docker logs chu_superset 2>&1 | tail -30
    exit 1
fi
