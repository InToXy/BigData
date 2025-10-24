#!/bin/bash
# Script d'initialisation des tables Trino pour la zone Gold
# Permet à PowerBI d'accéder aux données via SQL

echo "🔧 Initialisation des tables Trino pour PowerBI..."
echo "=================================================="

# Attendre que Trino soit prêt
echo "⏳ Attente du démarrage de Trino..."
sleep 30

# Fonction pour exécuter une requête Trino
run_trino_query() {
    docker exec -i chu_trino trino --server localhost:8080 --catalog $1 --schema $2 --execute "$3"
}

echo ""
echo "📊 Création du schéma Gold dans MinIO..."

# Créer le schéma gold dans le catalogue minio
run_trino_query "minio" "default" "CREATE SCHEMA IF NOT EXISTS gold WITH (location = 's3a://gold/')"

echo ""
echo "📋 Enregistrement des tables Gold existantes..."

# Liste des tables Gold à enregistrer
GOLD_TABLES=(
    "kpi_taux_consultation_periode"
    "kpi_consultation_par_diagnostic"
    "kpi_taux_hospitalisation_global"
    "kpi_hospitalisation_par_diagnostic"
    "kpi_hospitalisation_sexe_age"
    "kpi_consultation_par_professionnel"
    "kpi_deces_par_region_2019"
    "kpi_satisfaction_par_region_2020"
)

# Enregistrer chaque table Parquet comme table externe
for table in "${GOLD_TABLES[@]}"; do
    echo "  📌 Enregistrement de $table..."
    
    # Supprimer la table si elle existe déjà
    run_trino_query "minio" "gold" "DROP TABLE IF EXISTS $table" 2>/dev/null || true
    
    # Créer la table externe pointant vers les fichiers Parquet
    run_trino_query "minio" "gold" "
    CREATE TABLE IF NOT EXISTS $table (
        dummy VARCHAR
    )
    WITH (
        external_location = 's3a://gold/$table/',
        format = 'PARQUET'
    )"
done

echo ""
echo "🔍 Vérification des tables créées..."
run_trino_query "minio" "gold" "SHOW TABLES"

echo ""
echo "✅ Initialisation terminée !"
echo ""
echo "🌐 Accès Trino:"
echo "   URL: http://localhost:8090"
echo "   Web UI: http://localhost:8090/ui"
echo ""
echo "📊 Connexion PowerBI:"
echo "   Hôte: localhost"
echo "   Port: 8090"
echo "   Catalogue: minio"
echo "   Schéma: gold"
echo "   Authentication: None (ou username seulement)"
echo ""
echo "🧪 Test rapide:"
echo "   docker exec -it chu_trino trino --server localhost:8080"
echo "   USE minio.gold;"
echo "   SHOW TABLES;"
echo ""
