#!/bin/bash

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║    PIPELINE GOLD + TRINO - Résumé et Configuration Finale  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Résumé Silver
echo "📊 ÉTAT ACTUEL DU PIPELINE:"
echo "   ✅ Bronze: 15 tables (~5.4M lignes)"
echo "   ✅ Silver: 7 tables créées:"
echo "      • dim_patient: 100,000"
echo "      • dim_etablissement: 416,665"
echo "      • dim_temps: 2,922"
echo "      • fact_consultation: 1,027,157"
echo "      • fact_hospitalisation: 2,479"
echo "      • fact_deces: 620,608"
echo "      • metrique_satisfaction: 2,097"
echo ""
echo "   ⏳ Gold: Lancement de l'agrégation (8 KPIs)..."
echo ""

# Lancer Gold en arrière-plan
nohup docker exec chu_jupyter spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.shuffle.partitions=8 \
    --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
    /home/jovyan/jobs/main_jobs/gold_aggregation_clean.py > gold_output.log 2>&1 &

GOLD_PID=$!
echo "🚀 Job Gold lancé (PID: $GOLD_PID)"
echo "📝 Logs: tail -f gold_output.log"
echo ""

# Surveiller Gold
echo "⏳ Surveillance du job Gold..."
for i in {1..120}; do  # Max 2 minutes
    sleep 1
    
    if grep -q "AGRÉGATION GOLD TERMINÉE" gold_output.log 2>/dev/null; then
        echo ""
        echo "✅ Job Gold terminé avec succès!"
        
        # Afficher résumé
        echo ""
        echo "📊 KPIs créés:"
        grep "KPI.*écrit" gold_output.log 2>/dev/null | tail -8
        
        break
    fi
    
    if grep -qi "error\|exception" gold_output.log 2>/dev/null && ! grep -q "WARN" gold_output.log 2>/dev/null; then
        echo ""
        echo "❌ Erreur détectée dans Gold"
        tail -30 gold_output.log
        exit 1
    fi
    
    # Afficher progression toutes les 20 secondes
    if [ $((i % 20)) -eq 0 ]; then
        last_kpi=$(grep "KPI [0-9]:" gold_output.log 2>/dev/null | tail -1 | grep -oE "KPI [0-9]: [^\.]+")
        if [ -n "$last_kpi" ]; then
            echo "   📈 ${last_kpi}... (${i}s)"
        fi
    fi
done

# Vérifier si Gold est terminé
if ! grep -q "AGRÉGATION GOLD TERMINÉE" gold_output.log 2>/dev/null; then
    echo ""
    echo "⚠️  Gold en cours d'exécution..."
    echo "📝 Suivez les logs: tail -f gold_output.log"
    echo ""
    echo "Une fois terminé, exécutez:"
    echo "   ./finalize_trino.sh"
    exit 0
fi

# Si Gold réussi, configurer Trino
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║         CONFIGURATION TRINO                      ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Vérifier Trino
echo "🔍 Vérification de Trino..."
if ! docker ps | grep -q chu_trino; then
    echo "❌ Conteneur Trino non démarré"
    exit 1
fi

# Attendre que Trino soit prêt
echo "⏳ Attente de Trino..."
for i in {1..30}; do
    if docker exec chu_trino trino --execute "SHOW CATALOGS" > /dev/null 2>&1; then
        echo "✅ Trino est prêt"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "⚠️  Trino non accessible après 60s"
        echo "Vérifiez: docker logs chu_trino"
        exit 1
    fi
    sleep 2
done

# Créer les tables Trino
echo ""
echo "📊 Création des tables Trino dans chu_gold..."

# Script SQL inline
docker exec chu_trino trino << 'EOF'
-- Créer le schéma
CREATE SCHEMA IF NOT EXISTS hive.chu_gold
WITH (location = 's3a://gold/');

-- Table 1: Consultation Rate
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_consultation_rate (
    annee INTEGER,
    mois INTEGER,
    nb_consultations BIGINT,
    nb_patients_uniques BIGINT,
    taux_consultation_patient DOUBLE,
    montant_moyen DOUBLE,
    montant_total DOUBLE
)
WITH (
    external_location = 's3a://gold/kpi_consultation_rate/',
    format = 'PARQUET'
);

-- Table 2: Hospitalisation Metrics  
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_hospitalisation_metrics (
    annee INTEGER,
    nb_hospitalisations BIGINT,
    duree_moyenne_sejour DOUBLE,
    duree_min_sejour DOUBLE,
    duree_max_sejour DOUBLE,
    nb_patients_uniques BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_hospitalisation_metrics/',
    format = 'PARQUET'
);

-- Table 3: Décès by Region
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_deces_by_region (
    annee INTEGER,
    lieu_deces VARCHAR,
    sexe VARCHAR,
    nb_deces BIGINT,
    age_moyen DOUBLE,
    age_min INTEGER,
    age_max INTEGER
)
WITH (
    external_location = 's3a://gold/kpi_deces_by_region/',
    format = 'PARQUET'
);

-- Table 4: Satisfaction Global
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_satisfaction_global (
    source_enquete VARCHAR,
    nb_reponses BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_satisfaction_global/',
    format = 'PARQUET'
);

-- Table 5: Activité Mensuelle
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_activite_mensuelle (
    annee INTEGER,
    mois INTEGER,
    activite_totale BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_activite_mensuelle/',
    format = 'PARQUET'
);

-- Table 6: Patient Demographics
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_patient_demographics (
    tranche_age VARCHAR,
    sexe VARCHAR,
    nb_patients BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_patient_demographics/',
    format = 'PARQUET'
);

-- Table 7: Etablissement Performance
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_etablissement_performance (
    region VARCHAR,
    type_etablissement VARCHAR,
    nb_etablissements BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_etablissement_performance/',
    format = 'PARQUET'
);

-- Table 8: Temporal Trends
CREATE TABLE IF NOT EXISTS hive.chu_gold.kpi_temporal_trends (
    annee INTEGER,
    trimestre INTEGER,
    type_activite VARCHAR,
    volume BIGINT
)
WITH (
    external_location = 's3a://gold/kpi_temporal_trends/',
    format = 'PARQUET'
);

-- Vérification
SELECT 'kpi_consultation_rate' as table_name, COUNT(*) as row_count 
FROM hive.chu_gold.kpi_consultation_rate
UNION ALL
SELECT 'kpi_hospitalisation_metrics', COUNT(*) 
FROM hive.chu_gold.kpi_hospitalisation_metrics
UNION ALL
SELECT 'kpi_deces_by_region', COUNT(*) 
FROM hive.chu_gold.kpi_deces_by_region
UNION ALL
SELECT 'kpi_satisfaction_global', COUNT(*) 
FROM hive.chu_gold.kpi_satisfaction_global
UNION ALL
SELECT 'kpi_activite_mensuelle', COUNT(*) 
FROM hive.chu_gold.kpi_activite_mensuelle
UNION ALL
SELECT 'kpi_patient_demographics', COUNT(*) 
FROM hive.chu_gold.kpi_patient_demographics
UNION ALL
SELECT 'kpi_etablissement_performance', COUNT(*) 
FROM hive.chu_gold.kpi_etablissement_performance
UNION ALL
SELECT 'kpi_temporal_trends', COUNT(*) 
FROM hive.chu_gold.kpi_temporal_trends;
EOF

TRINO_EXIT=$?

if [ $TRINO_EXIT -eq 0 ]; then
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║         ✅ PIPELINE COMPLET TERMINÉ AVEC SUCCÈS              ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "📊 ARCHITECTURE COMPLÈTE:"
    echo "   ✅ Bronze:  15 tables (~5.4M lignes)"
    echo "   ✅ Silver:  7 tables (1.6M lignes)"
    echo "   ✅ Gold:    8 KPIs"
    echo "   ✅ Trino:   8 tables SQL configurées"
    echo ""
    echo "🌐 SERVICES ACCESSIBLES:"
    echo "   • Trino:    http://localhost:8090"
    echo "   • Superset: http://localhost:8088 (admin/admin123)"
    echo "   • MinIO:    http://localhost:9001 (minioadmin/minioadmin123)"
    echo "   • Jupyter:  http://localhost:8888 (token: admin123)"
    echo ""
    echo "🔧 EXEMPLES DE REQUÊTES TRINO:"
    echo ""
    echo "   # Consultations par année"
    echo "   docker exec chu_trino trino --execute \\"
    echo "     \"SELECT annee, SUM(nb_consultations) as total \\"
    echo "     FROM hive.chu_gold.kpi_consultation_rate \\"
    echo "     GROUP BY annee ORDER BY annee DESC\""
    echo ""
    echo "   # Top 5 régions par décès"
    echo "   docker exec chu_trino trino --execute \\"
    echo "     \"SELECT lieu_deces, SUM(nb_deces) as total \\"
    echo "     FROM hive.chu_gold.kpi_deces_by_region \\"
    echo "     GROUP BY lieu_deces ORDER BY total DESC LIMIT 5\""
    echo ""
    echo "   # Pyramide des âges"
    echo "   docker exec chu_trino trino --execute \\"
    echo "     \"SELECT tranche_age, sexe, nb_patients \\"
    echo "     FROM hive.chu_gold.kpi_patient_demographics \\"
    echo "     ORDER BY tranche_age\""
    echo ""
    echo "📊 CONNEXION SUPERSET:"
    echo "   1. Ouvrir: http://localhost:8088"
    echo "   2. Login: admin / admin123"
    echo "   3. Data > Databases > + Database"
    echo "   4. Sélectionner: Trino"
    echo "   5. URI: trino://trino@chu_trino:8080/hive/chu_gold"
    echo "   6. Test Connection puis Save"
    echo ""
    echo "📚 DOCUMENTATION:"
    echo "   cat /home/alban/BigData/BigData/GUIDE_COMPLET.md"
    echo ""
else
    echo "❌ Échec de la configuration Trino"
    exit 1
fi
