#!/bin/bash

echo "╔══════════════════════════════════════════════════╗"
echo "║       PIPELINE COMPLET - SILVER → GOLD → TRINO  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Fonction pour surveiller Silver
wait_for_silver() {
    echo "⏳ Attente de la fin du job Silver..."
    local max_wait=300  # 5 minutes max
    local elapsed=0
    
    while [ $elapsed -lt $max_wait ]; do
        if tail -20 silver_output.log 2>/dev/null | grep -q "TRANSFORMATION SILVER TERMINÉE"; then
            echo "✅ Job Silver terminé avec succès!"
            return 0
        fi
        
        if tail -20 silver_output.log 2>/dev/null | grep -qi "error\|exception\|failed"; then
            echo "❌ Erreur détectée dans le job Silver"
            tail -50 silver_output.log
            return 1
        fi
        
        sleep 10
        elapsed=$((elapsed + 10))
        
        # Afficher progression tous les 30 secondes
        if [ $((elapsed % 30)) -eq 0 ]; then
            last_line=$(tail -1 silver_output.log 2>/dev/null | grep -oE "(dim_|fact_|metrique_)[a-z_]+" | tail -1)
            if [ -n "$last_line" ]; then
                echo "   📊 En cours: $last_line... (${elapsed}s)"
            fi
        fi
    done
    
    echo "⚠️  Timeout atteint (${max_wait}s)"
    return 1
}

# Vérifier que Silver est en cours
if ! pgrep -f "silver_transformation_clean.py" > /dev/null; then
    echo "⚠️  Job Silver non détecté, lancement..."
    nohup docker exec chu_jupyter spark-submit \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=8 \
        --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        /home/jovyan/jobs/main_jobs/silver_transformation_clean.py > silver_output.log 2>&1 &
    
    sleep 5
fi

# Attendre Silver
if wait_for_silver; then
    echo ""
    echo "╔══════════════════════════════════════════════════╗"
    echo "║           LANCEMENT JOB GOLD                     ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""
    
    # Lancer Gold
    docker exec chu_jupyter spark-submit \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=8 \
        --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.driver.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.executor.extraClassPath=/home/jovyan/jars/hadoop-aws-3.3.4.jar:/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
        /home/jovyan/jobs/main_jobs/gold_aggregation_clean.py
    
    GOLD_EXIT=$?
    
    if [ $GOLD_EXIT -eq 0 ]; then
        echo ""
        echo "╔══════════════════════════════════════════════════╗"
        echo "║         CONFIGURATION TRINO                      ║"
        echo "╚══════════════════════════════════════════════════╝"
        echo ""
        
        # Attendre que Trino soit prêt
        echo "🔍 Vérification de Trino..."
        for i in {1..30}; do
            if docker exec chu_trino trino --execute "SHOW CATALOGS" > /dev/null 2>&1; then
                echo "✅ Trino est prêt"
                break
            fi
            if [ $i -eq 30 ]; then
                echo "⚠️  Trino non accessible, continuons quand même..."
            fi
            sleep 2
        done
        
        # Exécuter le setup SQL
        echo "📊 Création des tables Trino Gold..."
        docker exec chu_trino trino --file /etc/trino/setup_trino_gold.sql
        
        echo ""
        echo "╔══════════════════════════════════════════════════╗"
        echo "║         ✅ PIPELINE COMPLET TERMINÉ              ║"
        echo "╚══════════════════════════════════════════════════╝"
        echo ""
        echo "📊 Services disponibles:"
        echo "   • Trino:    http://localhost:8090"
        echo "   • Superset: http://localhost:8088 (admin/admin123)"
        echo "   • MinIO:    http://localhost:9001 (minioadmin/minioadmin123)"
        echo ""
        echo "📝 Prochaines étapes:"
        echo "   1. Connecter Superset à Trino:"
        echo "      URI: trino://trino@chu_trino:8080/hive/chu_gold"
        echo ""
        echo "   2. Requêtes SQL exemple:"
        echo "      docker exec chu_trino trino --execute \"SELECT * FROM hive.chu_gold.kpi_consultation_rate LIMIT 10\""
        echo ""
    else
        echo "❌ Échec du job Gold"
        exit 1
    fi
else
    echo "❌ Échec du job Silver"
    exit 1
fi
