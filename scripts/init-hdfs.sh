#!/bin/bash

# Démarrer HDFS avec le script d'entrypoint original
exec /entrypoint.sh "$@"

# Attendre que le service soit disponible
echo "Attente du démarrage du service..."
sleep 10

# Vérifier si HDFS est accessible
MAX_TRIES=30
TRIES=0

while [ $TRIES -lt $MAX_TRIES ]; do
    if hdfs dfs -ls / > /dev/null 2>&1; then
        echo "HDFS est démarré et accessible!"
        break
    fi
    TRIES=$((TRIES+1))
    echo "Tentative $TRIES/$MAX_TRIES - En attente du démarrage de HDFS..."
    sleep 5
done

if [ $TRIES -eq $MAX_TRIES ]; then
    echo "ERREUR: HDFS n'a pas démarré après $MAX_TRIES tentatives"
    exit 1
fi

echo "HDFS est prêt, configuration des permissions..."

# Créer les répertoires nécessaires
hdfs dfs -mkdir -p /user/airflow/bronze
hdfs dfs -mkdir -p /user/airflow/silver
hdfs dfs -mkdir -p /user/airflow/gold

# Définir les permissions
hdfs dfs -chmod -R 755 /user
hdfs dfs -chown -R airflow:airflow /user/airflow

echo "Configuration HDFS terminée!"