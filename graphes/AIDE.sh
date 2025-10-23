#!/bin/bash
# AIDE RAPIDE - Commandes essentielles pour les analyses de performance

cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════════╗
║                   📊 ANALYSES DE PERFORMANCE MINIO                       ║
║                   Aide Rapide - Commandes Essentielles                   ║
╚══════════════════════════════════════════════════════════════════════════╝

🚀 COMMANDES PRINCIPALES
═══════════════════════════════════════════════════════════════════════════

1️⃣  Analyser tout (Bronze + Silver)
    ./analyser_tout.sh

2️⃣  Analyser uniquement Bronze
    cd bucket_bronze && ./generer_tout.sh

3️⃣  Analyser uniquement Silver
    cd bucket_silver && ./generer_tout.sh

4️⃣  Générer seulement les graphiques (sans rapport HTML)
    cd bucket_bronze && python3 performance_minio.py
    cd bucket_silver && python3 performance_minio.py

5️⃣  Générer seulement le rapport HTML (graphiques déjà créés)
    cd bucket_bronze && python3 generer_rapport.py
    cd bucket_silver && python3 generer_rapport.py

═══════════════════════════════════════════════════════════════════════════
📖 DOCUMENTATION
═══════════════════════════════════════════════════════════════════════════

📄 Vue d'ensemble
    cat README.md

📄 Comparaison Bronze vs Silver
    cat COMPARAISON_BRONZE_SILVER.md

📄 Index global de tous les fichiers
    cat INDEX_GLOBAL.md

📄 Guide complet Bronze
    cat bucket_bronze/GUIDE_COMPLET.md

📄 Guide complet Silver
    cat bucket_silver/GUIDE_COMPLET.md

📄 Documentation technique des graphiques (Bronze)
    cat bucket_bronze/README_GRAPHIQUES.md

📄 Documentation technique des graphiques (Silver)
    cat bucket_silver/README_GRAPHIQUES.md

═══════════════════════════════════════════════════════════════════════════
🌐 VISUALISATION DES RAPPORTS
═══════════════════════════════════════════════════════════════════════════

🔗 Ouvrir le rapport Bronze dans le navigateur
    xdg-open bucket_bronze/rapport_performance.html
    # OU
    firefox bucket_bronze/rapport_performance.html

🔗 Ouvrir le rapport Silver dans le navigateur
    xdg-open bucket_silver/rapport_performance.html
    # OU
    firefox bucket_silver/rapport_performance.html

🔗 URLs directes (copier/coller dans le navigateur)
    Bronze: file:///home/alban/BigData/BigData/graphes/bucket_bronze/rapport_performance.html
    Silver: file:///home/alban/BigData/BigData/graphes/bucket_silver/rapport_performance.html

═══════════════════════════════════════════════════════════════════════════
🔧 VÉRIFICATIONS ET DIAGNOSTICS
═══════════════════════════════════════════════════════════════════════════

✅ Vérifier que MinIO est accessible
    curl http://127.0.0.1:9000/minio/health/live

✅ Lister les buckets MinIO
    docker exec chu_minio mc ls local/

✅ Vérifier le contenu du bucket Bronze
    docker exec chu_minio mc ls local/bronze/

✅ Vérifier le contenu du bucket Silver
    docker exec chu_minio mc ls local/silver/

✅ Vérifier les conteneurs Docker
    docker ps | grep chu

✅ Démarrer MinIO si nécessaire
    cd /home/alban/BigData/BigData
    docker-compose up -d chu_minio

✅ Voir les logs MinIO
    docker logs chu_minio

✅ Vérifier les packages Python
    python3 -c "import boto3, pyarrow, pandas, matplotlib, seaborn, numpy; print('✅ Tous les packages sont installés')"

✅ Installer les dépendances manquantes
    pip install boto3 pyarrow pandas matplotlib seaborn numpy

═══════════════════════════════════════════════════════════════════════════
📊 GRAPHIQUES GÉNÉRÉS
═══════════════════════════════════════════════════════════════════════════

Pour chaque bucket (Bronze et Silver), 9 graphiques sont créés :

1. 1_temps_reponse_barres.png           - Temps de réponse par dataset
2. 2_evolution_temporelle_courbes.png   - Évolution cache (cold/warm/hot)
3. 3_distribution_histogramme.png       - Distribution des temps
4. 4_dispersion_boxplot.png             - Dispersion par type de requête
5. 4b_dispersion_boxplot_datasets.png   - Dispersion par dataset (top 15)
6. 5_correlation_scatter.png            - Corrélation volume/temps
7. 6_heatmap_latence.png                - Carte thermique des latences
8. 7_performance_debit.png              - Débit (lignes/seconde)
9. 8_dashboard_complet.png              - Dashboard récapitulatif complet

═══════════════════════════════════════════════════════════════════════════
🏗️ STRUCTURE DES DOSSIERS
═══════════════════════════════════════════════════════════════════════════

graphes/
├── analyser_tout.sh              # Script global Bronze + Silver
├── README.md                     # Guide principal
├── COMPARAISON_BRONZE_SILVER.md  # Différences Bronze/Silver
├── INDEX_GLOBAL.md               # Index complet des fichiers
├── AIDE.sh                       # Ce fichier
│
├── bucket_bronze/                # Analyses Bronze
│   ├── performance_minio.py
│   ├── generer_rapport.py
│   ├── generer_tout.sh
│   ├── README.md
│   ├── GUIDE_COMPLET.md
│   ├── INDEX.md
│   ├── README_GRAPHIQUES.md
│   └── [graphiques générés]
│
└── bucket_silver/                # Analyses Silver
    ├── performance_minio.py
    ├── generer_rapport.py
    ├── generer_tout.sh
    ├── README.md
    ├── GUIDE_COMPLET.md
    ├── INDEX.md
    ├── README_GRAPHIQUES.md
    └── [graphiques générés]

═══════════════════════════════════════════════════════════════════════════
🔥 EXEMPLES D'UTILISATION
═══════════════════════════════════════════════════════════════════════════

# Scénario 1 : Première utilisation
./analyser_tout.sh

# Scénario 2 : Analyser seulement les nouvelles données Bronze
cd bucket_bronze && python3 performance_minio.py

# Scénario 3 : Comparer les performances Bronze vs Silver
./analyser_tout.sh
# Puis comparer les deux rapports HTML

# Scénario 4 : Débugger un problème de performance
cd bucket_bronze
python3 performance_minio.py 2>&1 | tee debug.log
# Analyser debug.log

# Scénario 5 : Générer un rapport pour présentation
./analyser_tout.sh
xdg-open bucket_bronze/rapport_performance.html
xdg-open bucket_silver/rapport_performance.html

═══════════════════════════════════════════════════════════════════════════
⚠️  TROUBLESHOOTING
═══════════════════════════════════════════════════════════════════════════

❌ Erreur : "Aucun dataset trouvé"
   → Vérifier que les données sont ingérées :
     docker exec chu_minio mc ls local/bronze/
     docker exec chu_minio mc ls local/silver/
   → Si vide, lancer l'ingestion :
     cd /home/alban/BigData
     docker exec chu_spark spark-submit /spark_jobs/main_jobs/bronze_ingestion.py
     docker exec chu_spark spark-submit /spark_jobs/main_jobs/silver_transformation.py

❌ Erreur : "Connexion MinIO échouée"
   → Démarrer MinIO :
     cd /home/alban/BigData/BigData
     docker-compose up -d chu_minio
   → Vérifier :
     docker ps | grep minio
     curl http://127.0.0.1:9000/minio/health/live

❌ Erreur : "Module not found: boto3"
   → Installer les dépendances :
     pip install boto3 pyarrow pandas matplotlib seaborn numpy

❌ Graphiques vides ou erreurs
   → Vérifier les données :
     cd bucket_bronze
     python3 -c "
     import boto3
     s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000',
                       aws_access_key_id='minioadmin',
                       aws_secret_access_key='minioadmin123')
     print(s3.list_objects_v2(Bucket='bronze', Delimiter='/'))
     "

═══════════════════════════════════════════════════════════════════════════
📞 SUPPORT
═══════════════════════════════════════════════════════════════════════════

Pour plus d'informations :
1. Lire README.md pour la vue d'ensemble
2. Consulter GUIDE_COMPLET.md pour les détails
3. Voir README_GRAPHIQUES.md pour les aspects techniques
4. Comparer avec COMPARAISON_BRONZE_SILVER.md

═══════════════════════════════════════════════════════════════════════════

🏥 Projet : CHU - Big Data Healthcare Analytics
📅 Version : 2.0
📆 Date : Octobre 2025

═══════════════════════════════════════════════════════════════════════════
EOF
