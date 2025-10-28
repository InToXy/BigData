# 🚀 Apache Airflow - Orchestrateur du Pipeline Healthcare

## 📋 Vue d'Ensemble

Apache Airflow a été intégré à la stack pour orchestrer automatiquement l'ensemble du pipeline de données **Bronze → Silver → Gold → PostgreSQL → Superset**.

### 🎯 Objectifs
- Automatiser l'exécution quotidienne du pipeline complet
- Gérer les dépendances entre les tâches
- Monitorer l'état des jobs Spark
- Valider la qualité des données à chaque étape
- Notifier en cas de succès ou d'échec

---

## 🏗️ Architecture Airflow

### Composants Déployés

```
┌─────────────────────────────────────────────────────────┐
│                   AIRFLOW STACK                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  chu_airflow_db         PostgreSQL 15                   │
│  │                      Base de données Airflow         │
│  │                                                      │
│  ├─→ chu_airflow_init   Initialisation                 │
│  │                      - Création DB                   │
│  │                      - Création admin user           │
│  │                                                      │
│  ├─→ chu_airflow_webserver  UI Web (port 8080)         │
│  │                           Interface graphique        │
│  │                                                      │
│  └─→ chu_airflow_scheduler  Scheduler                  │
│                             Exécution des DAGs          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Configuration Réseau
- **Réseau**: bigdata_network (partagé avec MinIO, Spark, PostgreSQL, Superset)
- **Port Webserver**: 8080
- **Accès**: http://localhost:8081
- **Credentials**: admin / admin123

---

## 📂 Structure des Fichiers

```
BigData/
├── dags/                              # 📁 DAGs Airflow
│   ├── healthcare_pipeline.py        # DAG principal (pipeline complet)
│   ├── superset_exposition.py        # DAG exposition Superset
│   └── .airflowignore                # Fichiers à ignorer
│
├── airflow/                           # 📁 Configuration Airflow
│   ├── logs/                         # Logs d'exécution (auto-généré)
│   ├── plugins/                      # Plugins custom (vide)
│   └── .env                          # Variables d'environnement
│
├── spark_jobs/                        # 📁 Jobs Spark (exécutés par Airflow)
│   ├── bronze_ingestion_rgpd_complete.py
│   ├── silver_transformation.py
│   ├── gold_aggregation.py
│   ├── gold_kpis_metier.py
│   └── gold_to_postgres.py
│
├── tools/                             # 📁 Scripts utilitaires
│   ├── fix_superset_connection.py
│   └── expose_new_kpis_superset.py
│
└── docker-compose.yml                 # 🐳 Stack complète avec Airflow
```

---

## 🔄 DAGs Disponibles

### 1. **healthcare_pipeline_complete** 🏥

**Description**: Pipeline complet d'ingestion et transformation des données de santé

**Planification**: Tous les jours à 2h00 du matin (configurable)

**Tâches (10 étapes)** :

```
1. bronze_ingestion_rgpd
   ↓
2. check_bronze_data
   ↓
3. silver_transformation
   ↓
4. check_silver_data
   ↓
5. gold_aggregation_base ──┐
                           ├─→ 7. check_gold_kpis
6. gold_kpis_metier ───────┘
   ↓
8. load_gold_to_postgresql
   ↓
9. check_postgresql_tables
   ↓
10. success_notification
```

**Détails des tâches** :

| Tâche | Type | Description | Durée estimée |
|-------|------|-------------|---------------|
| bronze_ingestion_rgpd | BashOperator | Ingestion 21 tables CSV avec anonymisation RGPD | ~15 min |
| check_bronze_data | BashOperator | Validation des données Bronze (21 tables) | ~10 sec |
| silver_transformation | BashOperator | Création modèle dimensionnel (4 dims + 3 faits) | ~10 min |
| check_silver_data | BashOperator | Validation Silver (7 tables) | ~10 sec |
| gold_aggregation_base | BashOperator | Génération 7 KPIs de base | ~3 min |
| gold_kpis_metier | BashOperator | Génération 7 KPIs métier | ~5 min |
| check_gold_kpis | BashOperator | Validation Gold (14 KPIs) | ~10 sec |
| load_gold_to_postgresql | BashOperator | Chargement PostgreSQL (14 tables) | ~2 min |
| check_postgresql_tables | BashOperator | Validation PostgreSQL (14 tables) | ~5 sec |
| success_notification | PythonOperator | Notification de succès avec résumé | ~1 sec |

**Durée totale estimée** : ~35 minutes

**Configuration** :
- **Retries** : 2 tentatives en cas d'échec
- **Retry delay** : 5 minutes
- **Timeout** : 2 heures
- **Max active runs** : 1 (évite les exécutions parallèles)

---

### 2. **superset_exposition** 🎨

**Description**: Exposition automatique des KPIs dans Superset

**Planification**: Manuelle ou déclenchée après le pipeline principal

**Tâches (3 étapes)** :

```
1. expose_new_kpis_superset
   ↓
2. check_superset_datasets
   ↓
3. success_notification
```

**Utilisation** :
- Exposer automatiquement les 14 KPIs comme datasets Superset
- Vérifier que tous les datasets sont bien créés
- Prépare Superset pour la création de visualisations

---

## 🚀 Démarrage d'Airflow

### 1. Lancer la Stack Complète

```bash
# Démarrer tous les services (y compris Airflow)
cd /home/alban/BigData/BigData
docker-compose up -d

# Vérifier les conteneurs Airflow
docker ps | grep airflow
```

Vous devriez voir 4 conteneurs Airflow :
- `chu_airflow_db` - Base de données PostgreSQL
- `chu_airflow_init` - Initialisation (se termine après setup)
- `chu_airflow_webserver` - Interface web
- `chu_airflow_scheduler` - Planificateur

### 2. Accéder à l'Interface Web

```
URL: http://localhost:8081
Username: admin
Password: admin123
```

### 3. Vérifier les DAGs

Une fois connecté :
1. Vous devriez voir 2 DAGs :
   - `healthcare_pipeline_complete`
   - `superset_exposition`
2. Les DAGs sont **pausés par défaut** (toggle gris)

---

## 📊 Utilisation des DAGs

### Activer un DAG

1. Cliquez sur le **toggle** à gauche du nom du DAG (doit devenir bleu)
2. Le DAG sera maintenant exécuté selon sa planification

### Exécuter Manuellement

**Méthode 1 - Via l'interface** :
1. Cliquez sur le nom du DAG
2. Cliquez sur le bouton **▶️ Play** (en haut à droite)
3. Sélectionnez **Trigger DAG**

**Méthode 2 - Via CLI** :
```bash
# Déclencher le pipeline complet
docker exec chu_airflow_scheduler airflow dags trigger healthcare_pipeline_complete

# Déclencher l'exposition Superset
docker exec chu_airflow_scheduler airflow dags trigger superset_exposition
```

### Suivre l'Exécution

1. **Graph View** : Vue graphique des tâches et dépendances
   - Cliquez sur le DAG → **Graph**
   - Les couleurs indiquent l'état :
     - 🟢 Vert : Succès
     - 🔵 Bleu : En cours
     - 🟡 Jaune : En attente
     - 🔴 Rouge : Échec

2. **Calendar View** : Vue calendrier des exécutions
   - Voir l'historique sur plusieurs jours

3. **Logs** : Consulter les logs détaillés
   - Cliquez sur une tâche → **Log**
   - Voir la sortie Spark complète

---

## ⚙️ Configuration Avancée

### Modifier la Planification

Éditez `/dags/healthcare_pipeline.py` :

```python
dag = DAG(
    'healthcare_pipeline_complete',
    # ...
    schedule_interval='0 2 * * *',  # Cron expression
    # Exemples:
    # '0 2 * * *'    - Tous les jours à 2h00
    # '0 */6 * * *'  - Toutes les 6 heures
    # '0 0 * * 0'    - Tous les dimanches à minuit
    # '@daily'       - Tous les jours à minuit
    # '@hourly'      - Toutes les heures
    # None           - Pas de planification automatique
)
```

### Ajuster les Ressources Spark

Éditez les commandes dans le DAG :

```python
bronze_ingestion = BashOperator(
    task_id='bronze_ingestion_rgpd',
    bash_command="""
    docker exec chu_jupyter spark-submit \
        --master local[4] \              # Augmenter le parallélisme
        --driver-memory 4g \              # Plus de mémoire
        --executor-memory 2g \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /home/jovyan/bronze_ingestion_rgpd_complete.py
    """,
)
```

### Ajouter des Notifications Email

1. Configurer SMTP dans `airflow/.env` :
```bash
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@chu.com
```

2. Modifier les `default_args` :
```python
default_args = {
    'owner': 'chu_data_team',
    'email': ['admin@chu.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    # ...
}
```

---

## 🔍 Monitoring et Logs

### Consulter les Logs Airflow

```bash
# Logs du webserver
docker logs chu_airflow_webserver --tail 100 -f

# Logs du scheduler
docker logs chu_airflow_scheduler --tail 100 -f

# Logs d'une tâche spécifique
docker exec chu_airflow_scheduler airflow tasks logs healthcare_pipeline_complete bronze_ingestion_rgpd 2025-10-28
```

### Logs des Jobs Spark

Les logs Spark sont visibles dans l'interface Airflow :
1. Cliquez sur la tâche concernée
2. **Log** → Vous verrez toute la sortie du job Spark

### Métriques et Statistiques

Dans l'interface Airflow :
- **Dashboard** : Vue d'ensemble des DAGs actifs
- **Browse → Task Instances** : Toutes les exécutions de tâches
- **Browse → DAG Runs** : Historique des exécutions complètes
- **Browse → Jobs** : État des jobs du scheduler

---

## 🛠️ Dépannage

### Problème : DAG n'apparaît pas

**Symptôme** : Le DAG n'est pas visible dans l'interface

**Solutions** :
```bash
# 1. Vérifier les erreurs de parsing
docker exec chu_airflow_scheduler airflow dags list-import-errors

# 2. Forcer le rafraîchissement
docker exec chu_airflow_scheduler airflow dags reserialize

# 3. Vérifier les logs du scheduler
docker logs chu_airflow_scheduler --tail 50
```

### Problème : Tâche échoue systématiquement

**Symptôme** : Une tâche Spark échoue toujours

**Solutions** :
```bash
# 1. Consulter les logs détaillés dans l'UI

# 2. Tester la commande manuellement
docker exec chu_jupyter spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/jovyan/bronze_ingestion_rgpd_complete.py

# 3. Vérifier que Jupyter et MinIO sont accessibles
docker ps | grep chu_jupyter
docker ps | grep chu_minio
```

### Problème : Webserver ne répond pas

**Symptôme** : http://localhost:8081 inaccessible

**Solutions** :
```bash
# 1. Vérifier le conteneur
docker ps | grep chu_airflow_webserver

# 2. Vérifier le healthcheck
docker inspect chu_airflow_webserver | grep -A 5 Health

# 3. Redémarrer le webserver
docker restart chu_airflow_webserver

# 4. Consulter les logs
docker logs chu_airflow_webserver --tail 100
```

### Problème : DAG bloqué "running"

**Symptôme** : Le DAG semble tourner indéfiniment

**Solutions** :
```bash
# 1. Identifier le DAG run bloqué (dans l'UI ou CLI)
docker exec chu_airflow_scheduler airflow dags list-runs -d healthcare_pipeline_complete

# 2. Marquer comme échec
docker exec chu_airflow_scheduler airflow dags state healthcare_pipeline_complete <date> --mark-failed

# 3. Ou nettoyer complètement
docker exec chu_airflow_scheduler airflow dags delete healthcare_pipeline_complete -y
```

---

## 🔒 Sécurité et Bonnes Pratiques

### 1. Changer les Credentials par Défaut

```bash
# 1. Modifier airflow/.env
POSTGRES_PASSWORD=<nouveau_mot_de_passe_fort>
_AIRFLOW_WWW_USER_PASSWORD=<nouveau_mot_de_passe_admin>

# 2. Recréer le conteneur
docker-compose down
docker-compose up -d airflow-db airflow-init
```

### 2. Limiter les Accès

```python
# Dans les DAGs, utiliser des variables Airflow
from airflow.models import Variable

MINIO_ACCESS_KEY = Variable.get("minio_access_key")
POSTGRES_PASSWORD = Variable.get("postgres_password")
```

Définir les variables dans l'UI : **Admin → Variables**

### 3. Utiliser des Connections

Au lieu de hardcoder les connexions :

1. **Admin → Connections** → **+**
2. Créer une connexion "minio_s3" :
   - Conn Type: S3
   - Login: minioadmin
   - Password: minioadmin123
   - Extra: `{"endpoint_url": "http://172.18.0.2:9000"}`

3. Utiliser dans le DAG :
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

s3_hook = S3Hook(aws_conn_id='minio_s3')
```

---

## 📈 Évolutions Futures

### Idées d'Amélioration

1. **SLA Monitoring** : Ajouter des SLA sur les tâches critiques
```python
bronze_ingestion = BashOperator(
    task_id='bronze_ingestion_rgpd',
    sla=timedelta(minutes=20),  # Alerte si > 20 min
    # ...
)
```

2. **Sensors pour Données Externes** :
```python
from airflow.sensors.filesystem import FileSensor

wait_for_new_data = FileSensor(
    task_id='wait_for_csv_files',
    filepath='/path/to/data/*.csv',
    poke_interval=300,  # Check toutes les 5 min
)
```

3. **DAG Dynamique par Département** :
```python
# Créer un DAG par département automatiquement
for dept in ['75', '92', '93', '94']:
    create_pipeline_dag(f'healthcare_pipeline_{dept}', dept)
```

4. **Intégration Slack/Teams** :
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

slack_alert = SlackWebhookOperator(
    task_id='slack_notification',
    http_conn_id='slack_webhook',
    message='✅ Pipeline Healthcare terminé avec succès!',
)
```

5. **Data Quality Checks avec Great Expectations** :
```python
from airflow.providers.great_expectations.operators.great_expectations import GreatExpectationsOperator

data_quality = GreatExpectationsOperator(
    task_id='validate_bronze_quality',
    expectation_suite_name='bronze_suite',
    # ...
)
```

---

## 📚 Ressources

### Documentation Officielle
- **Airflow**: https://airflow.apache.org/docs/
- **DAG Writing Best Practices**: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- **Operators**: https://airflow.apache.org/docs/apache-airflow/stable/operators-and-hooks-ref.html

### Commandes Utiles

```bash
# Lister tous les DAGs
docker exec chu_airflow_scheduler airflow dags list

# Tester un DAG
docker exec chu_airflow_scheduler airflow dags test healthcare_pipeline_complete 2025-10-28

# Lister les tâches d'un DAG
docker exec chu_airflow_scheduler airflow tasks list healthcare_pipeline_complete

# Tester une tâche spécifique
docker exec chu_airflow_scheduler airflow tasks test healthcare_pipeline_complete bronze_ingestion_rgpd 2025-10-28

# Pause/Unpause un DAG
docker exec chu_airflow_scheduler airflow dags pause healthcare_pipeline_complete
docker exec chu_airflow_scheduler airflow dags unpause healthcare_pipeline_complete

# Backfill (rejouer des dates passées)
docker exec chu_airflow_scheduler airflow dags backfill healthcare_pipeline_complete \
    --start-date 2025-10-01 \
    --end-date 2025-10-28
```

---

## ✅ Checklist de Déploiement

Avant de mettre en production :

- [ ] Airflow webserver accessible sur http://localhost:8081
- [ ] Login avec admin/admin123 fonctionne
- [ ] Les 2 DAGs apparaissent dans l'interface
- [ ] Test manuel du DAG `healthcare_pipeline_complete` réussi
- [ ] Toutes les tâches vertes dans Graph View
- [ ] Vérification des données dans MinIO (bronze/silver/gold)
- [ ] Vérification des tables PostgreSQL (14 KPIs)
- [ ] Logs Airflow consultables et complets
- [ ] Planification activée (toggle bleu)
- [ ] Credentials par défaut changés (production)
- [ ] Notifications configurées (email/Slack)

---

## 🎯 Résumé

**Airflow est maintenant intégré et prêt à orchestrer votre pipeline !**

- ✅ **4 conteneurs** Airflow déployés
- ✅ **2 DAGs** configurés (pipeline + exposition)
- ✅ **10 tâches** orchestrées automatiquement
- ✅ **Monitoring** complet via interface web
- ✅ **Validation** à chaque étape du pipeline
- ✅ **Planification** quotidienne à 2h00

**Accès** :
- **Airflow UI** : http://localhost:8081 (admin/admin123)
- **Superset** : http://localhost:8088 (admin/admin123)
- **MinIO Console** : http://localhost:9001 (minioadmin/minioadmin123)

**Prochaines étapes** :
1. Activer le DAG principal (toggle bleu)
2. Lancer une première exécution manuelle
3. Vérifier les résultats dans Superset
4. Configurer les notifications (optionnel)
5. Créer des visualisations dans Superset

---

*Documentation créée le 28 Octobre 2025*  
*Stack: Healthcare Data Lakehouse avec Orchestration Airflow*
