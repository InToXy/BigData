# 🔧 DÉPANNAGE - Scripts de Validation Gold

## ❌ Problème: `NumberFormatException: For input string: "60s"`

### Cause
Conflit entre les versions de Hadoop lors de l'exécution locale de PySpark. 
La bibliothèque Hadoop-AWS 3.3.4 attend des valeurs numériques en millisecondes,
mais trouve des configurations avec le suffixe "s" (secondes).

### ✅ Solutions Recommandées

#### Solution 1: Utiliser Trino (⚡ RAPIDE - Recommandé)
```bash
cd /home/alban/BigData/BigData/gold_validation
./quick_check_trino.sh
```
**Avantages:**
- Pas de dépendance Spark locale
- Execution rapide (5-10 secondes)
- Pas de configuration complexe

#### Solution 2: Exécution via Conteneur Jupyter  
```bash
cd /home/alban/BigData/BigData/gold_validation
./validate_gold.sh
# Choisir option 1: "Jupyter container"
```
**Avantages:**
- Configuration Spark complète et fonctionnelle
- Analyse détaillée avec statistiques
- Export CSV possible

#### Solution 3: Exécution dans Docker
```bash
# Copier le script dans le conteneur Jupyter
docker cp BigData/gold_validation/validate_gold_tables.py chu_jupyter:/opt/workspace/

# Exécuter dans le conteneur
docker exec -it chu_jupyter bash -c "cd /opt/workspace && python3 validate_gold_tables.py"
```

---

## 🐛 Autres Problèmes Courants

### "Tables manquantes (8/8)"

**Cause:** Les tables Gold n'ont pas encore été générées par le job Spark.

**Solution:**
```bash
# 1. Vérifier que le job Gold a été exécuté
docker logs chu_jupyter | grep gold_aggregation

# 2. Lancer le job Gold manuellement
docker exec -it chu_jupyter bash -c \
  "cd /opt/workspace && spark-submit --master local[*] \
   --packages org.apache.hadoop:hadoop-aws:3.3.4 \
   spark_jobs/main_jobs/gold_aggregation.py"

# 3. Vérifier MinIO
# Ouvrir http://localhost:9001 (minioadmin / minioadmin123)
# Naviguer dans le bucket "gold"
```

### "Container chu_trino not found"

**Cause:** Le conteneur Trino n'est pas démarré.

**Solution:**
```bash
cd /home/alban/BigData/BigData
docker-compose up -d chu_trino

# Attendre 30 secondes le démarrage
sleep 30

# Tester
./gold_validation/quick_check_trino.sh
```

### "Connection refused to MinIO"

**Cause:** MinIO n'est pas accessible.

**Solution:**
```bash
# Vérifier que MinIO tourne
docker ps | grep minio

# Redémarrer si nécessaire
cd /home/alban/BigData/BigData
docker-compose restart minio

# Attendre
 30 secondes
sleep 30
```

---

## 📊 Workflow Complet de Validation

```bash
# 1. S'assurer que tous les services tournent
cd /home/alban/BigData/BigData
docker-compose ps

# 2. Lancer les jobs de création des données Gold (si pas fait)
docker exec -it chu_jupyter bash -c \
  "cd /opt/workspace && spark-submit --master local[*] \
   --packages org.apache.hadoop:hadoop-aws:3.3.4 \
   spark_jobs/main_jobs/gold_aggregation.py"

# 3. Attendre la fin du traitement
docker logs -f chu_jupyter

# 4. Valider avec Trino (rapide)
cd gold_validation
./quick_check_trino.sh

# 5. Analyse détaillée (optionnel)
./validate_gold.sh
# Choisir "Jupyter container" puis options --detailed --export
```

---

## 🎯 Configuration Spark Locale (Avancé)

Si vous **devez absolument** exécuter localement, créez un fichier de configuration Hadoop:

```bash
# Créer hadoop-site.xml
cat > /tmp/hadoop-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.connection.establish.timeout</name>
        <value>60000</value>
    </property>
    <property>
        <name>fs.s3a.connection.timeout</name>
        <value>60000</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
</configuration>
EOF

# Exporter la configuration
export HADOOP_CONF_DIR=/tmp

# Relancer le script
python3 gold_validation/validate_gold_tables.py
```

⚠️ **Note**: Cette méthode reste complexe et sujette à erreurs. **Privilégiez Trino ou le conteneur Jupyter**.

---

## 📞 Support

Si le problème persiste:
1. Vérifiez les logs: `docker logs chu_jupyter` et `docker logs chu_trino`
2. Testez la connectivité MinIO: `curl http://localhost:9000/minio/health/live`
3. Vérifiez les buckets: Ouvrir http://localhost:9001 dans le navigateur

Pour plus d'informations, consultez:
- `README.md` - Documentation principale
- `SUMMARY.txt` - Vue d'ensemble visuelle
- `../trino/README.md` - Documentation Trino
