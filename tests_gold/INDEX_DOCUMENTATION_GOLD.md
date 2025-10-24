# 📖 INDEX - DOCUMENTATION ZONE GOLD

## Navigation Rapide dans la Documentation

**Version:** 1.0  
**Date:** 24 Octobre 2025

---

## 🎯 DÉMARRAGE RAPIDE

### Vous êtes nouveau ? Commencez ici ! 👇

1. **Lire en premier:** [`README_DOCUMENTATION_GOLD.md`](README_DOCUMENTATION_GOLD.md)
   - Guide complet d'utilisation de la documentation
   - Explique quel document lire selon votre besoin

2. **Vue d'ensemble:** [`FICHE_RECAP_GOLD.md`](FICHE_RECAP_GOLD.md)
   - Résumé 1 page avec les chiffres clés
   - Parfait pour une première lecture rapide

3. **Document principal:** [`RAPPORT_GOLD_COMPLET.md`](RAPPORT_GOLD_COMPLET.md)
   - Document de synthèse complet (15 pages)
   - Base pour votre rapport officiel

---

## 📚 DOCUMENTS PAR THÈME

### 🎯 POUR LA DIRECTION / MANAGEMENT

| Document | Description | Pages | Priorité |
|----------|-------------|-------|----------|
| [RAPPORT_GOLD_COMPLET.md](RAPPORT_GOLD_COMPLET.md) | **Synthèse exécutive complète** | 15 | ⭐⭐⭐ |
| [FICHE_RECAP_GOLD.md](FICHE_RECAP_GOLD.md) | Vue d'ensemble 1 page | 1 | ⭐⭐⭐ |
| [PERFORMANCE_ZONES.md](PERFORMANCE_ZONES.md) | Comparaison Bronze/Silver/Gold | 5 | ⭐⭐ |

**Focus:** Chiffres clés, valeur métier (15M€), recommandations stratégiques

---

### 📊 POUR LES ANALYSTES MÉTIER

| Document | Description | Pages | Priorité |
|----------|-------------|-------|----------|
| [GOLD_TABLES_CATALOG.md](GOLD_TABLES_CATALOG.md) | **Catalogue des 12 tables** | 10 | ⭐⭐⭐ |
| [spark_jobs/main_jobs/GOLD_KPI_SUMMARY.md](spark_jobs/main_jobs/GOLD_KPI_SUMMARY.md) | Détails des 8 KPIs | 8 | ⭐⭐ |
| [RAPPORT_GOLD_COMPLET.md](RAPPORT_GOLD_COMPLET.md) - Section 6 | Cas d'usage métier | 3 | ⭐⭐ |

**Focus:** Tables disponibles, KPIs métier, cas d'usage, exemples

---

### 💻 POUR LES DATA ENGINEERS

| Document | Description | Type | Priorité |
|----------|-------------|------|----------|
| [spark_jobs/main_jobs/gold_aggregation.py](spark_jobs/main_jobs/gold_aggregation.py) | **Job Spark principal** | Code | ⭐⭐⭐ |
| [GOLD_PERFORMANCE_TESTS.md](GOLD_PERFORMANCE_TESTS.md) | Tests de performance | Doc | ⭐⭐⭐ |
| [spark_jobs/test_gold_queries.py](spark_jobs/test_gold_queries.py) | Suite de tests (17 req) | Code | ⭐⭐ |
| [spark_jobs/audit_gold.py](spark_jobs/audit_gold.py) | Audit automatique | Code | ⭐⭐ |
| [spark_jobs/main_jobs/README_GOLD.md](spark_jobs/main_jobs/README_GOLD.md) | Guide d'exécution | Doc | ⭐⭐ |

**Focus:** Code source, performances, optimisations, architecture

---

### 🔬 POUR LES DATA SCIENTISTS

| Document | Description | Pages | Priorité |
|----------|-------------|-------|----------|
| [GOLD_TABLES_CATALOG.md](GOLD_TABLES_CATALOG.md) | Schémas et statistiques | 10 | ⭐⭐⭐ |
| [GOLD_PERFORMANCE_TESTS.md](GOLD_PERFORMANCE_TESTS.md) - Section 4 | Requêtes ML/Feature Eng | 5 | ⭐⭐ |
| [spark_jobs/test_gold_queries.py](spark_jobs/test_gold_queries.py) | Code exemples ML | Code | ⭐⭐ |

**Focus:** Features ML, clustering, corrélations, outliers

---

## 🗂️ ORGANISATION DES FICHIERS

```
BigData/
│
├── 📄 INDEX_DOCUMENTATION_GOLD.md (ce fichier)
│
├── 📚 DOCUMENTATION PRINCIPALE
│   ├── RAPPORT_GOLD_COMPLET.md ⭐⭐⭐ (15 pages)
│   ├── FICHE_RECAP_GOLD.md ⭐⭐⭐ (1 page)
│   ├── README_DOCUMENTATION_GOLD.md ⭐⭐⭐ (6 pages)
│   └── LIVRABLE_COMPLET_GOLD.md ⭐ (Liste de tous les fichiers)
│
├── 📊 DOCUMENTATION TECHNIQUE
│   ├── GOLD_TABLES_CATALOG.md ⭐⭐⭐ (10 pages)
│   ├── GOLD_PERFORMANCE_TESTS.md ⭐⭐ (20 pages)
│   └── PERFORMANCE_ZONES.md ⭐⭐ (5 pages)
│
└── 💻 SCRIPTS SPARK
    └── spark_jobs/
        ├── main_jobs/
        │   ├── gold_aggregation.py ⭐⭐⭐ (426 lignes)
        │   ├── README_GOLD.md ⭐⭐
        │   └── GOLD_KPI_SUMMARY.md ⭐⭐ (8 pages)
        │
        ├── test_gold_queries.py ⭐⭐ (350 lignes)
        ├── audit_gold.py ⭐⭐ (200 lignes)
        └── document_gold_tables.py ⭐ (200 lignes)
```

---

## 🎯 PARCOURS RECOMMANDÉS

### Parcours 1: "Je dois rédiger un rapport pour la direction"

**Durée estimée:** 2-3 heures

1. **Lire** [`FICHE_RECAP_GOLD.md`](FICHE_RECAP_GOLD.md) (5 min)
2. **Lire** [`RAPPORT_GOLD_COMPLET.md`](RAPPORT_GOLD_COMPLET.md) (30 min)
3. **Consulter** [`README_DOCUMENTATION_GOLD.md`](README_DOCUMENTATION_GOLD.md) - Checklist (15 min)
4. **Rédiger** votre rapport en adaptant les sections (2h)

**Résultat:** Rapport managérial 8-10 pages

---

### Parcours 2: "Je dois intégrer les KPIs dans un dashboard"

**Durée estimée:** 3-4 heures

1. **Lire** [`GOLD_TABLES_CATALOG.md`](GOLD_TABLES_CATALOG.md) (45 min)
2. **Lire** [`spark_jobs/main_jobs/GOLD_KPI_SUMMARY.md`](spark_jobs/main_jobs/GOLD_KPI_SUMMARY.md) (30 min)
3. **Tester** connexion aux tables (voir commandes) (30 min)
4. **Développer** votre dashboard (2h)

**Résultat:** Dashboard opérationnel connecté à Gold

---

### Parcours 3: "Je dois valider les performances"

**Durée estimée:** 1-2 heures

1. **Lire** [`GOLD_PERFORMANCE_TESTS.md`](GOLD_PERFORMANCE_TESTS.md) (30 min)
2. **Exécuter** [`spark_jobs/test_gold_queries.py`](spark_jobs/test_gold_queries.py) (5 min)
3. **Analyser** les résultats (30 min)
4. **Rédiger** section performance de votre rapport (30 min)

**Résultat:** Validation performance + section rapport

---

### Parcours 4: "Je veux comprendre l'architecture complète"

**Durée estimée:** 2-3 heures

1. **Lire** [`PERFORMANCE_ZONES.md`](PERFORMANCE_ZONES.md) (20 min)
2. **Lire** [`RAPPORT_GOLD_COMPLET.md`](RAPPORT_GOLD_COMPLET.md) - Section 2 (30 min)
3. **Examiner** [`spark_jobs/main_jobs/gold_aggregation.py`](spark_jobs/main_jobs/gold_aggregation.py) (1h)
4. **Exécuter** le job (voir [`spark_jobs/main_jobs/README_GOLD.md`](spark_jobs/main_jobs/README_GOLD.md)) (30 min)

**Résultat:** Compréhension approfondie de l'architecture

---

## 🔍 RECHERCHE RAPIDE PAR MOT-CLÉ

### Chiffres Clés
- **99.996% compression** → [`PERFORMANCE_ZONES.md`](PERFORMANCE_ZONES.md)
- **0.2s temps moyen** → [`GOLD_PERFORMANCE_TESTS.md`](GOLD_PERFORMANCE_TESTS.md)
- **15M€ économies** → [`RAPPORT_GOLD_COMPLET.md`](RAPPORT_GOLD_COMPLET.md) - Section 6
- **12 tables, 1,563 lignes** → [`FICHE_RECAP_GOLD.md`](FICHE_RECAP_GOLD.md)

### Tables Spécifiques
- **kpi_taux_hospitalisation_global** → [`GOLD_TABLES_CATALOG.md`](GOLD_TABLES_CATALOG.md) - Table #4
- **kpi_hospitalisation_par_diagnostic** → [`GOLD_TABLES_CATALOG.md`](GOLD_TABLES_CATALOG.md) - Table #5
- **kpi_hospitalisation_sexe_age** → [`GOLD_TABLES_CATALOG.md`](GOLD_TABLES_CATALOG.md) - Table #6

### Requêtes de Test
- **Top 10 diagnostics** → [`GOLD_PERFORMANCE_TESTS.md`](GOLD_PERFORMANCE_TESTS.md) - Section 1.1
- **Taux par sexe** → [`GOLD_PERFORMANCE_TESTS.md`](GOLD_PERFORMANCE_TESTS.md) - Section 1.2
- **Feature engineering ML** → [`GOLD_PERFORMANCE_TESTS.md`](GOLD_PERFORMANCE_TESTS.md) - Section 4.1

### Code et Scripts
- **Job principal Spark** → [`spark_jobs/main_jobs/gold_aggregation.py`](spark_jobs/main_jobs/gold_aggregation.py)
- **Suite de tests** → [`spark_jobs/test_gold_queries.py`](spark_jobs/test_gold_queries.py)
- **Audit performance** → [`spark_jobs/audit_gold.py`](spark_jobs/audit_gold.py)

---

## ⚡ COMMANDES RAPIDES

### Générer les KPIs Gold
```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/gold_aggregation.py
```

### Exécuter Tests de Performance
```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/test_gold_queries.py
```

### Auditer Zone Gold
```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/audit_gold.py
```

### Visualiser Tables Gold
```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/visu/visu_gold.py
```

---

## 📊 STATISTIQUES DE LA DOCUMENTATION

| Métrique | Valeur |
|----------|--------|
| **Fichiers Markdown** | 8 documents |
| **Fichiers Python** | 4 scripts |
| **Total pages documentation** | ~70 pages |
| **Total lignes code** | ~1,200 lignes |
| **Tables documentées** | 12/12 (100%) |
| **KPIs documentés** | 8/8 (100%) |
| **Tests documentés** | 17/17 (100%) |
| **Cas d'usage présentés** | 4 |

---

## ✅ VALIDATION DE LA DOCUMENTATION

### Complétude
- ✅ Toutes les tables documentées (12/12)
- ✅ Tous les KPIs documentés (8/8)
- ✅ Tous les tests documentés (17/17)
- ✅ Architecture complète expliquée
- ✅ Cas d'usage métier présentés (4)

### Qualité
- ✅ Exemples de code fonctionnels
- ✅ Chiffres cohérents entre documents
- ✅ Formatage Markdown correct
- ✅ Navigation facilitée (liens internes)

### Utilisabilité
- ✅ Guide de démarrage rapide
- ✅ Parcours recommandés par profil
- ✅ Index par mot-clé
- ✅ Commandes prêtes à l'emploi

---

## 🎯 PROCHAINES ACTIONS RECOMMANDÉES

### Immédiat (Aujourd'hui)
1. [ ] Lire [`README_DOCUMENTATION_GOLD.md`](README_DOCUMENTATION_GOLD.md)
2. [ ] Parcourir [`FICHE_RECAP_GOLD.md`](FICHE_RECAP_GOLD.md)
3. [ ] Identifier votre parcours recommandé

### Court Terme (Cette Semaine)
4. [ ] Lire document principal selon votre besoin
5. [ ] Exécuter tests si nécessaire
6. [ ] Commencer rédaction de votre rapport

### Moyen Terme (Ce Mois)
7. [ ] Finaliser votre rapport
8. [ ] Présenter aux équipes
9. [ ] Planifier mise en production

---

## 📞 SUPPORT

**Questions sur la documentation ?**
- 📧 Email: data-engineering@chu.fr
- 💬 Slack: #gold-zone
- 📅 Réunion hebdo: Lundi 10h

**Problème technique ?**
- Consulter [`spark_jobs/main_jobs/README_GOLD.md`](spark_jobs/main_jobs/README_GOLD.md) - Section Troubleshooting
- Vérifier logs Spark
- Contacter équipe DevOps

---

## 🏆 STATUT GLOBAL

```
╔══════════════════════════════════════════╗
║                                          ║
║   ✅ DOCUMENTATION ZONE GOLD COMPLÈTE    ║
║                                          ║
║   📚 8 documents Markdown                ║
║   💻 4 scripts Python                    ║
║   📄 ~70 pages                           ║
║   💾 ~1,200 lignes de code               ║
║                                          ║
║   🎯 PRÊT POUR UTILISATION               ║
║                                          ║
╚══════════════════════════════════════════╝
```

---

**📅 Dernière mise à jour:** 24 Octobre 2025  
**🏷️ Version:** 1.0  
**👤 Responsable:** Équipe Data Engineering CHU

---

**🎉 Bonne exploration de la documentation ! 🎉**
