# 📚 DOCUMENTATION ZONE GOLD - GUIDE D'UTILISATION

Ce répertoire contient l'ensemble de la documentation de la zone Gold du Data Lake médical.

## 🗂️ STRUCTURE DES DOCUMENTS

### 📊 Documents Principaux

| Document | Description | Audience | Pages |
|----------|-------------|----------|-------|
| **RAPPORT_GOLD_COMPLET.md** | 🎯 **Document principal de synthèse** | Direction, Chef de projet | 15 |
| **GOLD_TABLES_CATALOG.md** | Catalogue détaillé des 12 tables Gold | Analystes, Data Scientists | 10 |
| **GOLD_PERFORMANCE_TESTS.md** | Tests de performance avec 17 requêtes | Data Engineers, DevOps | 20 |
| **PERFORMANCE_ZONES.md** | Comparaison Bronze/Silver/Gold | Architectes, Direction technique | 5 |
| **GOLD_KPI_SUMMARY.md** | Documentation exhaustive des 8 KPIs | Analystes métier, Product Owners | 8 |

### 🔧 Documents Techniques

| Document | Description | Audience |
|----------|-------------|----------|
| **spark_jobs/main_jobs/README_GOLD.md** | Guide d'exécution du job Spark | Data Engineers |
| **spark_jobs/test_gold_queries.py** | Suite de tests automatisés (17 requêtes) | DevOps, QA |
| **spark_jobs/document_gold_tables.py** | Script de génération documentation | Data Engineers |
| **spark_jobs/audit_gold.py** | Audit de performance automatique | DevOps |

---

## 🎯 QUEL DOCUMENT POUR QUEL BESOIN ?

### Pour rédiger un rapport managérial
➡️ **Commencez par:** `RAPPORT_GOLD_COMPLET.md`
- Vue d'ensemble exécutive
- Chiffres clés et performances
- Valeur métier quantifiée
- Recommandations stratégiques

### Pour comprendre les tables disponibles
➡️ **Consultez:** `GOLD_TABLES_CATALOG.md`
- Schémas complets des 12 tables
- Exemples de données
- Cas d'usage par table
- Recommandations d'utilisation

### Pour tester les performances
➡️ **Utilisez:** `GOLD_PERFORMANCE_TESTS.md` + `test_gold_queries.py`
- 17 requêtes de test prêtes à l'emploi
- Résultats attendus et benchmarks
- Comparaison avec objectifs

### Pour intégrer les KPIs dans une application
➡️ **Référez-vous à:** `GOLD_KPI_SUMMARY.md`
- Définition précise de chaque KPI
- Formules de calcul
- Colonnes et types
- Insights métier

### Pour comparer avec Bronze/Silver
➡️ **Lisez:** `PERFORMANCE_ZONES.md`
- Tableaux comparatifs
- Métriques de compression
- Gains de performance

---

## 📋 CHECKLIST POUR VOTRE RAPPORT

### ✅ Section 1: Introduction
- [ ] Contexte du Data Lake médical
- [ ] Objectifs de la zone Gold
- [ ] Architecture en 3 zones (Bronze → Silver → Gold)

**Source:** `RAPPORT_GOLD_COMPLET.md` - Section 1

---

### ✅ Section 2: Données et Tables

#### Tables à présenter dans le rapport:

| Priorité | Table | Lignes | Raison |
|----------|-------|--------|--------|
| ⭐⭐⭐ | `kpi_taux_hospitalisation_global` | 1 | KPI stratégique principal |
| ⭐⭐⭐ | `kpi_hospitalisation_par_diagnostic` | 768 | Plus volumineuse, grande valeur métier |
| ⭐⭐⭐ | `kpi_hospitalisation_sexe_age` | 10 | Analyse démographique clé |
| ⭐⭐ | `kpi_deces_par_region_2019` | ~15 | Mortalité (indicateur sensible) |
| ⭐⭐ | `kpi_satisfaction_region_annee` | ~60 | Qualité perçue |
| ⭐ | Autres tables | Variable | Pour exhaustivité |

**Sources:**
- Schémas: `GOLD_TABLES_CATALOG.md`
- Statistiques: `RAPPORT_GOLD_COMPLET.md` - Section 3

---

### ✅ Section 3: Performances

#### Tableaux à inclure:

**Tableau 1: Comparaison Bronze/Silver/Gold**
```
| Métrique | Bronze | Silver | Gold | Gain |
|----------|--------|--------|------|------|
| Lignes   | 7.6M   | 2.17M  | 1,563| -99.996% |
| Stockage | 726 MB | 207 MB | 0.03 MB | -99.996% |
| Temps lecture | ~5s | ~1s | ~0.2s | -96% |
```
**Source:** `PERFORMANCE_ZONES.md`

**Tableau 2: Temps d'exécution des requêtes**
```
| Catégorie | Nb Requêtes | Temps Moyen | Temps Total |
|-----------|-------------|-------------|-------------|
| KPI Analytiques | 5 | 0.14s | 0.70s |
| Temporelles | 3 | 0.13s | 0.39s |
| Techniques | 5 | 0.22s | 1.10s |
| Data Science | 4 | 0.30s | 1.20s |
| **TOTAL** | **17** | **0.20s** | **3.39s** |
```
**Source:** `GOLD_PERFORMANCE_TESTS.md` - Section Résumé

**Graphiques recommandés:**
- 📊 Évolution volumétrie Bronze → Silver → Gold (bar chart)
- 📈 Temps de réponse par catégorie de requête (bar chart)
- 🥧 Distribution des lignes par table (pie chart des top 5)

---

### ✅ Section 4: Tests et Validations

**Tests réalisés à mentionner:**

✅ **Fonctionnels:** 5/5 passés
- Création des 8 KPIs
- Intégrité des données
- Cohérence des calculs
- Format Parquet
- Accès S3A

✅ **Performance:** 5/5 passés
- Scan < 0.3s
- Agrégation < 0.5s
- Jointure < 0.5s
- Cache speedup > 5x
- Filtres < 0.2s

✅ **Qualité:** 100%
- Complétude: 100%
- Cohérence: 100%
- Validations métier: passées

**Source:** `RAPPORT_GOLD_COMPLET.md` - Section 5

---

### ✅ Section 5: Valeur Métier

**4 cas d'usage à présenter:**

1. **Pilotage Stratégique** (Direction)
   - KPI: Taux d'hospitalisation global
   - Bénéfice: Vision synthétique instantanée

2. **Planification Capacités** (Direction des soins)
   - KPI: Hospitalisations par diagnostic
   - Bénéfice: Dimensionnement data-driven

3. **Prévention Ciblée** (Santé publique)
   - KPI: Hospitalisations sexe/âge
   - Bénéfice: Ciblage populations à risque

4. **Optimisation Financière** (DAF)
   - KPI: Tous
   - Bénéfice: 15M€ d'économies potentielles

**Source:** `RAPPORT_GOLD_COMPLET.md` - Section 6.1

---

### ✅ Section 6: Recommandations

**À inclure:**
- ✅ Priorisation haute: 3 actions immédiates
- ⚠️ Priorisation moyenne: 3 actions à 3 mois
- 💡 Priorisation basse: 2 actions à 6 mois
- 📅 Roadmap 2026 (Q1-Q4)

**Source:** `RAPPORT_GOLD_COMPLET.md` - Section 7

---

## 🚀 EXÉCUTION DES TESTS

### Test Complet (17 requêtes)

```bash
cd /home/alban/BigData/BigData

docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/test_gold_queries.py
```

**Durée estimée:** ~3-5 secondes  
**Résultats:** Affichés dans le terminal + métriques de performance

### Audit de Performance

```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/audit_gold.py
```

**Durée estimée:** ~2-3 secondes  
**Résultats:** Statistiques par table + résumé global

### Génération Job Gold

```bash
docker exec -it chu_jupyter spark-submit \
  --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar \
  /home/jovyan/jobs/gold_aggregation.py
```

**Durée estimée:** ~30-60 secondes  
**Résultats:** 8 KPIs créés dans `s3a://gold/`

---

## 📊 EXEMPLES DE GRAPHIQUES POUR LE RAPPORT

### Graphique 1: Réduction Volumétrique

```
Volumétrie (MB)
800 │
700 │ ████████
600 │ ████████
500 │ ████████
400 │ ████████
300 │ ████████  ████
200 │ ████████  ████
100 │ ████████  ████
  0 │ ████████  ████  ░
    └─────────────────────
      Bronze   Silver Gold
      726 MB   207 MB 0.03 MB
```

**Données:** `PERFORMANCE_ZONES.md` - Tableau récapitulatif

---

### Graphique 2: Temps de Requête par Catégorie

```
Temps (secondes)
0.35│
0.30│                        ████
0.25│                        ████
0.20│          ████          ████  ████
0.15│   ████   ████          ████  ████
0.10│   ████   ████   ████   ████  ████
0.05│   ████   ████   ████   ████  ████
0.00│   ████   ████   ████   ████  ████
    └────────────────────────────────────
        KPI   Temp.  Tech.   DS   Global
       0.14s  0.13s  0.22s  0.30s  0.20s
```

**Données:** `GOLD_PERFORMANCE_TESTS.md` - Tableau récapitulatif

---

### Graphique 3: Distribution des Lignes par Table

```
Répartition des données Gold (1,563 lignes total)

kpi_hospitalisation_par_diagnostic ██████████████████ 768 (49.1%)
kpi_hospitalisation_sexe_age       █ 10 (0.6%)
kpi_deces_par_region_2019          █ 15 (1.0%)
kpi_satisfaction_region_annee      ██ 60 (3.8%)
Autres tables                      ████████ 710 (45.5%)
```

**Données:** `audit_gold.py` - Résultats d'exécution

---

## 💡 CONSEILS POUR LA RÉDACTION

### Style Recommandé

✅ **Faire:**
- Utiliser les chiffres clés (99.996% compression, 0.2s temps moyen)
- Mettre en avant la valeur métier (15M€ économies)
- Inclure des exemples concrets (Top 5 diagnostics)
- Présenter des recommandations actionnables

❌ **Éviter:**
- Détails techniques trop complexes (sauf pour audience technique)
- Jargon sans explication (ex: "S3A", "Parquet" → expliquer)
- Tableaux trop longs (limiter à Top 5-10)

### Niveaux de Détail par Audience

**Direction / Management:**
- 📄 **Pages:** 3-5 pages max
- 📊 **Focus:** Valeur métier, chiffres clés, recommandations
- 📚 **Documents:** `RAPPORT_GOLD_COMPLET.md` (sections 1, 4, 6)

**Analystes Métier:**
- 📄 **Pages:** 8-12 pages
- 📊 **Focus:** KPIs détaillés, cas d'usage, exemples
- 📚 **Documents:** `GOLD_KPI_SUMMARY.md` + `GOLD_TABLES_CATALOG.md`

**Data Engineers:**
- 📄 **Pages:** 15-20 pages
- 📊 **Focus:** Architecture, performances, optimisations
- 📚 **Documents:** Tous + scripts techniques

---

## 📞 SUPPORT

### Questions Fréquentes

**Q: Comment exécuter les tests de performance ?**  
R: Voir section "Exécution des tests" ci-dessus. Script: `test_gold_queries.py`

**Q: Où trouver les schémas des tables ?**  
R: `GOLD_TABLES_CATALOG.md` - Section 3

**Q: Comment calculer les KPIs ?**  
R: `GOLD_KPI_SUMMARY.md` - Formules détaillées pour chaque KPI

**Q: Que faire si les tests échouent ?**  
R: Vérifier:
1. MinIO accessible (docker ps)
2. Données Silver présentes (`s3a://silver/`)
3. JARs disponibles (hadoop-aws, aws-sdk)

---

## 📅 MISE À JOUR

**Dernière mise à jour:** 24 Octobre 2025  
**Version:** 1.0  
**Prochaine révision:** Q1 2026 (après mise en production)

---

## ✅ CHECKLIST FINALE AVANT SOUMISSION DU RAPPORT

- [ ] Relu le document principal (`RAPPORT_GOLD_COMPLET.md`)
- [ ] Vérifié les chiffres clés (compression, temps, lignes)
- [ ] Exécuté les tests de performance (17 requêtes)
- [ ] Inclus les 3 graphiques recommandés
- [ ] Présenté les 4 cas d'usage métier
- [ ] Listé les recommandations prioritaires
- [ ] Relu pour fautes et cohérence
- [ ] Validé avec l'équipe technique
- [ ] Obtenu approbation chef de projet

**Une fois terminé, votre rapport sera prêt pour présentation ! 🎉**

---

**Bon courage pour votre rapport !**  
**L'équipe Data Engineering CHU**
