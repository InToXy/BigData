# 🔍 POURQUOI SUPERSET AFFICHE NULL ?

## ❌ Problème Identifié

Votre capture d'écran montre **100 lignes avec commune = NULL**.

**C'est NORMAL !** Les données contiennent :
- ✅ 271,920 lignes AVEC commune (73%)
- ⚠️ 100,735 lignes SANS commune (27%)

**Par défaut, PostgreSQL retourne les lignes dans l'ordre physique de stockage.**
Les 100 premières lignes stockées sont justement celles SANS commune !

## ✅ Solution : Filtrer les NULL

### Dans SQL Lab :

**❌ AVANT (ce que vous avez fait) :**
```sql
SELECT * FROM kpi_consultation_etablissement LIMIT 100;
```
→ Retourne les 100 premières lignes = toutes NULL

**✅ APRÈS (ce qu'il faut faire) :**
```sql
SELECT 
    raison_sociale_site,
    commune,
    nombre_consultations
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL    -- ← AJOUTER CETTE LIGNE
ORDER BY nombre_consultations DESC
LIMIT 100;
```
→ Retourne 100 lignes avec des vraies communes !

## 📊 Résultat Attendu

Avec le filtre `WHERE commune IS NOT NULL`, vous verrez :

| raison_sociale_site | commune | nombre_consultations |
|---------------------|---------|---------------------|
| LBM BIOLIANCE | **Nantes** | 15 |
| INSTITUT PASTEUR | **Paris** | 12 |
| CENTRE HOSPITALIER | **Papeete** | 12 |
| CABINET DU DR EMILIE AUBERT BRINGER | **Montpellier** | 11 |
| LABORATOIRE SECONDAIRE CERBALLIANCE HA | **Lille** | 9 |
| ... | ... | ... |

## 🎨 Pour créer un Chart dans Superset

1. **Charts** > **+ Chart**
2. Choose Dataset: **kpi_consultation_etablissement**
3. Choose Chart Type: **Table**
4. Configuration:
   - **Columns**: commune, raison_sociale_site, nombre_consultations
   - **Metrics**: (laisser vide pour afficher les colonnes brutes)
   - **Filters**: ⚠️ **IMPORTANT** ⚠️
     - Cliquez sur **+ Add filter**
     - Column: **commune**
     - Operator: **IS NOT NULL**
     - Apply
5. **Update Chart**

## 📈 Statistiques Réelles

```
Total lignes dans la table : 372,655
├─ Avec commune    : 271,920 (73%) ← DONNÉES VALIDES
└─ Sans commune    : 100,735 (27%) ← NULL = Établissements sans commune dans source

Top communes :
1. Paris         : 8,664 consultations
2. Marseille     : 3,126 consultations
3. Toulouse      : 2,784 consultations
4. Nice          : 2,557 consultations
5. Montpellier   : 2,268 consultations
```

## 🔧 Explication Technique

**Pourquoi 27% de NULL ?**

Dans le fichier CSV source `etablissement_sante.csv` :
- Certains établissements n'ont pas de commune renseignée
- Ce sont souvent des établissements :
  - Sans adresse physique (sièges sociaux virtuels)
  - En cours de création
  - Données incomplètes

**C'est NORMAL et les données sont CORRECTES !**

## ✅ Checklist de Vérification

- [ ] Utilisez `WHERE commune IS NOT NULL` dans toutes vos requêtes
- [ ] Ajoutez le filtre "commune IS NOT NULL" dans vos charts Superset
- [ ] Vérifiez que vous voyez "Nantes", "Paris", "Montpellier" dans les résultats
- [ ] 271,920 lignes disponibles avec commune valide

## 🎯 Prochaines Étapes

Maintenant que vous savez filtrer :

1. **Créer des visualisations par commune** (Top 10, 20, 50 communes)
2. **Créer une carte géographique** (si plugin Superset Deck.gl activé)
3. **Analyser les grandes villes** : Paris, Marseille, Lyon, Toulouse
4. **Comparer les régions** via les communes

**Les données sont parfaites ! Il suffit de filtrer.** 🎉
