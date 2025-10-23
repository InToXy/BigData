# 📊 Guide des Exports CSV

## 📁 Emplacement des fichiers
```
/home/alban/BigData/BigData/graphes/tests_ingestion/exports/
```

## 📋 Liste des fichiers exportés

### 1. **schema_consultations.csv**
Schéma complet de la table avec les types de données et les valeurs manquantes.

**Colonnes :**
- `Colonne` : Nom de la colonne
- `Type` : Type de données (object, int32, datetime64, etc.)
- `Nulls` : Nombre de valeurs manquantes
- `Pourcentage` : Pourcentage de valeurs manquantes

**Utilisation :**
```bash
# Afficher le schéma
cat exports/schema_consultations.csv | column -t -s,

# Ouvrir avec LibreOffice
libreoffice --calc exports/schema_consultations.csv
```

---

### 2. **statistiques_consultations.csv**
Statistiques descriptives pour les colonnes numériques.

**Métriques incluses :**
- count : Nombre de valeurs
- mean : Moyenne
- std : Écart-type
- min : Valeur minimale
- 25% : Premier quartile
- 50% : Médiane
- 75% : Troisième quartile
- max : Valeur maximale

**Colonnes analysées :**
- `_version`
- `Id_mut`
- `id_patient`

---

### 3. **echantillon_10_premieres_lignes.csv**
Les 10 premières lignes de la table consultations avec toutes les colonnes.

**Contenu :** Données complètes pour analyse et vérification

**Utilisation :**
```bash
# Afficher de manière lisible
cat exports/echantillon_10_premieres_lignes.csv | head -5 | column -t -s,

# Convertir en format Excel
python3 -c "import pandas as pd; df = pd.read_csv('exports/echantillon_10_premieres_lignes.csv'); df.to_excel('echantillon_tete.xlsx', index=False)"
```

---

### 4. **echantillon_10_dernieres_lignes.csv**
Les 10 dernières lignes de la table consultations.

**Utilité :** Vérifier les données les plus récentes

---

### 5. **valeurs_manquantes.csv**
Liste des colonnes contenant des valeurs NULL/manquantes.

**Colonnes :**
- `Colonne` : Nom de la colonne
- `Nulls` : Nombre de valeurs manquantes
- `Pourcentage` : Pourcentage de valeurs manquantes

**Résultat actuel :**
- `Num_consultation` : 100% de valeurs manquantes (1,027,157 lignes)

---

### 6. **plages_valeurs.csv**
Plages de valeurs pour les colonnes numériques.

**Colonnes :**
- `Colonne` : Nom de la colonne
- `Min` : Valeur minimale
- `Max` : Valeur maximale
- `Moyenne` : Moyenne arithmétique

**Exemples :**
- `_version` : Min=1, Max=1, Moyenne=1
- `Id_mut` : Min=0, Max=253, Moyenne=195.16
- `id_patient` : Min=1, Max=100,000, Moyenne=47,145.61

---

### 7. **distribution_*.csv**
Distribution des valeurs pour les colonnes catégorielles.

**Fichiers générés :**
- `distribution__source_system.csv`
- `distribution__source_table.csv`
- `distribution__batch_id.csv`

**Colonnes :**
- `Valeur` : Valeur unique
- `Fréquence` : Nombre d'occurrences
- `Pourcentage` : Pourcentage du total

---

### 8. **fichiers_parquet.csv**
Métadonnées des fichiers Parquet sur MinIO.

**Colonnes :**
- `Fichier` : Nom du fichier
- `Taille` : Taille en MB/KB
- `Dernière modification` : Date et heure de dernière modification

**Résultats :**
- 3 fichiers Parquet
- Taille totale : 263.91 MB sur disque
- 998.06 MB en mémoire après chargement

---

## 🔧 Commandes utiles

### Consulter tous les exports
```bash
cd /home/alban/BigData/BigData/graphes/tests_ingestion/exports

# Lister tous les fichiers avec leur taille
ls -lh

# Compter le nombre de lignes dans chaque fichier
wc -l *.csv
```

### Ouvrir avec des outils graphiques
```bash
# LibreOffice Calc
libreoffice --calc schema_consultations.csv

# Gnumeric
gnumeric schema_consultations.csv
```

### Convertir en Excel avec Python
```python
import pandas as pd
import os

export_dir = "/home/alban/BigData/BigData/graphes/tests_ingestion/exports"
output_dir = "/home/alban/BigData/BigData/graphes/tests_ingestion/exports_excel"
os.makedirs(output_dir, exist_ok=True)

csv_files = [f for f in os.listdir(export_dir) if f.endswith('.csv')]

for csv_file in csv_files:
    csv_path = os.path.join(export_dir, csv_file)
    excel_file = csv_file.replace('.csv', '.xlsx')
    excel_path = os.path.join(output_dir, excel_file)
    
    df = pd.read_csv(csv_path)
    df.to_excel(excel_path, index=False)
    print(f"✅ {csv_file} → {excel_file}")
```

### Fusionner tous les exports dans un seul fichier Excel (multiples onglets)
```python
import pandas as pd
import os

export_dir = "/home/alban/BigData/BigData/graphes/tests_ingestion/exports"
output_file = "/home/alban/BigData/BigData/graphes/tests_ingestion/rapport_consultations.xlsx"

csv_files = [f for f in os.listdir(export_dir) if f.endswith('.csv')]

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for csv_file in csv_files:
        csv_path = os.path.join(export_dir, csv_file)
        sheet_name = csv_file.replace('.csv', '')[:31]  # Excel limite à 31 caractères
        
        df = pd.read_csv(csv_path)
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"✅ Onglet créé : {sheet_name}")

print(f"\n📊 Rapport complet créé : {output_file}")
```

---

## 📈 Analyses avancées

### Analyser les motifs de consultation
```bash
# Top 10 des motifs
cat exports/echantillon_10_premieres_lignes.csv | cut -d',' -f17 | sort | uniq -c | sort -rn | head -10
```

### Vérifier la qualité des données
```bash
# Colonnes avec le plus de valeurs manquantes
cat exports/valeurs_manquantes.csv | sort -t',' -k3 -rn
```

---

## 🔄 Re-générer les exports

Pour regénérer tous les exports CSV :

```bash
cd /home/alban/BigData/BigData/graphes/tests_ingestion
./run_test.sh
```

Les fichiers seront automatiquement écrasés et mis à jour.

---

## 📊 Métriques clés

| Métrique | Valeur |
|----------|--------|
| **Nombre de lignes** | 1,027,157 |
| **Nombre de colonnes** | 20 |
| **Taille en mémoire** | 998.06 MB |
| **Taille sur disque** | 263.91 MB |
| **Taux de compression** | ~73.5% |
| **Fichiers Parquet** | 3 |
| **Doublons** | 0 |
| **Colonnes avec nulls** | 1 (Num_consultation) |

---

## 🎯 Cas d'usage

### 1. Audit de qualité des données
```bash
# Vérifier les valeurs manquantes
cat exports/valeurs_manquantes.csv

# Vérifier les plages de valeurs
cat exports/plages_valeurs.csv
```

### 2. Documentation du schéma
```bash
# Générer une documentation Markdown du schéma
cat exports/schema_consultations.csv | python3 -c "
import sys
import csv
reader = csv.DictReader(sys.stdin)
print('# Schéma de la table consultations\n')
for row in reader:
    print(f\"- **{row['Colonne']}** ({row['Type']}) - Nulls: {row['Nulls']} ({row['Pourcentage']})\")
"
```

### 3. Import dans une base de données
```python
import pandas as pd
from sqlalchemy import create_engine

# Lire le CSV
df = pd.read_csv('exports/echantillon_10_premieres_lignes.csv')

# Connexion PostgreSQL
engine = create_engine('postgresql://user:pass@localhost:5432/db')

# Importer dans une table
df.to_sql('consultations_sample', engine, if_exists='replace', index=False)
```

---

## 💡 Astuces

1. **Vérifier rapidement les types de colonnes :**
   ```bash
   cat exports/schema_consultations.csv | grep -v "^Colonne" | cut -d',' -f2 | sort | uniq -c
   ```

2. **Trouver les colonnes avec 100% de nulls :**
   ```bash
   cat exports/valeurs_manquantes.csv | grep "100.00%"
   ```

3. **Comparer les échantillons tête/queue :**
   ```bash
   diff exports/echantillon_10_premieres_lignes.csv exports/echantillon_10_dernieres_lignes.csv
   ```

---

## 📝 Notes

- Les exports sont regénérés à chaque exécution du script de test
- Les fichiers CSV utilisent la virgule (`,`) comme séparateur
- Les nombres utilisent le formatage avec virgules (ex: `1,027,157`)
- Les dates sont au format ISO 8601 (`YYYY-MM-DD HH:MM:SS`)
- Les fichiers sont encodés en UTF-8

---

**📅 Dernière mise à jour :** 23/10/2025  
**🔧 Script source :** `test_consultation_bronze.py`  
**📁 Dossier exports :** `/home/alban/BigData/BigData/graphes/tests_ingestion/exports/`
