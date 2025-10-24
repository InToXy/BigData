import pandas as pd
import os
import glob
from pathlib import Path
import shutil

def convert_xlsx_to_csv_folder(source_folder, target_folder, sheet_name=None, encoding='utf-8', delimiter=','):
    """
    Convertit tous les fichiers Excel d'un dossier source vers un dossier cible en CSV
    
    Args:
        source_folder (str): Dossier source contenant les fichiers Excel
        target_folder (str): Dossier cible pour les fichiers CSV
        sheet_name (str): Nom spécifique de feuille à convertir (optionnel)
        encoding (str): Encodage des fichiers CSV
        delimiter (str): Séparateur CSV
    """
    
    # Vérification du dossier source
    if not os.path.exists(source_folder):
        print(f"❌ Le dossier source n'existe pas: {source_folder}")
        return False
    
    # Création du dossier cible
    os.makedirs(target_folder, exist_ok=True)
    print(f"📁 Dossier cible créé: {target_folder}")
    
    # Recherche des fichiers Excel
    excel_patterns = [
        os.path.join(source_folder, "*.xlsx"),
        os.path.join(source_folder, "*.xls")
    ]
    
    excel_files = []
    for pattern in excel_patterns:
        excel_files.extend(glob.glob(pattern))
    
    if not excel_files:
        print("❌ Aucun fichier Excel trouvé dans le dossier source")
        return False
    
    print(f"📊 {len(excel_files)} fichier(s) Excel trouvé(s)")
    
    # Statistiques
    total_files_converted = 0
    total_sheets_converted = 0
    total_rows_converted = 0
    
    # Conversion de chaque fichier
    for excel_file in excel_files:
        try:
            file_name = os.path.basename(excel_file)
            print(f"\n🔄 Conversion de: {file_name}")
            
            # Lecture du fichier Excel
            excel_data = pd.ExcelFile(excel_file)
            
            # Déterminer les feuilles à convertir
            if sheet_name:
                sheets_to_convert = [sheet_name] if sheet_name in excel_data.sheet_names else []
            else:
                sheets_to_convert = excel_data.sheet_names
            
            if not sheets_to_convert:
                print(f"  ⚠️  Aucune feuille trouvée pour {file_name}")
                continue
            
            # Conversion de chaque feuille
            for sheet in sheets_to_convert:
                try:
                    # Lecture de la feuille
                    df = pd.read_excel(excel_file, sheet_name=sheet)
                    
                    # Nom du fichier CSV de sortie
                    base_name = Path(excel_file).stem
                    if len(sheets_to_convert) > 1:
                        csv_filename = f"{base_name}_{sheet}.csv"
                    else:
                        csv_filename = f"{base_name}.csv"
                    
                    # Chemin complet du fichier CSV
                    csv_path = os.path.join(target_folder, csv_filename)
                    
                    # Conversion en CSV
                    df.to_csv(csv_path, index=False, encoding=encoding, sep=delimiter)
                    
                    # Statistiques
                    total_sheets_converted += 1
                    total_rows_converted += len(df)
                    
                    print(f"  ✅ {sheet} -> {csv_filename}")
                    print(f"     📈 {len(df):,} lignes × {len(df.columns)} colonnes")
                    
                except Exception as e:
                    print(f"  ❌ Erreur avec la feuille '{sheet}': {e}")
            
            total_files_converted += 1
            
        except Exception as e:
            print(f"❌ Erreur avec le fichier {excel_file}: {e}")
    
    # Rapport final
    print(f"\n🎉 CONVERSION TERMINÉE!")
    print(f"📂 Dossier source: {source_folder}")
    print(f"📂 Dossier cible: {target_folder}")
    print(f"📄 Fichiers Excel traités: {total_files_converted}/{len(excel_files)}")
    print(f"📋 Feuilles converties: {total_sheets_converted}")
    print(f"📊 Lignes totales converties: {total_rows_converted:,}")
    
    return True

# Exemple d'utilisation simple
if __name__ == "__main__":
    source = "../data/source/xlsx"  # Dossier contenant vos fichiers Excel
    target = "../data/source/csv"    # Dossier où sauvegarder les CSV
    
    convert_xlsx_to_csv_folder(source, target)