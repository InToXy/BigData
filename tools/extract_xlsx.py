import os
import pandas as pd

# Chemin vers le dossier contenant les fichiers xlsx
folder_path = '/home/matheo/medical-data-stack/data/source/xlsx'

# Parcourir tous les fichiers du dossier
for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(folder_path, filename)
        
        # Lire le fichier XLSX avec toutes les feuilles
        xls = pd.ExcelFile(file_path)
        
        print(f"Fichier: {filename}")
        # Parcourir chaque feuille
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            print(f"Feuille : {sheet_name}")
            print("Colonnes :", df.columns.tolist())
            print()  # Ligne vide pour lisibilité

