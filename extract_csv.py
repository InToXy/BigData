import os
import pandas as pd
import chardet

folder_path = '/home/matheo/medical-data-stack/data/source/csv'

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())
            encoding = result['encoding']
            
            df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
            
            print(f"Fichier: {filename}")
            print("Colonnes :", df.columns.tolist())
            print()
        except Exception as e:
            print(f"Erreur avec le fichier {filename}: {e}")
