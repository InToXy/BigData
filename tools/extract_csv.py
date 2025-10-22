import os
import pandas as pd
import chardet
from tabulate import tabulate
import csv

def detect_separator(file_path, encoding):
    """Détecte le séparateur utilisé dans le fichier CSV."""
    with open(file_path, 'r', encoding=encoding) as f:
        # Lire les premières lignes
        first_line = f.readline()
        
        # Liste des séparateurs possibles à tester
        separators = [',', ';', '|', '\t']
        
        # Compter les occurrences de chaque séparateur
        counts = {sep: first_line.count(sep) for sep in separators}
        
        # Retourner le séparateur le plus fréquent
        max_sep = max(counts.items(), key=lambda x: x[1])
        
        # Si aucun séparateur trouvé, retourner None
        if max_sep[1] == 0:
            return None
        
        return max_sep[0]

def print_separator():
    print("\n" + "="*100 + "\n")

folder_path = 'data/source/csv'

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        try:
            # Détection de l'encodage
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())
            encoding = result['encoding']
            
            # Détection du séparateur
            separator = detect_separator(file_path, encoding)
            
            # Lecture du CSV avec le séparateur détecté
            df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip', 
                           sep=separator, engine='python')
            
            print_separator()
            print(f"📄 Fichier: {filename}")
            print(f"📊 Nombre total de lignes: {len(df)}")
            print(f"🔤 Encodage détecté: {encoding}")
            print(f"🔍 Séparateur détecté: '{separator}'")
            if separator == ',':
                print("   Type: Virgule (CSV standard)")
            elif separator == ';':
                print("   Type: Point-virgule (CSV européen)")
            elif separator == '\t':
                print("   Type: Tabulation (TSV)")
            elif separator == '|':
                print("   Type: Pipe (Format alternatif)")
            
            print("\n📋 Colonnes:", df.columns.tolist())
            
            print("\n🔍 3 premières lignes:")
            print(tabulate(df.head(3), headers='keys', tablefmt='pretty', showindex=False))
            
        except Exception as e:
            print(f"❌ Erreur avec le fichier {filename}: {e}")
            
    print_separator()
