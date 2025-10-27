#!/usr/bin/env python3
import os
import pandas as pd
import chardet
from datetime import datetime

# Liste des fichiers CSV dans le dossier source/csv
CSV_DIR = 'data/source/csv'

def detect_encoding(file_path):
    """Détecte l'encodage d'un fichier."""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def detect_separator(file_path, encoding):
    """Détecte le séparateur utilisé dans le fichier CSV."""
    # Lire les premières lignes du fichier
    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        header = f.readline().strip()
        
    # Liste des séparateurs possibles avec leur compte
    separators = {
        ',': header.count(','),
        ';': header.count(';'),
        '|': header.count('|'),
        '\t': header.count('\t')
    }
    
    # Retourner le séparateur le plus fréquent
    max_sep = max(separators.items(), key=lambda x: x[1])
    return max_sep[0] if max_sep[1] > 0 else ','

def create_output_dir():
    """Crée le répertoire de sortie s'il n'existe pas."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f'data/csv_samples/{timestamp}'
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def extract_sample(csv_file, output_dir):
    """Extrait les 100 premiers et derniers enregistrements d'un fichier CSV."""
    try:
        filename = os.path.basename(csv_file)
        print(f"📊 Extraction de {filename}...")
        
        # Fichier de sortie
        output_file = f"{output_dir}/{os.path.splitext(filename)[0]}_samples.csv"
        
        # Détection de l'encodage et du séparateur
        encoding = detect_encoding(csv_file)
        separator = detect_separator(csv_file, encoding)
        
        # Lecture du fichier CSV avec les paramètres détectés
        df = pd.read_csv(
            csv_file,
            encoding=encoding,
            sep=separator,
            low_memory=False,
            on_bad_lines='skip',  # Ignorer les lignes mal formées
            dtype=str  # Tout lire comme texte pour éviter les problèmes de type
        )
        total_rows = len(df)
        
        if total_rows == 0:
            print(f"⚠️ {filename} est vide (encodage: {encoding}, séparateur: {separator})")
            # Créer un fichier vide avec l'en-tête
            if df.columns.size > 0:
                df.to_csv(output_file, index=False)
            return True
        
        # Sélection des premiers et derniers enregistrements
        first_100 = df.head(100).copy()
        last_100 = df.tail(100).copy()
        
        # Ajout de la colonne echantillon
        first_100['echantillon'] = 'Premier'
        last_100['echantillon'] = 'Dernier'
        
        # Combinaison des résultats
        result = pd.concat([first_100, last_100])
        
        # Sauvegarde du résultat
        result.to_csv(output_file, index=False)
        
        total_samples = len(result)
        print(f"✅ {filename}:")
        print(f"   - Total: {total_samples} lignes → {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur avec {filename}: {str(e)}")
        return False

def main():
    """Fonction principale."""
    print("""
    ╔════════════════════════════════════════╗
    ║      EXTRACTION DES ÉCHANTILLONS       ║
    ║  Premier et dernier 100 lignes CSV     ║
    ╚════════════════════════════════════════╝
    """)
    
    # Création du répertoire de sortie
    output_dir = create_output_dir()
    print(f"📁 Dossier de sortie: {output_dir}")
    
    # Liste tous les fichiers CSV
    try:
        csv_files = [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not csv_files:
            print(f"❌ Aucun fichier CSV trouvé dans {CSV_DIR}")
            return
            
        print(f"📂 {len(csv_files)} fichiers CSV trouvés")
        
        # Extraction pour chaque fichier
        results = []
        for csv_file in csv_files:
            success = extract_sample(csv_file, output_dir)
            results.append((os.path.basename(csv_file), success))
        
        # Résumé
        print("\n📊 RÉSUMÉ:")
        success_count = sum(1 for _, success in results if success)
        print(f"✓ {success_count}/{len(csv_files)} fichiers traités avec succès")
        
        if success_count != len(csv_files):
            print("\n❌ Fichiers en erreur:")
            for file, success in results:
                if not success:
                    print(f"  - {file}")
                    
    except Exception as e:
        print(f"\n❌ Erreur inattendue: {e}")

if __name__ == "__main__":
    main()