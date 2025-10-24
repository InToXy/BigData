#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime
import glob

def format_header_line(line, width=80):
    """Formate une ligne de titre avec des caractères décoratifs."""
    return f"""
{'=' * width}
{line.center(width)}
{'=' * width}
"""

def process_samples_to_text(base_dirs):
    """Combine tous les fichiers d'échantillons en un seul fichier texte."""
    print("""
    ╔════════════════════════════════════════╗
    ║     COMBINAISON DES ÉCHANTILLONS       ║
    ║        EN UN SEUL FICHIER TXT          ║
    ╚════════════════════════════════════════╝
    """)
    
    # Création du répertoire de sortie
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f'data/combined_txt/{timestamp}'
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/all_samples.txt"
    
    print(f"📁 Dossier de sortie: {output_dir}")
    
    # Compteurs pour les statistiques
    total_files = 0
    total_samples = 0
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # En-tête du fichier
            f.write(format_header_line("ÉCHANTILLONS DE DONNÉES"))
            f.write(f"Date de génération: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Traiter chaque répertoire de base
            for base_dir in base_dirs:
                if not os.path.exists(base_dir):
                    print(f"⚠️ Dossier non trouvé: {base_dir}")
                    continue
                
                # Récupérer tous les sous-dossiers
                sample_dirs = sorted(glob.glob(os.path.join(base_dir, '*')))
                
                for sample_dir in sample_dirs:
                    # Chercher tous les fichiers CSV dans ce dossier
                    sample_files = glob.glob(os.path.join(sample_dir, "*_samples.csv"))
                    
                    for sample_file in sample_files:
                        filename = os.path.basename(sample_file)
                        source_name = filename.replace("_samples.csv", "")
                        print(f"📊 Traitement de {filename}...")
                        
                        try:
                            # Lecture du fichier CSV
                            df = pd.read_csv(sample_file)
                            
                            # Écriture dans le fichier texte
                            f.write(format_header_line(f"Source: {source_name}"))
                            
                            # Information sur l'échantillon
                            premier_count = len(df[df['echantillon'] == 'Premier'])
                            dernier_count = len(df[df['echantillon'] == 'Dernier'])
                            f.write(f"Nombre d'enregistrements:\n")
                            f.write(f"- Premiers: {premier_count}\n")
                            f.write(f"- Derniers: {dernier_count}\n")
                            f.write(f"- Total: {len(df)}\n\n")
                            
                            # En-têtes des colonnes
                            headers = df.columns.tolist()
                            f.write("COLONNES:\n")
                            for i, header in enumerate(headers, 1):
                                f.write(f"{i}. {header}\n")
                            f.write("\n")
                            
                            # Échantillon des données
                            f.write("DONNÉES:\n")
                            f.write(df.to_string(index=False))
                            f.write("\n\n")
                            
                            total_files += 1
                            total_samples += len(df)
                            print(f"✅ {len(df)} lignes traitées")
                            
                        except Exception as e:
                            print(f"❌ Erreur avec {filename}: {str(e)}")
            
            # Résumé final
            f.write(format_header_line("RÉSUMÉ"))
            f.write(f"Total des fichiers traités: {total_files}\n")
            f.write(f"Total des échantillons: {total_samples}\n")
    
    except Exception as e:
        print(f"❌ Erreur lors de la génération du fichier: {str(e)}")
        return
    
    print(f"\n✅ Fichier texte créé avec succès:")
    print(f"   - {total_files} fichiers traités")
    print(f"   - {total_samples} échantillons au total")
    print(f"   - Fichier: {output_file}")

if __name__ == "__main__":
    # Dossiers à traiter
    dirs_to_process = [
        'data/csv_samples',    # Échantillons des CSV
        'data/samples'      # Échantillons de la base de données
    ]
    
    process_samples_to_text(dirs_to_process)