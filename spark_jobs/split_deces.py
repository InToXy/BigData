import os
import pandas as pd
import gc

def split_csv_file(input_file, output_dir, chunk_size=100000):
    """
    Divise un fichier CSV en plusieurs fichiers plus petits.
    
    Args:
        input_file (str): Chemin du fichier CSV d'entrée
        output_dir (str): Répertoire de sortie pour les morceaux
        chunk_size (int): Nombre de lignes par fichier
    """
    print(f"🔪 Découpage de {input_file} en morceaux de {chunk_size:,} lignes...")
    
    # Créer le répertoire de sortie si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Lire le fichier par morceaux
        reader = pd.read_csv(
            input_file, 
            chunksize=chunk_size,
            encoding='utf-8',
            low_memory=True
        )
        
        total_chunks = 0
        total_rows = 0
        
        for i, chunk in enumerate(reader, 1):
            # Nom du fichier de sortie
            output_file = os.path.join(output_dir, f"deces_part_{i:04d}.csv")
            
            # Écrire le morceau
            chunk.to_csv(
                output_file, 
                index=False,
                encoding='utf-8'
            )
            
            chunk_rows = len(chunk)
            total_rows += chunk_rows
            total_chunks += 1
            
            print(f"✓ Partie {i}: {chunk_rows:,} lignes → {output_file}")
            
            # Forcer le garbage collection
            del chunk
            gc.collect()
        
        print(f"\n✅ Découpage terminé!")
        print(f"   - {total_chunks} fichiers créés")
        print(f"   - {total_rows:,} lignes au total")
        print(f"   - Stockés dans: {output_dir}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors du découpage: {e}")
        raise

if __name__ == "__main__":
    # Chemins des fichiers
    INPUT_FILE = "/data/source/csv/deces.csv"
    OUTPUT_DIR = "/data/source/csv/deces_parts"
    CHUNK_SIZE = 50000  # 50k lignes par fichier
    
    print("""
    ╔═════════════════════════════════════════════╗
    ║    DÉCOUPAGE DU FICHIER DECES.CSV (2GO)    ║
    ╚═════════════════════════════════════════════╝
    """)
    
    try:
        success = split_csv_file(INPUT_FILE, OUTPUT_DIR, CHUNK_SIZE)
        if success:
            print("\n🎉 Découpage réussi! Vous pouvez maintenant traiter les fichiers individuellement.")
        else:
            print("\n💥 Échec du découpage")
            
    except Exception as e:
        print(f"\n💥 Erreur critique: {e}")
        import traceback
        traceback.print_exc()