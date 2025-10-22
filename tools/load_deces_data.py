import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import os
from tqdm import tqdm

def load_deces_data(host, port, database, user, password, csv_path):
    """Charge les données du fichier CSV dans la table deces."""
    try:
        print(f"📊 Lecture du fichier CSV: {csv_path}")
        # Lecture du CSV avec pandas par chunks pour économiser la mémoire
        chunk_size = 10000
        chunks = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
        
        # Connexion à PostgreSQL
        print(f"� Connexion à PostgreSQL sur {host}:{port}...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        # Préparation de la requête d'insertion
        insert_query = """
        INSERT INTO public.deces (
            nom, prenom, sexe, date_naissance,
            code_lieu_naissance, lieu_naissance, pays_naissance,
            date_deces, code_lieu_deces, numero_acte_deces
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        total_rows = 0
        print("📥 Insertion des données par lots...")
        for chunk in chunks:
            # Convertir les dates en format datetime et les transformer en chaînes ISO
            for date_col in ['date_deces', 'date_naissance']:
                if date_col in chunk.columns:
                    chunk[date_col] = pd.to_datetime(chunk[date_col], errors='coerce')
                    # Convertir les dates en format ISO string, None pour les NaT
                    chunk[date_col] = chunk[date_col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        
            # Conversion du chunk en liste de tuples pour l'insertion
            # Remplacer NaN et NaT par None pour chaque colonne
            for col in chunk.columns:
                chunk[col] = chunk[col].where(chunk[col].notna(), None)
            
            # Convertir en liste de tuples
            values = chunk[[
                'nom', 'prenom', 'sexe', 'date_naissance',
                'code_lieu_naissance', 'lieu_naissance', 'pays_naissance',
                'date_deces', 'code_lieu_deces', 'numero_acte_deces'
            ]].values.tolist()
            
            # Insertion par lots
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                execute_batch(cursor, insert_query, batch)
                conn.commit()
            
            total_rows += len(values)
            print(f"✓ {total_rows} lignes insérées...")
        
        print("✅ Données insérées avec succès!")
        
        # Vérification du nombre total de lignes
        cursor.execute("SELECT COUNT(*) FROM deces")
        total_rows = cursor.fetchone()[0]
        print(f"📊 Nombre total de lignes dans la table: {total_rows}")
        
        cursor.close()
        conn.close()
        return True
            
    except Exception as e:
        print(f"❌ Erreur lors du chargement des données: {e}")
        return False

if __name__ == "__main__":
    print("""
    ╔═════════════════════════════════════╗
    ║  CHARGEMENT DES DONNÉES DECES       ║
    ╚═════════════════════════════════════╝
    """)
    
    # Paramètres de connexion
    PG_HOST = "10.169.128.247"
    PG_PORT = "5432"
    PG_DB = "postgres"
    PG_USER = "postgres"
    PG_PASS = "matheop2003"
    
    # Chemin vers le fichier CSV
    CSV_PATH = "data/source/csv/deces.csv"
    
    try:
        success = load_deces_data(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS, CSV_PATH)
        if not success:
            print("💥 Le chargement des données a échoué")
            exit(1)
        
        print("\n✨ Opération terminée avec succès!")
        
    except Exception as e:
        print(f"💥 Erreur critique: {e}")
        import traceback
        traceback.print_exc()
        exit(1)