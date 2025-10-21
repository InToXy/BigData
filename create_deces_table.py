import psycopg2
from psycopg2 import sql

def create_deces_table(host, port, database, user, password):
    """Crée la table deces dans PostgreSQL."""
    try:
        print(f"🔍 Connexion à PostgreSQL sur {host}:{port}...")
        
        # Connexion à PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        # Créer un curseur
        cursor = conn.cursor()
        
        # Supprimer la table si elle existe
        print("🗑️  Suppression de la table existante...")
        cursor.execute("DROP TABLE IF EXISTS public.deces CASCADE;")
        conn.commit()
        
        # Vérifier si le schéma public existe
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'public';
        """)
        if not cursor.fetchone():
            print("📁 Création du schéma public...")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public;")
            conn.commit()
        
        print("🔍 Vérification des privilèges...")
        cursor.execute("GRANT ALL ON SCHEMA public TO postgres;")
        cursor.execute("GRANT ALL ON SCHEMA public TO public;")
        
        # Création de la table deces
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.deces (
            id SERIAL PRIMARY KEY,
            nom VARCHAR(100),
            prenom VARCHAR(100),
            sexe VARCHAR(1),
            date_naissance DATE,
            code_lieu_naissance VARCHAR(10),
            lieu_naissance VARCHAR(100),
            pays_naissance VARCHAR(100),
            date_deces DATE,
            code_lieu_deces VARCHAR(10),
            numero_acte_deces VARCHAR(20)
        );
        """
        
        print("🔧 Création de la table deces...")
        cursor.execute(create_table_query)
        
        # Création des index pour optimiser les performances
        print("🔧 Création des index...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nom ON public.deces(nom);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date_deces ON public.deces(date_deces);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date_naissance ON public.deces(date_naissance);")
        
        # Donner les privilèges sur la table
        print("🔑 Attribution des privilèges...")
        cursor.execute("GRANT ALL PRIVILEGES ON TABLE public.deces TO postgres;")
        cursor.execute("GRANT ALL PRIVILEGES ON SEQUENCE public.deces_id_seq TO postgres;")
        
        # Valider les changements
        conn.commit()
        print("✅ Table deces créée avec succès!")
        
        cursor.close()
        conn.close()
        return True
            
    except Exception as e:
        print(f"❌ Erreur lors de la création de la table: {e}")
        return False

if __name__ == "__main__":
    print("""
    ╔═════════════════════════════════════╗
    ║  CRÉATION DE LA TABLE DECES         ║
    ╚═════════════════════════════════════╝
    """)
    
    # Paramètres de connexion
    PG_HOST = "10.169.128.247"
    PG_PORT = "5432"
    PG_DB = "postgres"
    PG_USER = "postgres"
    PG_PASS = "matheop2003"
    
    try:
        success = create_deces_table(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
        if not success:
            print("💥 La création de la table a échoué")
            exit(1)
        
        print("\n✨ Opération terminée avec succès!")
        
    except Exception as e:
        print(f"💥 Erreur critique: {e}")
        import traceback
        traceback.print_exc()
        exit(1)