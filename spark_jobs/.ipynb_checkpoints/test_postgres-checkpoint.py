import os
import psycopg2
from psycopg2 import sql

def test_postgres_connection(host, port, database, user, password):
    """Teste la connexion à PostgreSQL et affiche les tables disponibles."""
    try:
        print(f"🔍 Test de connexion à PostgreSQL sur {host}:{port}...")
        
        # Connexion à PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        print("✅ Connexion établie!")
        
        # Créer un curseur
        cursor = conn.cursor()
        
        # Afficher les informations de connexion
        cursor.execute("SELECT current_database(), current_user;")
        info = cursor.fetchone()
        print("\nInformations de connexion:")
        print(f"Base de données: {info[0]}")
        print(f"Utilisateur: {info[1]}")
        
        # Liste des tables
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_schema, table_name;
        """)
        
        tables = cursor.fetchall()
        print("\nTables disponibles:")
        for schema, table in tables:
            print(f"- {schema}.{table}")
            
        cursor.close()
        conn.close()
        return True
            
    except Exception as e:
        print(f"❌ Test PostgreSQL échoué: {e}")
        return False
    """Teste la connexion à PostgreSQL et affiche les tables disponibles."""
    try:
        print(f"🔍 Test de connexion à PostgreSQL sur {host}:{port}...")
        
        # Tentative de lecture d'une requête simple
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", "SELECT current_database(), current_user") \
            .load()
            
        print("✅ Connexion établie!")
        print("\nInformations de connexion:")
        df.show()
        
        # Liste des tables
        tables_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """) \
            .load()
            
        print("\nTables disponibles:")
        tables_df.show(truncate=False)
        
        return True
            
    except Exception as e:
        print(f"❌ Test PostgreSQL échoué: {e}")
        return False

if __name__ == "__main__":
    print("""
    ╔═════════════════════════════════════╗
    ║  TEST DE CONNEXION POSTGRESQL       ║
    ╚═════════════════════════════════════╝
    """)
    
    # Paramètres de connexion
    PG_HOST = "10.169.128.247"
    PG_PORT = "5432"
    PG_DB = "postgres"
    PG_USER = "postgres"
    PG_PASS = "matheop2003"
    
    try:
        success = test_postgres_connection(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
        if not success:
            print("💥 Le test a échoué")
            exit(1)
        
        print("\n✨ Test terminé avec succès!")
        
    except Exception as e:
        print(f"💥 Erreur critique: {e}")
        import traceback
        traceback.print_exc()
        exit(1)