import psycopg2

def check_table(host, port, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        # Vérifier les schémas
        print("\n📁 Schémas disponibles:")
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            ORDER BY schema_name;
        """)
        for schema in cursor.fetchall():
            print(f"  - {schema[0]}")
        
        # Vérifier les tables du schéma public
        print("\n📋 Tables dans le schéma public:")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        for table in cursor.fetchall():
            print(f"  - {table[0]}")
            
        # Vérifier la structure de la table deces
        print("\n🔍 Structure de la table deces:")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'deces'
            ORDER BY ordinal_position;
        """)
        for col in cursor.fetchall():
            print(f"  - {col[0]}: {col[1]}", end='')
            if col[2]:
                print(f" ({col[2]})")
            else:
                print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    PG_HOST = "10.169.128.247"
    PG_PORT = "5432"
    PG_DB = "postgres"
    PG_USER = "postgres"
    PG_PASS = "matheop2003"
    
    print("🔍 Vérification de la base de données...")
    check_table(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)