#!/usr/bin/env python3#!/usr/bin/env python3

import subprocessimport subprocess

import osimport os

from datetime import datetimefrom datetime import datetime



# Configuration de la connexion# Configuration de la connexion

DB_CONFIG = {DB_CONFIG = {

    'container': 'chu_postgres_data',    'container': 'chu_postgres_data',

    'database': 'healthcare_data',    'database': 'healthcare_data',

    'user': 'admin'    'user': 'admin'

}}



# Liste des tables à extraire# Liste des tables à extraire

TABLES = [TABLES = [

    'deces',    'deces',

    'Professionnel_de_sante',    'Professionnel_de_sante',

    'Consultation',    'Consultation',

    'Prescription',    'Prescription',

    'Salle',    'Salle',

    'Patient',    'Patient',

    'Adher',    'Adher',

    'date',    'date',

    'AAAA',    'AAAA',

    'Diagnostic',    'Diagnostic',

    'Medicaments',    'Medicaments',

    'Laboratoire',    'Laboratoire',

    'Mutuelle',    'Mutuelle',

    'Specialites'    'Specialites'

]]



def create_output_dir():def create_output_dir():

    """Crée le répertoire de sortie s'il n'existe pas."""    """Crée le répertoire de sortie s'il n'existe pas."""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_dir = f'data/db_samples/{timestamp}'    output_dir = f'data/samples/{timestamp}'

    os.makedirs(output_dir, exist_ok=True)    os.makedirs(output_dir, exist_ok=True)

    return output_dir    return output_dir



def extract_sample(table_name, output_dir):def extract_sample(table_name, output_dir):

    """Extrait les 100 premiers et derniers enregistrements d'une table et les sauvegarde en CSV."""    """Extrait les 100 premiers et derniers enregistrements d'une table et les sauvegarde en CSV."""

    try:    try:

        print(f"📊 Extraction de {table_name}...")        print(f"📊 Extraction de {table_name}...")

                

        # Fichier de sortie combiné        # Fichier de sortie combiné

        output_file = f"{output_dir}/{table_name.lower()}_samples.csv"        output_file = f"{output_dir}/{table_name.lower()}_samples.csv"

                

        # D'abord on récupère le nom de la première colonne pour le tri        # Récupérer toutes les colonnes de la table pour trouver la première non-null

        column_cmd = [        column_cmd = [

            "docker", "exec", DB_CONFIG["container"],            "docker", "exec", DB_CONFIG["container"],

            "psql",            "psql",

            "-U", DB_CONFIG["user"],            "-U", DB_CONFIG["user"],

            "-d", DB_CONFIG["database"],            "-d", DB_CONFIG["database"],

            "-t", "-A",  # -t pour supprimer les en-têtes, -A pour unaligned output            "-t", "-A",  # -t pour supprimer les en-têtes, -A pour unaligned output

            "-c", f"""            "-c", f"""

                SELECT column_name                 SELECT column_name 

                FROM information_schema.columns                 FROM information_schema.columns 

                WHERE table_name = '{table_name.lower()}'                 WHERE table_name = '{table_name.lower()}' 

                AND column_name != ''                AND column_name != ''

                ORDER BY ordinal_position                 ORDER BY ordinal_position 

                LIMIT 1                LIMIT 1

            """            """

        ]        ]

                

        column_result = subprocess.run(        column_result = subprocess.run(

            column_cmd,            column_cmd,

            stdout=subprocess.PIPE,            stdout=subprocess.PIPE,

            stderr=subprocess.PIPE,            stderr=subprocess.PIPE,

            text=True            text=True

        )        )

                

        if column_result.returncode != 0 or not column_result.stdout.strip():        if column_result.returncode != 0 or not column_result.stdout.strip():

            # Si on ne trouve pas de colonne valide, on utilise une approche simple sans tri            # Si on ne trouve pas de colonne valide, on utilise une approche simple sans tri

            combined_cmd = [            combined_cmd = [

                "docker", "exec", DB_CONFIG["container"],                "docker", "exec", DB_CONFIG["container"],

                "psql",                "psql",

                "-U", DB_CONFIG["user"],                "-U", DB_CONFIG["user"],

                "-d", DB_CONFIG["database"],                "-d", DB_CONFIG["database"],

                "-c", f"""\\COPY (                "-c", f"""\\COPY (

                    WITH source_data AS (                    WITH source_data AS (

                        (SELECT *, 'Premier' as echantillon                        (SELECT *, 'Premier' as echantillon

                         FROM \"{table_name}\"                         FROM \"{table_name}\"

                         LIMIT 100)                         LIMIT 100)

                        UNION ALL                        UNION ALL

                        (SELECT *, 'Dernier' as echantillon                        (SELECT *, 'Dernier' as echantillon

                         FROM \"{table_name}\"                         FROM \"{table_name}\"

                         OFFSET (SELECT GREATEST(COUNT(*) - 100, 0) FROM \"{table_name}\"))                         OFFSET (SELECT GREATEST(COUNT(*) - 100, 0) FROM \"{table_name}\"))

                    )                    )

                    SELECT * FROM source_data                    SELECT * FROM source_data

                ) TO STDOUT WITH CSV HEADER"""                ) TO STDOUT WITH CSV HEADER"""

            ]            ]

        else:        else:

            sort_column = column_result.stdout.strip()            sort_column = column_result.stdout.strip()

            # Requête combinée pour les premiers et derniers enregistrements avec tri            # Requête combinée pour les premiers et derniers enregistrements avec tri

            combined_cmd = [            combined_cmd = [

                "docker", "exec", DB_CONFIG["container"],                "docker", "exec", DB_CONFIG["container"],

                "psql",                "psql",

                "-U", DB_CONFIG["user"],                "-U", DB_CONFIG["user"],

                "-d", DB_CONFIG["database"],                "-d", DB_CONFIG["database"],

                "-c", f"""\\COPY (                "-c", f"""\\COPY (

                    WITH source_data AS (                    WITH source_data AS (

                        (SELECT *, 'Premier' as echantillon                        (SELECT *, 'Premier' as echantillon

                         FROM \"{table_name}\"                         FROM \"{table_name}\"

                         ORDER BY \"{sort_column}\" ASC                         ORDER BY \"{sort_column}\" ASC

                         LIMIT 100)                         LIMIT 100)

                        UNION ALL                        UNION ALL

                        (SELECT *, 'Dernier' as echantillon                        (SELECT *, 'Dernier' as echantillon

                         FROM \"{table_name}\"                         FROM \"{table_name}\"

                         ORDER BY \"{sort_column}\" DESC                         ORDER BY \"{sort_column}\" DESC

                         LIMIT 100)                         LIMIT 100)

                    )                    )

                    SELECT * FROM source_data                    SELECT * FROM source_data

                ) TO STDOUT WITH CSV HEADER"""                ) TO STDOUT WITH CSV HEADER"""

            ]            ]

                

        # Extraction des enregistrements        # Extraction des enregistrements

        with open(output_file, 'w') as f:        with open(output_file, 'w') as f:

            result = subprocess.run(            result = subprocess.run(

                combined_cmd,                combined_cmd,

                stdout=f,                stdout=f,

                stderr=subprocess.PIPE,                stderr=subprocess.PIPE,

                text=True                text=True

            )            )

                

        if result.returncode == 0:        if result.returncode == 0:

            # Compter les lignes dans le fichier            # Compter les lignes dans le fichier

            with open(output_file, 'r') as f:            with open(output_file, 'r') as f:

                total_count = sum(1 for _ in f) - 1  # -1 pour l'en-tête                total_count = sum(1 for _ in f) - 1  # -1 pour l'en-tête

                                

            print(f"✅ {table_name}:")            print(f"✅ {table_name}:")

            print(f"   - Total: {total_count} lignes → {output_file}")            print(f"   - Total: {total_count} lignes → {output_file}")

            return True            return True

        else:        else:

            print(f"❌ Erreur avec {table_name}: {result.stderr}")            print(f"❌ Erreur avec {table_name}: {result.stderr}")

            return False            return False

                        

    except Exception as e:    except Exception as e:

        print(f"❌ Erreur avec {table_name}: {e}")        print(f"❌ Erreur avec {table_name}: {e}")

        return False        return False



def main():def main():

    """Fonction principale."""    """Fonction principale."""

    print("""    print("""

    ╔════════════════════════════════════════╗    ╔════════════════════════════════════════╗

    ║      EXTRACTION DES ÉCHANTILLONS       ║    ║      EXTRACTION DES ÉCHANTILLONS       ║

    ║   Premier et dernier 100 par table     ║    ║   Premier et dernier 100 par table     ║

    ╚════════════════════════════════════════╝    ╚════════════════════════════════════════╝

    """)    """)

        

    # Création du répertoire de sortie    # Création du répertoire de sortie

    output_dir = create_output_dir()    output_dir = create_output_dir()

    print(f"📁 Dossier de sortie: {output_dir}")    print(f"📁 Dossier de sortie: {output_dir}")

        

    # Test de la connexion Docker    # Test de la connexion Docker

    try:    try:

        test_cmd = [        test_cmd = [

            "docker", "exec", DB_CONFIG["container"],            "docker", "exec", DB_CONFIG["container"],

            "psql", "-U", DB_CONFIG["user"],            "psql", "-U", DB_CONFIG["user"],

            "-d", DB_CONFIG["database"],            "-d", DB_CONFIG["database"],

            "-c", "SELECT 1"            "-c", "SELECT 1"

        ]        ]

        subprocess.run(test_cmd, check=True, capture_output=True)        subprocess.run(test_cmd, check=True, capture_output=True)

        print("✅ Connexion à la base de données établie")        print("✅ Connexion à la base de données établie")

                

        # Extraction pour chaque table        # Extraction pour chaque table

        results = []        results = []

        for table in TABLES:        for table in TABLES:

            success = extract_sample(table, output_dir)            success = extract_sample(table, output_dir)

            results.append((table, success))            results.append((table, success))

                

        # Résumé        # Résumé

        print("\n📊 RÉSUMÉ:")        print("\n📊 RÉSUMÉ:")

        success_count = sum(1 for _, success in results if success)        success_count = sum(1 for _, success in results if success)

        print(f"✓ {success_count}/{len(TABLES)} tables traitées avec succès")        print(f"✓ {success_count}/{len(TABLES)} tables traitées avec succès")

                

        if success_count != len(TABLES):        if success_count != len(TABLES):

            print("\n❌ Tables en erreur:")            print("\n❌ Tables en erreur:")

            for table, success in results:            for table, success in results:

                if not success:                if not success:

                    print(f"  - {table}")                    print(f"  - {table}")

                                        

    except subprocess.CalledProcessError as e:    except subprocess.CalledProcessError as e:

        print(f"\n❌ Erreur de connexion Docker/PostgreSQL: {e}")        print(f"\n❌ Erreur de connexion Docker/PostgreSQL: {e}")

    except Exception as e:    except Exception as e:

        print(f"\n❌ Erreur inattendue: {e}")        print(f"\n❌ Erreur inattendue: {e}")

        

if __name__ == "__main__":if __name__ == "__main__":

    main()    main()