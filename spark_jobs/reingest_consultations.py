#!/usr/bin/env python3
"""
Script pour relancer uniquement l'ingestion de la table consultations
"""
import sys
sys.path.insert(0, '/home/jovyan/jobs')

from bronze_ingestion import (
    get_spark_session,
    process_postgres_source,
    test_connections
)

print("""
╔══════════════════════════════════════════════════════════════╗
║  RÉINGESTION TABLE CONSULTATIONS - Correction colonnes TIME ║
╚══════════════════════════════════════════════════════════════╝
""")

try:
    spark = get_spark_session()
    
    if not test_connections(spark):
        print("❌ Erreur de connexion")
        sys.exit(1)
    
    # Configuration pour consultations
    config = {
        "type": "postgres",
        "source_name": "Consultations",
        "path": "\"Consultation\"",
        "output_table": "consultations",
        "pii_columns": [],
        "preserve_columns": ["Date", "Code_diag", "Id_prof_sante", "Id_patient", "Id_mut"]
    }
    
    print("\n🎯 Traitement de la table Consultation...")
    print("   - Correction des colonnes TIME (Heure_debut, Heure_fin)")
    print("   - Conversion: TIMESTAMP → STRING (HH:mm:ss)")
    
    success = process_postgres_source(spark, config)
    
    if success:
        print("\n" + "=" * 80)
        print("✅ RÉINGESTION TERMINÉE AVEC SUCCÈS")
        print("=" * 80)
        print("\n📊 Vérification des données...")
        
        # Lire et afficher un échantillon
        df = spark.read.parquet("s3a://bronze/consultations")
        
        print("\n📋 Schéma des colonnes temporelles:")
        for field in df.schema:
            if any(kw in field.name.lower() for kw in ["heure", "date", "naissance"]):
                print(f"  {field.name}: {field.dataType}")
        
        print("\n📊 Échantillon de données (5 lignes):")
        df.select("date_naissance", "Heure_debut", "Heure_fin").show(5, truncate=False)
        
        print("\n🔎 Valeurs distinctes de Heure_debut:")
        df.select("Heure_debut").distinct().orderBy("Heure_debut").show(10, truncate=False)
        
        print("\n🔎 Valeurs distinctes de Heure_fin:")
        df.select("Heure_fin").distinct().orderBy("Heure_fin").show(10, truncate=False)
        
        print("\n✅ Les heures sont maintenant au format HH:mm:ss (STRING)")
        print("✅ Plus de dates '1970-01-01' indésirables!")
        
    else:
        print("\n❌ Échec de la réingestion")
        sys.exit(1)
        
except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    if 'spark' in locals():
        spark.stop()
