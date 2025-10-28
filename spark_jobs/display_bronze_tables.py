"""
Affiche les 10 premières lignes de chaque table Bronze sous forme de tableaux
Exécuté depuis le conteneur Jupyter avec les bonnes credentials S3
"""

from pyspark.sql import SparkSession
from datetime import datetime

def get_spark_session():
    """Crée une session Spark avec configuration S3A pour MinIO"""
    spark = SparkSession.builder \
        .appName("Display_Bronze_Tables") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def print_header(title):
    """Affiche un en-tête formaté"""
    print(f"\n{'='*120}")
    print(f"  {title}")
    print(f"{'='*120}\n")

def display_table_sample(spark, table_name, s3_path):
    """Affiche les 10 premières lignes d'une table"""
    print_header(f"📊 {table_name.upper()}")
    
    try:
        # Lire la table
        df = spark.read.parquet(s3_path)
        total_rows = df.count()
        
        print(f"📈 Total lignes: {total_rows:,}")
        print(f"📝 Nombre de colonnes: {len(df.columns)}")
        print(f"\n🔍 10 premières lignes:\n")
        
        # Afficher sous forme de tableau
        df.show(10, truncate=40, vertical=False)
        
    except Exception as e:
        print(f"❌ Erreur: {str(e)}\n")

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                          VISUALISATION TABLES BRONZE                                                 ║
║                                            (10 premières lignes)                                                     ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
    """)
    
    start_time = datetime.now()
    spark = get_spark_session()
    print("✅ Session Spark initialisée\n")
    
    # Définir les tables à afficher
    tables = [
        ("DECES", "s3a://bronze/deces/"),
        ("ETABLISSEMENTS", "s3a://bronze/etablissements/"),
        ("PROFESSIONNELS_SANTE", "s3a://bronze/professionnels_sante/"),
        ("HOSPITALISATIONS", "s3a://bronze/hospitalisations/"),
        ("SATISFACTION_MCO_2017", "s3a://bronze/satisfaction_mco_2017/")
    ]
    
    # Afficher chaque table
    for table_name, s3_path in tables:
        display_table_sample(spark, table_name, s3_path)
    
    # Résumé final
    print_header("✅ RÉSUMÉ")
    print(f"  ✓ {len(tables)} tables affichées")
    print(f"  ✓ Durée: {(datetime.now() - start_time).total_seconds():.1f}s")
    print(f"\n{'='*120}\n")
    
    spark.stop()

if __name__ == "__main__":
    main()
