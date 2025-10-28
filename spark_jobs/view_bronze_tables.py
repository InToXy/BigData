"""
Script pour afficher les 10 premières lignes de chaque table Bronze
sous forme de tableaux formatés
"""

from pyspark.sql import SparkSession

def get_spark_session():
    """Crée une session Spark avec configuration S3A"""
    spark = SparkSession.builder \
        .appName("View_Bronze_Tables") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def display_table(spark, table_name, path):
    """Affiche les 10 premières lignes d'une table sous forme de tableau"""
    print(f"\n{'='*100}")
    print(f"📊 TABLE: {table_name.upper()}")
    print(f"{'='*100}")
    
    try:
        df = spark.read.parquet(path)
        total_rows = df.count()
        print(f"Total lignes: {total_rows:,}")
        print(f"\nSchéma ({len(df.columns)} colonnes):")
        
        # Afficher le schéma de manière compacte
        for field in df.schema.fields[:10]:  # Limiter à 10 premières colonnes pour lisibilité
            print(f"  - {field.name}: {field.dataType.simpleString()}")
        if len(df.columns) > 10:
            print(f"  ... et {len(df.columns) - 10} autres colonnes")
        
        print(f"\n10 premières lignes:")
        print("-" * 100)
        
        # Afficher les 10 premières lignes sous forme de tableau
        df.show(10, truncate=50, vertical=False)
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture de {table_name}: {str(e)}")

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    VISUALISATION TABLES BRONZE                               ║
║                         (10 premières lignes)                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    spark = get_spark_session()
    
    # Liste des tables à afficher
    tables = [
        ("deces", "s3a://bronze/deces/"),
        ("etablissements", "s3a://bronze/etablissements/"),
        ("professionnels_sante", "s3a://bronze/professionnels_sante/"),
        ("hospitalisations", "s3a://bronze/hospitalisations/"),
        ("satisfaction_mco_2017", "s3a://bronze/satisfaction_mco_2017/")
    ]
    
    for table_name, path in tables:
        display_table(spark, table_name, path)
    
    print(f"\n{'='*100}")
    print("✅ Visualisation terminée")
    print(f"{'='*100}\n")
    
    spark.stop()

if __name__ == "__main__":
    main()
