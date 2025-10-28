#!/usr/bin/env python3
"""
Vérification rapide du contenu du bucket Bronze
"""
import sys
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName("VerifyBronze") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = get_spark()
    sc = spark.sparkContext
    
    print("\n" + "="*70)
    print("🔍 VÉRIFICATION DU BUCKET BRONZE")
    print("="*70)
    
    try:
        # Utiliser l'API Hadoop FileSystem
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI("s3a://bronze/"),
            hadoop_conf
        )
        
        path = sc._jvm.org.apache.hadoop.fs.Path("s3a://bronze/")
        
        if fs.exists(path):
            file_statuses = fs.listStatus(path)
            
            if file_statuses:
                print(f"\n✅ {len(file_statuses)} tables trouvées dans Bronze:\n")
                
                for status in file_statuses:
                    table_name = status.getPath().getName()
                    # Compter les fichiers Parquet
                    table_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://bronze/{table_name}/")
                    if fs.isDirectory(table_path):
                        files = fs.listStatus(table_path)
                        parquet_files = [f for f in files if f.getPath().getName().endswith('.parquet')]
                        
                        # Lire avec Spark pour compter les lignes
                        try:
                            df = spark.read.parquet(f"s3a://bronze/{table_name}")
                            row_count = df.count()
                            col_count = len(df.columns)
                            print(f"   📊 {table_name:40s} → {row_count:>10,} lignes, {col_count:>3} colonnes")
                        except:
                            print(f"   📁 {table_name:40s} → {len(parquet_files)} fichiers")
                
                print("\n" + "="*70)
                print("✅ Zone Bronze opérationnelle!")
                print("="*70)
            else:
                print("\n⚠️  Le bucket Bronze est vide")
        else:
            print("\n❌ Le bucket Bronze n'existe pas encore")
            
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        sys.exit(1)
    
    spark.stop()

if __name__ == "__main__":
    main()
