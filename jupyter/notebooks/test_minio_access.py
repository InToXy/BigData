#!/usr/bin/env python3
"""
Script de test pour vérifier l'accès à MinIO (bucket bronze)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Configuration MinIO
MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bucket": "bronze"
}

def test_minio_access():
    """Test l'accès à MinIO"""
    
    # Initialiser Spark avec configuration S3A
    spark = SparkSession.builder \
        .appName("Test MinIO Access") \
        .config("spark.jars", "/home/jovyan/jars/hadoop-aws-3.3.4.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Configurer Hadoop
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
    hadoop_conf.set("fs.s3a.access.key", MINIO_CONFIG["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    
    print("✅ Spark + S3A configuré\n")
    
    # Test 1: Écrire un petit fichier dans MinIO
    print("🔍 Test 1: Écriture dans MinIO (bucket bronze)")
    try:
        test_df = spark.createDataFrame([
            (1, "test1", "value1"),
            (2, "test2", "value2"),
            (3, "test3", "value3")
        ], ["id", "name", "value"])
        
        test_path = f"s3a://{MINIO_CONFIG['bucket']}/test_connection/data"
        test_df.write.mode("overwrite").parquet(test_path)
        
        print(f"   ✅ Écriture réussie vers: {test_path}\n")
        
    except Exception as e:
        print(f"   ❌ Erreur d'écriture: {str(e)}\n")
        spark.stop()
        return
    
    # Test 2: Lire le fichier depuis MinIO
    print("🔍 Test 2: Lecture depuis MinIO")
    try:
        read_df = spark.read.parquet(test_path)
        count = read_df.count()
        
        print(f"   ✅ Lecture réussie: {count} lignes")
        read_df.show()
        print()
        
    except Exception as e:
        print(f"   ❌ Erreur de lecture: {str(e)}\n")
    
    spark.stop()
    print("✅ Test terminé - MinIO est accessible !")

if __name__ == "__main__":
    test_minio_access()
