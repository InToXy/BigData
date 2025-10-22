from pyspark.sql import SparkSession

# Configuration PostgreSQL
POSTGRES_CONFIG = {
    "host": "bigdata_postgres",
    "port": "5432",
    "database": "healthcare_data",
    "user": "admin",
    "password": "admin123"
}

# URL JDBC
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Test PostgreSQL Connection") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.2.27.jre7.jar") \
    .getOrCreate()

try:
    # Test simple query
    df = spark.read.format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", '"public"."Patient"') \
        .option("user", POSTGRES_CONFIG["user"]) \
        .option("password", POSTGRES_CONFIG["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print("\n=== Test de connexion PostgreSQL ===")
    print("Aperçu de la table Patient:")
    df.show(5)

except Exception as e:
    print(f"Erreur: {str(e)}")

finally:
    spark.stop()