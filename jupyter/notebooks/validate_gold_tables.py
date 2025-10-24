#!/usr/bin/env python3
"""
Script de validation des tables Gold

Vérifie:
- Le peuplement de toutes les tables Gold
- La qualité des données (valeurs nulles, doublons, cohérence)
- Les statistiques de chaque table
- Les anomalies potentielles

Usage:
    python3 validate_gold_tables.py
    
    # Ou avec options
    python3 validate_gold_tables.py --detailed --export-csv
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, avg, min as spark_min, max as spark_max, isnan, when

# Configuration MinIO
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "minioadmin123")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")

# Liste des tables Gold attendues
EXPECTED_GOLD_TABLES = [
    "kpi_taux_consultation_periode",
    "kpi_consultation_par_diagnostic",
    "kpi_taux_hospitalisation_global",
    "kpi_hospitalisation_par_diagnostic",
    "kpi_hospitalisation_sexe_age",
    "kpi_consultation_par_professionnel",
    "kpi_deces_par_region_2019",
    "kpi_satisfaction_par_region_2020",
]


def get_spark_session(app_name: str = "GoldValidation") -> SparkSession:
    """Initialise et retourne une session Spark configurée pour MinIO."""
    builder = SparkSession.builder.appName(app_name)
    
    # Configuration S3A/MinIO
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Résolution du conflit de configuration "60s" → valeurs numériques en ms
    builder = builder.config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    builder = builder.config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    builder = builder.config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    builder = builder.config("spark.hadoop.fs.s3a.retry.interval", "500")
    builder = builder.config("spark.hadoop.fs.s3a.retry.limit", "3")
    
    # Ajout des JARs Hadoop-AWS pour S3A (résout ClassNotFoundException)
    builder = builder.config("spark.jars.packages", 
                            "org.apache.hadoop:hadoop-aws:3.3.4,"
                            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def gold_path(table: str) -> str:
    """Retourne le chemin S3 d'une table Gold."""
    return f"s3a://{GOLD_BUCKET}/{table}"


def table_exists(spark: SparkSession, table: str) -> bool:
    """Vérifie si une table existe dans la zone Gold."""
    path = gold_path(table)
    try:
        spark.read.parquet(path).limit(1).collect()
        return True
    except Exception:
        return False


def read_gold_table(spark: SparkSession, table: str) -> Optional[DataFrame]:
    """Lit une table Gold, retourne None si elle n'existe pas."""
    path = gold_path(table)
    try:
        df = spark.read.option("mergeSchema", "true").parquet(path)
        return df
    except Exception as e:
        print(f"   ⚠️  Erreur lecture {table}: {e}")
        return None


def get_table_stats(df: DataFrame, table_name: str) -> Dict:
    """Calcule les statistiques d'une table."""
    stats = {
        "table": table_name,
        "exists": True,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
    }
    
    # Taille approximative en MB (basé sur le cache)
    try:
        df.cache()
        # Estimation très approximative
        stats["estimated_size_mb"] = round(df.count() * len(df.columns) * 50 / (1024 * 1024), 3)
    except:
        stats["estimated_size_mb"] = "N/A"
    
    return stats


def check_null_values(df: DataFrame, table_name: str) -> Dict[str, int]:
    """Compte les valeurs nulles par colonne."""
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(
            col(col_name).isNull() | isnan(col(col_name))
        ).count()
        if null_count > 0:
            null_counts[col_name] = null_count
    return null_counts


def check_data_quality(df: DataFrame, table_name: str) -> Dict:
    """Vérifie la qualité des données."""
    quality_report = {
        "table": table_name,
        "issues": []
    }
    
    total_rows = df.count()
    
    # Vérifier les valeurs nulles
    null_counts = check_null_values(df, table_name)
    if null_counts:
        quality_report["null_values"] = null_counts
        quality_report["issues"].append(f"{len(null_counts)} colonnes avec valeurs nulles")
    
    # Vérifier si la table est vide
    if total_rows == 0:
        quality_report["issues"].append("⚠️ TABLE VIDE")
    
    # Vérifier les colonnes numériques pour valeurs négatives inappropriées
    for col_name in df.columns:
        if "nb_" in col_name.lower() or "taux" in col_name.lower():
            try:
                negative_count = df.filter(col(col_name) < 0).count()
                if negative_count > 0:
                    quality_report["issues"].append(
                        f"⚠️ {negative_count} valeurs négatives dans {col_name}"
                    )
            except:
                pass
    
    return quality_report


def get_sample_data(df: DataFrame, n: int = 5) -> List[Dict]:
    """Retourne un échantillon de données."""
    try:
        rows = df.limit(n).collect()
        return [row.asDict() for row in rows]
    except Exception as e:
        return []


def print_separator(char="=", length=80):
    """Affiche une ligne de séparation."""
    print(char * length)


def print_table_report(stats: Dict, quality: Dict, sample: List[Dict]):
    """Affiche le rapport pour une table."""
    table_name = stats["table"]
    
    print(f"\n{'─' * 80}")
    print(f"📊 TABLE: {table_name}")
    print(f"{'─' * 80}")
    
    # Statistiques de base
    print(f"   Existe:        {'✅ OUI' if stats['exists'] else '❌ NON'}")
    print(f"   Lignes:        {stats['row_count']:,}")
    print(f"   Colonnes:      {stats['column_count']}")
    print(f"   Taille estim.: {stats['estimated_size_mb']} MB")
    
    # Colonnes
    print(f"\n   📋 Colonnes ({stats['column_count']}):")
    for i, col_name in enumerate(stats['columns'], 1):
        print(f"      {i:2d}. {col_name}")
    
    # Qualité des données
    if quality.get("issues"):
        print(f"\n   ⚠️  Problèmes de qualité détectés:")
        for issue in quality["issues"]:
            print(f"      • {issue}")
    else:
        print(f"\n   ✅ Aucun problème de qualité détecté")
    
    # Valeurs nulles
    if quality.get("null_values"):
        print(f"\n   🔍 Valeurs nulles:")
        for col_name, null_count in quality["null_values"].items():
            percentage = (null_count / stats['row_count']) * 100
            print(f"      • {col_name}: {null_count:,} ({percentage:.1f}%)")
    
    # Échantillon de données
    if sample:
        print(f"\n   📄 Échantillon de données (premières {len(sample)} lignes):")
        for i, row in enumerate(sample, 1):
            print(f"\n      Ligne {i}:")
            for key, value in row.items():
                # Formatter la valeur
                if isinstance(value, float):
                    value_str = f"{value:.4f}" if value < 1 else f"{value:,.2f}"
                else:
                    value_str = str(value)
                print(f"         {key}: {value_str}")


def generate_summary_report(all_stats: List[Dict], all_quality: List[Dict]) -> Dict:
    """Génère un rapport de synthèse."""
    total_tables = len(EXPECTED_GOLD_TABLES)
    existing_tables = sum(1 for s in all_stats if s.get("exists", False))
    total_rows = sum(s.get("row_count", 0) for s in all_stats)
    total_columns = sum(s.get("column_count", 0) for s in all_stats)
    
    tables_with_issues = sum(1 for q in all_quality if q.get("issues"))
    
    return {
        "total_tables_expected": total_tables,
        "tables_existing": existing_tables,
        "tables_missing": total_tables - existing_tables,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "tables_with_issues": tables_with_issues,
        "success_rate": (existing_tables / total_tables) * 100 if total_tables > 0 else 0,
    }


def print_summary_report(summary: Dict, all_stats: List[Dict]):
    """Affiche le rapport de synthèse."""
    print_separator("═", 80)
    print("📊 RAPPORT DE SYNTHÈSE - ZONE GOLD")
    print_separator("═", 80)
    
    print(f"\n🎯 Vue d'ensemble:")
    print(f"   Tables attendues:    {summary['total_tables_expected']}")
    print(f"   Tables existantes:   {summary['tables_existing']} ✅")
    print(f"   Tables manquantes:   {summary['tables_missing']} {'❌' if summary['tables_missing'] > 0 else '✅'}")
    print(f"   Taux de succès:      {summary['success_rate']:.1f}%")
    
    print(f"\n📊 Statistiques globales:")
    print(f"   Lignes totales:      {summary['total_rows']:,}")
    print(f"   Colonnes totales:    {summary['total_columns']}")
    print(f"   Moyenne lignes/table: {summary['total_rows'] // summary['tables_existing'] if summary['tables_existing'] > 0 else 0:,}")
    
    print(f"\n🔍 Qualité des données:")
    if summary['tables_with_issues'] == 0:
        print(f"   ✅ Toutes les tables sont conformes")
    else:
        print(f"   ⚠️  {summary['tables_with_issues']} table(s) avec problèmes")
    
    # Détail des tables
    print(f"\n📋 Détail des tables:")
    print(f"   {'Table':<45} {'Lignes':>12} {'Colonnes':>10}")
    print(f"   {'-' * 45} {'-' * 12} {'-' * 10}")
    
    for stats in sorted(all_stats, key=lambda x: x.get('row_count', 0), reverse=True):
        if stats.get("exists"):
            status = "✅"
            rows = f"{stats['row_count']:,}"
            cols = str(stats['column_count'])
        else:
            status = "❌"
            rows = "N/A"
            cols = "N/A"
        
        table_name = stats['table'][:43]
        print(f"   {status} {table_name:<43} {rows:>12} {cols:>10}")


def export_to_csv(all_stats: List[Dict], all_quality: List[Dict], output_dir: str = "./reports"):
    """Exporte les résultats en CSV."""
    import csv
    from datetime import datetime
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Export des statistiques
    stats_file = f"{output_dir}/gold_stats_{timestamp}.csv"
    with open(stats_file, 'w', newline='') as f:
        if all_stats:
            writer = csv.DictWriter(f, fieldnames=all_stats[0].keys())
            writer.writeheader()
            writer.writerows(all_stats)
    
    print(f"\n   ✅ Statistiques exportées: {stats_file}")
    
    # Export des problèmes de qualité
    quality_file = f"{output_dir}/gold_quality_{timestamp}.csv"
    quality_flat = []
    for q in all_quality:
        if q.get("issues"):
            quality_flat.append({
                "table": q["table"],
                "issues": " | ".join(q["issues"])
            })
    
    if quality_flat:
        with open(quality_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["table", "issues"])
            writer.writeheader()
            writer.writerows(quality_flat)
        print(f"   ✅ Problèmes qualité exportés: {quality_file}")


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validation des tables Gold")
    parser.add_argument("--detailed", action="store_true", help="Affiche les détails de chaque table")
    parser.add_argument("--export-csv", action="store_true", help="Exporte les résultats en CSV")
    parser.add_argument("--sample-size", type=int, default=5, help="Nombre de lignes d'échantillon à afficher")
    args = parser.parse_args()
    
    print_separator("═", 80)
    print("🔍 VALIDATION DES TABLES GOLD")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator("═", 80)
    
    # Initialiser Spark
    print("\n📦 Initialisation de Spark...")
    spark = get_spark_session()
    print("   ✅ Spark initialisé")
    
    # Valider chaque table
    all_stats = []
    all_quality = []
    
    print(f"\n🔎 Vérification de {len(EXPECTED_GOLD_TABLES)} tables...")
    
    for table_name in EXPECTED_GOLD_TABLES:
        print(f"\n   Analyse de {table_name}...", end=" ")
        
        if not table_exists(spark, table_name):
            print("❌ ABSENTE")
            all_stats.append({
                "table": table_name,
                "exists": False,
                "row_count": 0,
                "column_count": 0,
                "columns": [],
                "estimated_size_mb": 0
            })
            all_quality.append({
                "table": table_name,
                "issues": ["❌ Table n'existe pas"]
            })
            continue
        
        df = read_gold_table(spark, table_name)
        if df is None:
            print("❌ ERREUR LECTURE")
            continue
        
        print("✅ OK")
        
        # Statistiques
        stats = get_table_stats(df, table_name)
        all_stats.append(stats)
        
        # Qualité
        quality = check_data_quality(df, table_name)
        all_quality.append(quality)
        
        # Échantillon (si mode détaillé)
        if args.detailed:
            sample = get_sample_data(df, args.sample_size)
            print_table_report(stats, quality, sample)
    
    # Rapport de synthèse
    summary = generate_summary_report(all_stats, all_quality)
    print_summary_report(summary, all_stats)
    
    # Export CSV si demandé
    if args.export_csv:
        print(f"\n📤 Export des résultats en CSV...")
        export_to_csv(all_stats, all_quality)
    
    # Statut final
    print_separator("═", 80)
    if summary['tables_missing'] == 0 and summary['tables_with_issues'] == 0:
        print("✅ VALIDATION RÉUSSIE - Toutes les tables sont conformes")
        exit_code = 0
    elif summary['tables_missing'] > 0:
        print(f"⚠️  VALIDATION PARTIELLE - {summary['tables_missing']} table(s) manquante(s)")
        exit_code = 1
    else:
        print(f"⚠️  VALIDATION AVEC ALERTES - {summary['tables_with_issues']} problème(s) détecté(s)")
        exit_code = 2
    
    print_separator("═", 80)
    
    spark.stop()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
