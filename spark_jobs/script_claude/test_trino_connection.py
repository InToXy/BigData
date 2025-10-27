#!/usr/bin/env python3
"""
Script de test rapide pour vérifier la connexion Trino
"""

import subprocess
import sys

print("=" * 60)
print("  🔍 TEST DE CONNEXION TRINO")
print("=" * 60)
print()

# Test 1: Vérifier que Trino est démarré
print("1. Vérification que Trino est démarré...")
result = subprocess.run(
    ["docker", "logs", "chu_trino"],
    capture_output=True,
    text=True
)

if "SERVER STARTED" in result.stdout:
    print("   ✅ Trino est démarré")
else:
    print("   ❌ Trino n'est pas démarré")
    print("   Exécutez: docker restart chu_trino")
    sys.exit(1)

# Test 2: Lister les catalogues
print("\n2. Vérification des catalogues...")
result = subprocess.run(
    ["docker", "exec", "chu_trino", "trino", "--execute", "SHOW CATALOGS;"],
    capture_output=True,
    text=True
)

if "parquet" in result.stdout:
    print("   ✅ Catalogue 'parquet' trouvé")
else:
    print("   ❌ Catalogue 'parquet' non trouvé")
    sys.exit(1)

# Test 3: Vérifier le schéma gold
print("\n3. Vérification du schéma gold...")
result = subprocess.run(
    ["docker", "exec", "chu_trino", "trino", "--execute", "SHOW SCHEMAS FROM parquet;"],
    capture_output=True,
    text=True
)

if "gold" in result.stdout:
    print("   ✅ Schéma 'gold' existe")
else:
    print("   ❌ Schéma 'gold' n'existe pas")
    print("   Créez-le avec: docker exec chu_trino trino --execute \"CREATE SCHEMA parquet.gold;\"")
    sys.exit(1)

# Test 4: Lister les tables
print("\n4. Vérification des tables existantes...")
result = subprocess.run(
    ["docker", "exec", "chu_trino", "trino", "--execute", "SHOW TABLES FROM parquet.gold;"],
    capture_output=True,
    text=True
)

tables = [line.strip().replace('"', '') for line in result.stdout.split('\n') if line.strip() and not line.startswith('Oct') and 'WARNING' not in line]

if tables:
    print(f"   ✅ {len(tables)} table(s) trouvée(s):")
    for table in tables:
        print(f"      - {table}")
else:
    print("   ⚠️  Aucune table trouvée (c'est normal si vous n'avez pas encore exécuté le script)")

# Test 5: Vérifier MinIO
print("\n5. Vérification de la connexion MinIO...")
result = subprocess.run(
    ["docker", "exec", "chu_minio", "ls", "/data/gold/"],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    folders = [f.strip() for f in result.stdout.split('\n') if f.strip()]
    parquet_folders = [f for f in folders if f not in ['metastore', 'delta', '_SUCCESS']]
    print(f"   ✅ MinIO accessible - {len(parquet_folders)} tables disponibles dans gold/")
else:
    print("   ❌ Impossible d'accéder à MinIO")
    sys.exit(1)

print()
print("=" * 60)
print("  ✅ TOUS LES TESTS SONT PASSÉS!")
print("=" * 60)
print()
print("Vous pouvez maintenant exécuter:")
print("  python3 create_all_trino_tables.py")
print()
