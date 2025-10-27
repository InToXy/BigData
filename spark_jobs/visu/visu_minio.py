# visu_minio.py

from minio import Minio
from minio.error import S3Error
from collections import defaultdict
from datetime import datetime

# === Configuration MinIO ===
MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
}

# Buckets utiles (zones de données uniquement)
DATA_BUCKETS = {"bronze", "silver", "gold"}

def get_minio_client():
    """Initialise la connexion au serveur MinIO."""
    try:
        client = Minio(
            MINIO_CONFIG["endpoint"],
            access_key=MINIO_CONFIG["access_key"],
            secret_key=MINIO_CONFIG["secret_key"],
            secure=False
        )
        print("✅ Connexion à MinIO réussie")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MinIO : {e}")
        raise

def list_data_buckets(client):
    """Liste uniquement les buckets de données (bronze, silver, gold)."""
    all_buckets = client.list_buckets()
    ordered = ["bronze", "silver", "gold"]
    existing = {b.name for b in all_buckets}
    data_buckets = [b for b in ordered if b in existing]

    
    print("\n📦 BUCKETS DE DONNÉES DISPONIBLES :")
    for b in data_buckets:
        print(f"  - {b}")
    return data_buckets

def list_parquet_files(client, bucket_name, max_files=10):
    """Liste les fichiers Parquet dans un bucket donné."""
    print(f"\n🗂️  CONTENU DU BUCKET : {bucket_name}")
    objects = client.list_objects(bucket_name, recursive=True)
    
    parquet_files = []
    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            size_mb = obj.size / (1024 * 1024)
            parquet_files.append((obj.object_name, size_mb))
    
    total_size = sum([s for _, s in parquet_files])
    print(f"📄 {len(parquet_files)} fichiers Parquet - Total : {total_size:.2f} MB")
    
    for path, size in parquet_files[:max_files]:
        print(f"   📄 {path} ({size:.2f} MB)")
    if len(parquet_files) > max_files:
        print(f"   ... et {len(parquet_files) - max_files} fichiers supplémentaires")
    
    return parquet_files

def analyze_partition_structure(parquet_files):
    """Analyse les partitions de type colonne=valeur dans les chemins."""
    print("\n🔍 STRUCTURE DE PARTITIONNEMENT DÉTECTÉE :")
    partitions = defaultdict(set)
    
    for path, _ in parquet_files:
        parts = path.split("/")
        for part in parts:
            if "=" in part:
                key, val = part.split("=", 1)
                partitions[key].add(val)
    
    if not partitions:
        print("⚠️  Aucun partitionnement détecté.")
        return
    
    for key, values in partitions.items():
        print(f"🧩 Partition `{key}` : {len(values)} valeur(s)")
        print(f"   ➜ Exemple(s) : {sorted(list(values))[:5]}")

def generate_minio_report(client, buckets):
    """Affiche un rapport global des buckets de données (sans sauvegarde)."""
    print("\n════════════════════════════════════════════════════")
    print("📊 RAPPORT MINIO - DONNÉES BRONZE / SILVER / GOLD")
    print("════════════════════════════════════════════════════\n")
    print(f"🕒 Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    for bucket in buckets:
        print(f"📦 Bucket : {bucket}")
        parquet_files = list_parquet_files(client, bucket, max_files=5)
        total_size = sum([s for _, s in parquet_files])
        print(f"   ➤ Nombre de fichiers Parquet : {len(parquet_files)}")
        print(f"   ➤ Taille totale estimée      : {total_size:.2f} MB")
        
        # Analyse du partitionnement
        partitions = defaultdict(set)
        for path, _ in parquet_files:
            for part in path.split("/"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    partitions[k].add(v)
        if partitions:
            print("   ➤ Partitions détectées :")
            for k, v in partitions.items():
                ex = ', '.join(sorted(list(v))[:3])
                print(f"     - {k} : {len(v)} valeurs (ex : {ex})")
        else:
            print("   ➤ Aucun partitionnement détecté.")
        
        print("-" * 50)

def interactive_minio_browser():
    """Interface CLI interactive pour explorer les données MinIO."""
    print("""
╔════════════════════════════════════════════════════╗
║           VISUALISATEUR MINIO - EXPLORATEUR        ║
╚════════════════════════════════════════════════════╝
""")
    try:
        client = get_minio_client()
        
        while True:
            buckets = list_data_buckets(client)
            print("\n🎯 Menu MinIO:")
            print("1. 📂 Explorer un bucket")
            print("2. 📊 Générer un rapport complet (terminal uniquement)")
            print("3. 🚪 Quitter")
            
            choice = input("\nVotre choix (1-3): ").strip()
            
            if choice == "1":
                for i, b in enumerate(buckets, 1):
                    print(f"  {i}. {b}")
                selected = input("Numéro du bucket à explorer: ").strip()
                if selected.isdigit() and 1 <= int(selected) <= len(buckets):
                    bucket_name = buckets[int(selected)-1]
                    parquet_files = list_parquet_files(client, bucket_name)
                    analyze_partition_structure(parquet_files)
            elif choice == "2":
                generate_minio_report(client, buckets)
            elif choice == "3":
                print("👋 Au revoir !")
                break
            else:
                print("❌ Choix invalide.")
    
    except S3Error as e:
        print(f"❌ Erreur MinIO : {e}")

if __name__ == "__main__":
    interactive_minio_browser()
