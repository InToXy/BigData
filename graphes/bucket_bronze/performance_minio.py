#!/usr/bin/env python3
"""
Script de mesure des performances de lecture depuis MinIO (bronze layer)
VERSION AVANCÉE - Analyse complète avec graphiques de performance multiples
- Graphique en courbes (évolution temporelle)
- Histogramme (distribution des temps)
- Boxplot (dispersion et outliers)
- Scatter plot (corrélation volume/temps)
- Heatmap (latence par type de requête)
"""
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow.parquet as pq
import pyarrow.fs as fs
import boto3
from botocore.client import Config
import numpy as np
from datetime import datetime

print("🚀 Démarrage de l'analyse de performance MinIO...")
print("📊 Génération de graphiques avancés de performance\n")

# Configuration MinIO
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "bronze"

# Créer un client S3 (MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

print("✅ Connexion MinIO établie")

# Découverte automatique des datasets dans le bucket bronze
print(f"\n🔍 Découverte des datasets dans le bucket '{BUCKET}'...")
try:
    response = s3_client.list_objects_v2(Bucket=BUCKET, Delimiter='/')
    
    datasets = []
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            dataset_name = prefix['Prefix'].rstrip('/')
            datasets.append(dataset_name)
    
    datasets.sort()  # Tri alphabétique
    
    if not datasets:
        print(f"❌ Aucun dataset trouvé dans le bucket '{BUCKET}'")
        print(f"💡 Vérifiez que les données ont été ingérées dans MinIO")
        exit(1)
    
    print(f"✅ {len(datasets)} dataset(s) détecté(s) :")
    for ds in datasets:
        print(f"   • {ds}")
    
except Exception as e:
    print(f"❌ Erreur lors de la découverte des datasets: {e}")
    print(f"💡 Vérifiez que MinIO est accessible et que le bucket '{BUCKET}' existe")
    exit(1)

print(f"\n📊 Test de lecture sur {len(datasets)} datasets depuis MinIO...\n")
print(f"{'Dataset':<35} {'Lignes':>12} {'Temps':>10} {'Débit':>15}")
print("="*75)

performance_data = []
detailed_measurements = []  # Pour stocker les mesures détaillées

# Créer filesystem S3
s3 = fs.S3FileSystem(
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    endpoint_override=MINIO_ENDPOINT.replace('http://', ''),
    scheme='http'
)

# Effectuer 3 passes de lecture pour simuler requêtes chaudes/froides
query_types = ['Première lecture (froide)', 'Deuxième lecture (tiède)', 'Troisième lecture (chaude)']
iteration = 0

for pass_num, query_type in enumerate(query_types, 1):
    print(f"\n🔄 {query_type}...")
    for ds in datasets:
        dataset_path = f"{BUCKET}/{ds}"
        start_time = time.time()
        try:
            # Lire le dataset Parquet
            table = pq.read_table(dataset_path, filesystem=s3)
            count = len(table)
            elapsed = time.time() - start_time
            throughput = count / elapsed if elapsed > 0 else 0
            
            # Calculer la taille en MB
            size_mb = sum(table.column(i).nbytes for i in range(table.num_columns)) / (1024 * 1024)
            
            if pass_num == 1:  # Première passe seulement
                print(f"{ds:<35} {count:>12,} {elapsed:>9.2f}s {throughput:>13,.0f} r/s")
                performance_data.append({
                    "dataset": ds, 
                    "rows": count, 
                    "time": elapsed,
                    "size_mb": size_mb
                })
            
            # Enregistrer toutes les mesures pour analyse détaillée
            detailed_measurements.append({
                "iteration": iteration,
                "timestamp": datetime.now(),
                "dataset": ds,
                "query_type": query_type,
                "pass_num": pass_num,
                "rows": count,
                "time": elapsed,
                "throughput": throughput,
                "size_mb": size_mb,
                "mb_per_sec": size_mb / elapsed if elapsed > 0 else 0
            })
            iteration += 1
            
        except Exception as e:
            if pass_num == 1:
                print(f"{ds:<35} {'Non trouvé':>12}")
            continue

# Créer DataFrames pandas
performance_df = pd.DataFrame(performance_data)
detailed_df = pd.DataFrame(detailed_measurements)

if performance_df.empty:
    print("\n⚠️  Aucun dataset n'a pu être lu. Vérifiez que MinIO est accessible et contient des données.")
    exit(1)

print(f"\n" + "="*75)
print(f"📈 Génération des graphiques avancés de performance...")
print(f"="*75)

# Configuration du style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

# ============================================================================
# GRAPHIQUE 1: Temps de réponse par dataset (Barres)
# ============================================================================
print("\n1️⃣  Graphique en barres - Temps de réponse par dataset...")
fig, ax = plt.subplots(figsize=(16, 8))
bars = ax.bar(performance_df['dataset'], performance_df['time'], 
              color='steelblue', edgecolor='navy', linewidth=1.2, alpha=0.8)

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.2f}s',
            ha='center', va='bottom', fontsize=9, fontweight='bold')

ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
ax.set_ylabel('Temps de réponse (secondes)', fontsize=13, fontweight='bold')
ax.set_title('Temps de réponse des requêtes - Bronze Layer MinIO', 
             fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/1_temps_reponse_barres.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 1_temps_reponse_barres.png")
plt.close()

# ============================================================================
# GRAPHIQUE 2: Évolution temporelle (Line Chart) - Requêtes Chaudes/Froides
# ============================================================================
print("2️⃣  Graphique en courbes - Évolution des temps de réponse (chaude/froide)...")
fig, ax = plt.subplots(figsize=(16, 8))

# Grouper par dataset et type de requête
for query_type in query_types:
    subset = detailed_df[detailed_df['query_type'] == query_type]
    # Calculer la moyenne par dataset
    grouped = subset.groupby('dataset')['time'].mean().reset_index()
    ax.plot(grouped['dataset'], grouped['time'], 
            marker='o', linewidth=2.5, markersize=8, label=query_type, alpha=0.8)

ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
ax.set_ylabel('Temps de réponse moyen (secondes)', fontsize=13, fontweight='bold')
ax.set_title('Évolution des temps de réponse - Requêtes Chaudes vs Froides', 
             fontsize=16, fontweight='bold', pad=20)
ax.legend(fontsize=11, loc='upper left', framealpha=0.9)
plt.xticks(rotation=45, ha='right')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/2_evolution_temporelle_courbes.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 2_evolution_temporelle_courbes.png")
plt.close()

# ============================================================================
# GRAPHIQUE 3: Distribution des temps (Histogramme)
# ============================================================================
print("3️⃣  Histogramme - Distribution des temps de réponse...")
fig, ax = plt.subplots(figsize=(14, 8))

ax.hist(detailed_df['time'], bins=30, color='coral', edgecolor='darkred', 
        alpha=0.7, linewidth=1.2)

ax.set_xlabel('Temps de réponse (secondes)', fontsize=13, fontweight='bold')
ax.set_ylabel('Fréquence', fontsize=13, fontweight='bold')
ax.set_title('Distribution des temps de réponse - Toutes requêtes', 
             fontsize=16, fontweight='bold', pad=20)
ax.axvline(detailed_df['time'].mean(), color='red', linestyle='--', 
           linewidth=2, label=f'Moyenne: {detailed_df["time"].mean():.2f}s')
ax.axvline(detailed_df['time'].median(), color='green', linestyle='--', 
           linewidth=2, label=f'Médiane: {detailed_df["time"].median():.2f}s')
ax.legend(fontsize=11)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/3_distribution_histogramme.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 3_distribution_histogramme.png")
plt.close()

# ============================================================================
# GRAPHIQUE 4: Boxplot - Dispersion et outliers
# ============================================================================
print("4️⃣  Boxplot - Analyse de dispersion par type de requête...")
fig, ax = plt.subplots(figsize=(14, 8))

# Boxplot par type de requête
sns.boxplot(data=detailed_df, x='query_type', y='time', 
            palette='Set2', linewidth=2, ax=ax)

ax.set_xlabel('Type de requête', fontsize=13, fontweight='bold')
ax.set_ylabel('Temps de réponse (secondes)', fontsize=13, fontweight='bold')
ax.set_title('Dispersion des temps de réponse - Boxplot par type de requête', 
             fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=15, ha='right')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/4_dispersion_boxplot.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 4_dispersion_boxplot.png")
plt.close()

# Boxplot par dataset (top 8)
fig, ax = plt.subplots(figsize=(16, 8))
top_datasets = performance_df.nlargest(8, 'rows')['dataset'].tolist()
subset_data = detailed_df[detailed_df['dataset'].isin(top_datasets)]

sns.boxplot(data=subset_data, x='dataset', y='time', 
            palette='viridis', linewidth=1.5, ax=ax)

ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
ax.set_ylabel('Temps de réponse (secondes)', fontsize=13, fontweight='bold')
ax.set_title('Dispersion des temps de réponse par Dataset (Top 8)', 
             fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/4b_dispersion_boxplot_datasets.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 4b_dispersion_boxplot_datasets.png")
plt.close()

# ============================================================================
# GRAPHIQUE 5: Scatter Plot - Corrélation volume/temps
# ============================================================================
print("5️⃣  Scatter plot - Corrélation volume de données vs temps de réponse...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))

# Subplot 1: Nombre de lignes vs temps
scatter1 = ax1.scatter(performance_df['rows'], performance_df['time'], 
                       s=200, c=performance_df['size_mb'], cmap='coolwarm', 
                       alpha=0.7, edgecolors='black', linewidth=1.5)
for idx, row in performance_df.iterrows():
    ax1.annotate(row['dataset'], 
                (row['rows'], row['time']), 
                fontsize=8, alpha=0.7, 
                xytext=(5, 5), textcoords='offset points')

ax1.set_xlabel('Nombre de lignes', fontsize=12, fontweight='bold')
ax1.set_ylabel('Temps de réponse (secondes)', fontsize=12, fontweight='bold')
ax1.set_title('Corrélation: Nombre de lignes → Temps', fontsize=13, fontweight='bold')
ax1.grid(True, alpha=0.3)
cbar1 = plt.colorbar(scatter1, ax=ax1)
cbar1.set_label('Taille (MB)', fontsize=10)

# Subplot 2: Taille (MB) vs temps
scatter2 = ax2.scatter(performance_df['size_mb'], performance_df['time'], 
                       s=200, c=performance_df['rows'], cmap='viridis', 
                       alpha=0.7, edgecolors='black', linewidth=1.5)
for idx, row in performance_df.iterrows():
    ax2.annotate(row['dataset'], 
                (row['size_mb'], row['time']), 
                fontsize=8, alpha=0.7,
                xytext=(5, 5), textcoords='offset points')

ax2.set_xlabel('Taille des données (MB)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Temps de réponse (secondes)', fontsize=12, fontweight='bold')
ax2.set_title('Corrélation: Taille des données → Temps', fontsize=13, fontweight='bold')
ax2.grid(True, alpha=0.3)
cbar2 = plt.colorbar(scatter2, ax=ax2)
cbar2.set_label('Nombre de lignes', fontsize=10)

plt.suptitle('Analyse de corrélation - Goulets d\'étranglement', 
             fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/5_correlation_scatter.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 5_correlation_scatter.png")
plt.close()

# ============================================================================
# GRAPHIQUE 6: Heatmap - Latence par dataset et type de requête
# ============================================================================
print("6️⃣  Heatmap - Carte thermique des latences...")
fig, ax = plt.subplots(figsize=(14, 10))

# Créer une matrice pivot
pivot_data = detailed_df.pivot_table(
    values='time', 
    index='dataset', 
    columns='query_type', 
    aggfunc='mean'
)

# Créer la heatmap
sns.heatmap(pivot_data, annot=True, fmt='.2f', cmap='YlOrRd', 
            linewidths=1, linecolor='white', cbar_kws={'label': 'Temps (s)'},
            ax=ax, vmin=0)

ax.set_xlabel('Type de requête', fontsize=13, fontweight='bold')
ax.set_ylabel('Dataset', fontsize=13, fontweight='bold')
ax.set_title('Carte thermique des temps de réponse - Dataset × Type de requête', 
             fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=30, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/6_heatmap_latence.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 6_heatmap_latence.png")
plt.close()

# ============================================================================
# GRAPHIQUE 7: Performance comparée (Débit)
# ============================================================================
print("7️⃣  Graphique de débit - Lignes par seconde...")
performance_df['rows_per_sec'] = performance_df['rows'] / performance_df['time']

fig, ax = plt.subplots(figsize=(16, 8))
bars = ax.bar(performance_df['dataset'], performance_df['rows_per_sec'], 
              color='seagreen', edgecolor='darkgreen', linewidth=1.2, alpha=0.8)

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}',
            ha='center', va='bottom', fontsize=9, fontweight='bold', rotation=90)

ax.set_xlabel('Dataset', fontsize=13, fontweight='bold')
ax.set_ylabel('Débit (lignes/seconde)', fontsize=13, fontweight='bold')
ax.set_title('Performance de lecture - Débit par dataset', 
             fontsize=16, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/7_performance_debit.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 7_performance_debit.png")
plt.close()

# ============================================================================
# GRAPHIQUE 8: Dashboard récapitulatif
# ============================================================================
print("8️⃣  Dashboard récapitulatif complet...")
fig = plt.figure(figsize=(20, 12))
gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

# Subplot 1: Temps par dataset
ax1 = fig.add_subplot(gs[0, :2])
ax1.bar(performance_df['dataset'], performance_df['time'], 
        color='steelblue', alpha=0.7)
ax1.set_title('Temps de réponse par dataset', fontweight='bold')
ax1.set_ylabel('Temps (s)')
ax1.tick_params(axis='x', rotation=45)
ax1.grid(axis='y', alpha=0.3)

# Subplot 2: Distribution
ax2 = fig.add_subplot(gs[0, 2])
ax2.hist(detailed_df['time'], bins=20, color='coral', alpha=0.7, edgecolor='black')
ax2.set_title('Distribution des temps', fontweight='bold')
ax2.set_xlabel('Temps (s)')
ax2.set_ylabel('Fréquence')
ax2.grid(axis='y', alpha=0.3)

# Subplot 3: Boxplot par type
ax3 = fig.add_subplot(gs[1, :])
sns.boxplot(data=detailed_df, x='query_type', y='time', palette='Set2', ax=ax3)
ax3.set_title('Dispersion par type de requête', fontweight='bold')
ax3.set_ylabel('Temps (s)')
ax3.set_xlabel('')
ax3.grid(axis='y', alpha=0.3)

# Subplot 4: Scatter
ax4 = fig.add_subplot(gs[2, 0])
ax4.scatter(performance_df['rows'], performance_df['time'], 
           s=100, c=performance_df['size_mb'], cmap='viridis', alpha=0.7)
ax4.set_title('Corrélation lignes/temps', fontweight='bold')
ax4.set_xlabel('Lignes')
ax4.set_ylabel('Temps (s)')
ax4.grid(True, alpha=0.3)

# Subplot 5: Débit
ax5 = fig.add_subplot(gs[2, 1])
ax5.bar(range(len(performance_df)), performance_df['rows_per_sec'], 
       color='seagreen', alpha=0.7)
ax5.set_title('Débit de lecture', fontweight='bold')
ax5.set_xlabel('Dataset')
ax5.set_ylabel('Lignes/s')
ax5.grid(axis='y', alpha=0.3)

# Subplot 6: Statistiques texte
ax6 = fig.add_subplot(gs[2, 2])
ax6.axis('off')
stats_text = f"""
STATISTIQUES GLOBALES

Datasets: {len(performance_df)}
Total lignes: {performance_df['rows'].sum():,}

Temps de réponse:
  • Moyen: {detailed_df['time'].mean():.2f}s
  • Médian: {detailed_df['time'].median():.2f}s
  • Min: {detailed_df['time'].min():.2f}s
  • Max: {detailed_df['time'].max():.2f}s

Débit moyen:
  {performance_df['rows_per_sec'].mean():,.0f} lignes/s

Cache hit improvement:
  {((detailed_df[detailed_df['pass_num']==1]['time'].mean() - detailed_df[detailed_df['pass_num']==3]['time'].mean()) / detailed_df[detailed_df['pass_num']==1]['time'].mean() * 100):.1f}%
"""
ax6.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
        verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

plt.suptitle('🎯 Dashboard de Performance - Bronze Layer MinIO', 
             fontsize=18, fontweight='bold')
plt.savefig("/home/alban/BigData/BigData/graphes/8_dashboard_complet.png", dpi=150, bbox_inches='tight')
print("   ✅ Sauvegardé: 8_dashboard_complet.png")
plt.close()

print(f"\n" + "="*75)

# Afficher le résumé détaillé
print(f"📋 RÉSUMÉ DES PERFORMANCES")
print(f"="*75)
print(f"\n{'Dataset':<30} {'Lignes':>12} {'Temps':>10} {'Débit':>15} {'Taille':>10}")
print("-"*75)
for _, row in performance_df.iterrows():
    print(f"{row['dataset']:<30} {row['rows']:>12,} {row['time']:>9.2f}s "
          f"{row['rows_per_sec']:>13,.0f} r/s {row['size_mb']:>8.1f} MB")

print(f"="*75)

# Statistiques globales détaillées
total_rows = performance_df['rows'].sum()
total_time = performance_df['time'].sum()
total_size = performance_df['size_mb'].sum()
avg_speed = total_rows / total_time if total_time > 0 else 0

# Analyse des requêtes chaudes vs froides
cold_avg = detailed_df[detailed_df['pass_num'] == 1]['time'].mean()
warm_avg = detailed_df[detailed_df['pass_num'] == 2]['time'].mean()
hot_avg = detailed_df[detailed_df['pass_num'] == 3]['time'].mean()
cache_improvement = ((cold_avg - hot_avg) / cold_avg * 100) if cold_avg > 0 else 0

print(f"\n📊 STATISTIQUES GLOBALES:")
print(f"   • Datasets analysés: {len(performance_df)}")
print(f"   • Total de lignes: {total_rows:,}")
print(f"   • Taille totale: {total_size:.2f} MB")
print(f"   • Temps total: {total_time:.2f}s")
print(f"   • Débit moyen: {avg_speed:,.0f} lignes/seconde")
print(f"   • Débit en MB/s: {total_size/total_time:.2f} MB/s")

print(f"\n🔥 ANALYSE CACHE (Requêtes Chaudes vs Froides):")
print(f"   • Temps moyen requête FROIDE: {cold_avg:.3f}s")
print(f"   • Temps moyen requête TIÈDE: {warm_avg:.3f}s")
print(f"   • Temps moyen requête CHAUDE: {hot_avg:.3f}s")
print(f"   • Amélioration du cache: {cache_improvement:.1f}%")

print(f"\n📈 MÉTRIQUES DE DISPERSION:")
print(f"   • Temps min: {detailed_df['time'].min():.3f}s")
print(f"   • Temps max: {detailed_df['time'].max():.3f}s")
print(f"   • Temps moyen: {detailed_df['time'].mean():.3f}s")
print(f"   • Temps médian: {detailed_df['time'].median():.3f}s")
print(f"   • Écart-type: {detailed_df['time'].std():.3f}s")
print(f"   • Coefficient de variation: {(detailed_df['time'].std()/detailed_df['time'].mean()*100):.1f}%")

# Identifier les datasets les plus lents
print(f"\n⚠️  TOP 3 DATASETS LES PLUS LENTS:")
slowest = performance_df.nlargest(3, 'time')
for idx, (_, row) in enumerate(slowest.iterrows(), 1):
    print(f"   {idx}. {row['dataset']}: {row['time']:.2f}s ({row['rows']:,} lignes)")

# Identifier les datasets les plus rapides
print(f"\n⚡ TOP 3 DATASETS LES PLUS RAPIDES (débit):")
fastest = performance_df.nlargest(3, 'rows_per_sec')
for idx, (_, row) in enumerate(fastest.iterrows(), 1):
    print(f"   {idx}. {row['dataset']}: {row['rows_per_sec']:,.0f} lignes/s")

print(f"\n{'='*75}")
print("\n✅ Analyse de performance terminée avec succès!")
print(f"\n📁 Les {8} graphiques sont disponibles dans:")
print(f"   /home/alban/BigData/BigData/graphes/")
print(f"\n📊 Graphiques générés:")
print(f"   1. 1_temps_reponse_barres.png - Temps par dataset")
print(f"   2. 2_evolution_temporelle_courbes.png - Évolution chaude/froide")
print(f"   3. 3_distribution_histogramme.png - Distribution des temps")
print(f"   4. 4_dispersion_boxplot.png - Boxplot par type de requête")
print(f"   5. 4b_dispersion_boxplot_datasets.png - Boxplot par dataset")
print(f"   6. 5_correlation_scatter.png - Corrélation volume/temps")
print(f"   7. 6_heatmap_latence.png - Carte thermique")
print(f"   8. 7_performance_debit.png - Débit par dataset")
print(f"   9. 8_dashboard_complet.png - Dashboard récapitulatif")
print(f"\n" + "="*75)
