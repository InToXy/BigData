#!/usr/bin/env python3
"""
Script de mesure des performances de lecture depuis MinIO (silver layer)
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
print("📊 Génération de graphiques avancés de performance - BUCKET SILVER\n")

# Configuration MinIO
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "silver"

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

# Découverte automatique des datasets dans le bucket silver
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
        print(f"💡 Vérifiez que les données ont été transformées dans MinIO")
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
    endpoint_override=MINIO_ENDPOINT,
    scheme="http"
)

# Types de requêtes pour simuler cache cold/warm/hot
query_types = ['Froide', 'Tiède', 'Chaude']

for pass_num, query_type in enumerate(query_types, 1):
    print(f"\n🔄 {'Première' if pass_num == 1 else 'Deuxième' if pass_num == 2 else 'Troisième'} lecture ({query_type.lower()})...")
    
    for dataset in datasets:
        try:
            # Chemin vers les fichiers Parquet
            path = f"{BUCKET}/{dataset}"
            
            # Mesurer le temps de lecture
            start = time.time()
            table = pq.read_table(path, filesystem=s3)
            df = table.to_pandas()
            duration = time.time() - start
            
            # Calculer métriques
            num_rows = len(df)
            throughput = num_rows / duration if duration > 0 else 0
            size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            
            # Stocker dans les données détaillées
            detailed_measurements.append({
                'dataset': dataset,
                'query_type': query_type,
                'time': duration,
                'rows': num_rows,
                'throughput': throughput,
                'size_mb': size_mb,
                'pass_num': pass_num
            })
            
            # Première passe : afficher et stocker les données principales
            if pass_num == 1:
                performance_data.append({
                    'dataset': dataset,
                    'rows': num_rows,
                    'time': duration,
                    'throughput': throughput,
                    'size_mb': size_mb
                })
                
                print(f"{dataset:<35} {num_rows:>12,} {duration:>9.2f}s {throughput:>12,.0f} r/s")
        
        except Exception as e:
            if pass_num == 1:
                print(f"{dataset:<35} {'ERROR':>12} {str(e)[:40]}")
                performance_data.append({
                    'dataset': dataset,
                    'rows': 0,
                    'time': 0,
                    'throughput': 0,
                    'size_mb': 0
                })

# Convertir en DataFrame pour analyses
df_perf = pd.DataFrame(performance_data)
detailed_df = pd.DataFrame(detailed_measurements)

print("\n" + "="*75)
print("📈 Génération des graphiques avancés de performance...")
print("="*75)

# Configuration du style pour tous les graphiques
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# =============================================================================
# 1. GRAPHIQUE EN BARRES - Temps de réponse par dataset
# =============================================================================
print("\n1️⃣  Graphique en barres - Temps de réponse par dataset...")
fig, ax = plt.subplots(figsize=(14, 8))
colors = plt.cm.viridis(np.linspace(0, 1, len(df_perf)))
bars = ax.barh(df_perf['dataset'], df_perf['time'], color=colors)

# Ajouter les valeurs sur les barres
for i, (bar, time_val) in enumerate(zip(bars, df_perf['time'])):
    ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
            f'{time_val:.3f}s', va='center', fontsize=9)

ax.set_xlabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
ax.set_ylabel('Dataset', fontsize=12, fontweight='bold')
ax.set_title('Performance de lecture par dataset - SILVER LAYER\n(Requête froide)', 
             fontsize=14, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3, axis='x')
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/1_temps_reponse_barres.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 1_temps_reponse_barres.png")

# =============================================================================
# 2. GRAPHIQUE EN COURBES - Évolution des temps (cache cold/warm/hot)
# =============================================================================
print("2️⃣  Graphique en courbes - Évolution des temps de réponse (chaude/froide)...")
fig, ax = plt.subplots(figsize=(14, 8))

# Créer une courbe pour chaque type de requête
for query_type in query_types:
    subset = detailed_df[detailed_df['query_type'] == query_type].sort_values('dataset')
    ax.plot(range(len(subset)), subset['time'], marker='o', linewidth=2, 
            markersize=6, label=f'Requête {query_type}', alpha=0.8)

ax.set_xlabel('Dataset (Index)', fontsize=12, fontweight='bold')
ax.set_ylabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
ax.set_title('Évolution temporelle des performances - SILVER LAYER\n(Comparaison Cache Froid/Tiède/Chaud)', 
             fontsize=14, fontweight='bold', pad=20)
ax.legend(loc='best', fontsize=11, framealpha=0.9)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/2_evolution_temporelle_courbes.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 2_evolution_temporelle_courbes.png")

# =============================================================================
# 3. HISTOGRAMME - Distribution des temps de réponse
# =============================================================================
print("3️⃣  Histogramme - Distribution des temps de réponse...")
fig, ax = plt.subplots(figsize=(12, 7))
ax.hist(df_perf['time'], bins=20, color='skyblue', edgecolor='black', alpha=0.7)
ax.axvline(df_perf['time'].mean(), color='red', linestyle='--', linewidth=2, 
           label=f'Moyenne: {df_perf["time"].mean():.3f}s')
ax.axvline(df_perf['time'].median(), color='green', linestyle='--', linewidth=2,
           label=f'Médiane: {df_perf["time"].median():.3f}s')

ax.set_xlabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
ax.set_ylabel('Fréquence', fontsize=12, fontweight='bold')
ax.set_title('Distribution des temps de réponse - SILVER LAYER\n(Histogramme)', 
             fontsize=14, fontweight='bold', pad=20)
ax.legend(loc='best', fontsize=11)
ax.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/3_distribution_histogramme.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 3_distribution_histogramme.png")

# =============================================================================
# 4. BOXPLOT - Analyse de dispersion par type de requête
# =============================================================================
print("4️⃣  Boxplot - Analyse de dispersion par type de requête...")
fig, ax = plt.subplots(figsize=(12, 7))
sns.boxplot(data=detailed_df, x='query_type', y='time', 
            palette='Set2', ax=ax)

ax.set_xlabel('Type de requête', fontsize=12, fontweight='bold')
ax.set_ylabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
ax.set_title('Dispersion des temps de réponse par type de cache - SILVER LAYER\n(Boxplot avec quartiles)', 
             fontsize=14, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/4_dispersion_boxplot.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 4_dispersion_boxplot.png")

# Boxplot additionnel par dataset (top 15)
if len(df_perf) > 5:
    top_datasets = df_perf.nlargest(15, 'time')['dataset'].tolist()
    subset_data = detailed_df[detailed_df['dataset'].isin(top_datasets)]
    
    fig, ax = plt.subplots(figsize=(14, 8))
    sns.boxplot(data=subset_data, x='dataset', y='time', 
                palette='Set3', ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    ax.set_xlabel('Dataset (Top 15)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
    ax.set_title('Dispersion des temps par dataset - SILVER LAYER\n(Top 15 datasets les plus lents)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/4b_dispersion_boxplot_datasets.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("   ✅ Sauvegardé: 4b_dispersion_boxplot_datasets.png")

# =============================================================================
# 5. SCATTER PLOT - Corrélation volume de données vs temps de réponse
# =============================================================================
print("5️⃣  Scatter plot - Corrélation volume de données vs temps de réponse...")
fig, ax = plt.subplots(figsize=(12, 7))

# Créer le scatter plot avec taille proportionnelle au nombre de lignes
sizes = (df_perf['rows'] / df_perf['rows'].max()) * 500 + 50
colors_scatter = plt.cm.plasma(np.linspace(0, 1, len(df_perf)))

scatter = ax.scatter(df_perf['size_mb'], df_perf['time'], 
                     s=sizes, c=colors_scatter, alpha=0.6, edgecolors='black')

# Ajouter une ligne de tendance
z = np.polyfit(df_perf['size_mb'], df_perf['time'], 1)
p = np.poly1d(z)
x_trend = np.linspace(df_perf['size_mb'].min(), df_perf['size_mb'].max(), 100)
ax.plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2, label='Tendance linéaire')

ax.set_xlabel('Taille des données (MB)', fontsize=12, fontweight='bold')
ax.set_ylabel('Temps de lecture (secondes)', fontsize=12, fontweight='bold')
ax.set_title('Corrélation Volume vs Performance - SILVER LAYER\n(Taille des bulles = nombre de lignes)', 
             fontsize=14, fontweight='bold', pad=20)
ax.legend(loc='best', fontsize=11)
ax.grid(True, alpha=0.3)

# Ajouter les labels pour les points outliers
for idx, row in df_perf.iterrows():
    if row['time'] > df_perf['time'].quantile(0.75) or row['size_mb'] > df_perf['size_mb'].quantile(0.75):
        ax.annotate(row['dataset'], (row['size_mb'], row['time']),
                   xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.7)

plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/5_correlation_scatter.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 5_correlation_scatter.png")

# =============================================================================
# 6. HEATMAP - Carte thermique des latences par dataset et type de requête
# =============================================================================
print("6️⃣  Heatmap - Carte thermique des latences...")
# Créer une matrice pivot
heatmap_data = detailed_df.pivot_table(values='time', index='dataset', 
                                        columns='query_type', aggfunc='mean')

fig, ax = plt.subplots(figsize=(10, 12))
sns.heatmap(heatmap_data, annot=True, fmt='.3f', cmap='YlOrRd', 
            cbar_kws={'label': 'Temps (secondes)'}, ax=ax, linewidths=0.5)

ax.set_title('Heatmap des temps de réponse - SILVER LAYER\n(Par dataset et type de cache)', 
             fontsize=14, fontweight='bold', pad=20)
ax.set_xlabel('Type de requête', fontsize=12, fontweight='bold')
ax.set_ylabel('Dataset', fontsize=12, fontweight='bold')
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/6_heatmap_latence.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 6_heatmap_latence.png")

# =============================================================================
# 7. GRAPHIQUE DE DÉBIT - Lignes par seconde
# =============================================================================
print("7️⃣  Graphique de débit - Lignes par seconde...")
fig, ax = plt.subplots(figsize=(14, 8))

# Trier par débit décroissant
df_sorted = df_perf.sort_values('throughput', ascending=True)
colors_throughput = plt.cm.RdYlGn(np.linspace(0.2, 0.9, len(df_sorted)))

bars = ax.barh(df_sorted['dataset'], df_sorted['throughput'], color=colors_throughput)

# Ajouter les valeurs
for bar, throughput in zip(bars, df_sorted['throughput']):
    ax.text(bar.get_width() + max(df_sorted['throughput'])*0.01, 
            bar.get_y() + bar.get_height()/2,
            f'{throughput:,.0f}', va='center', fontsize=9)

ax.set_xlabel('Débit (lignes/seconde)', fontsize=12, fontweight='bold')
ax.set_ylabel('Dataset', fontsize=12, fontweight='bold')
ax.set_title('Performance de lecture en débit - SILVER LAYER\n(Lignes traitées par seconde)', 
             fontsize=14, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3, axis='x')
plt.tight_layout()
plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/7_performance_debit.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 7_performance_debit.png")

# =============================================================================
# 8. DASHBOARD RÉCAPITULATIF - 4 graphiques en un
# =============================================================================
print("8️⃣  Dashboard récapitulatif complet...")
fig = plt.figure(figsize=(18, 12))
gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

# Sous-graphique 1: Bar chart des temps
ax1 = fig.add_subplot(gs[0, 0])
top10 = df_perf.nlargest(10, 'time')
ax1.barh(top10['dataset'], top10['time'], color='coral')
ax1.set_xlabel('Temps (s)')
ax1.set_title('Top 10 - Temps de réponse', fontweight='bold')
ax1.grid(True, alpha=0.3, axis='x')

# Sous-graphique 2: Pie chart de la répartition des tailles
ax2 = fig.add_subplot(gs[0, 1])
top5_size = df_perf.nlargest(5, 'size_mb')
other_size = df_perf['size_mb'].sum() - top5_size['size_mb'].sum()
sizes_pie = list(top5_size['size_mb']) + [other_size]
labels_pie = list(top5_size['dataset']) + ['Autres']
ax2.pie(sizes_pie, labels=labels_pie, autopct='%1.1f%%', startangle=90)
ax2.set_title('Répartition des tailles (MB)', fontweight='bold')

# Sous-graphique 3: Boxplot des temps par type de requête
ax3 = fig.add_subplot(gs[1, :])
sns.boxplot(data=detailed_df, x='query_type', y='time', palette='Set2', ax=ax3)
ax3.set_title('Distribution des temps par type de cache', fontweight='bold')
ax3.set_ylabel('Temps (s)')
ax3.grid(True, alpha=0.3, axis='y')

# Sous-graphique 4: Scatter volume vs temps
ax4 = fig.add_subplot(gs[2, 0])
ax4.scatter(df_perf['rows'], df_perf['time'], s=100, alpha=0.6, c='purple')
ax4.set_xlabel('Nombre de lignes')
ax4.set_ylabel('Temps (s)')
ax4.set_title('Volume vs Temps', fontweight='bold')
ax4.grid(True, alpha=0.3)

# Sous-graphique 5: Top débit
ax5 = fig.add_subplot(gs[2, 1])
top10_throughput = df_perf.nlargest(10, 'throughput')
ax5.barh(top10_throughput['dataset'], top10_throughput['throughput'], color='lightgreen')
ax5.set_xlabel('Lignes/seconde')
ax5.set_title('Top 10 - Meilleur débit', fontweight='bold')
ax5.grid(True, alpha=0.3, axis='x')

# Titre global
fig.suptitle('🎯 Dashboard de Performance MinIO - SILVER LAYER', 
             fontsize=16, fontweight='bold', y=0.995)

plt.savefig("/home/alban/BigData/BigData/graphes/bucket_silver/8_dashboard_complet.png", dpi=150, bbox_inches='tight')
plt.close()
print("   ✅ Sauvegardé: 8_dashboard_complet.png")

# =============================================================================
# AFFICHAGE DU RÉSUMÉ FINAL
# =============================================================================
print("\n" + "="*75)
print("📋 RÉSUMÉ DES PERFORMANCES - SILVER LAYER")
print("="*75)
print(f"\n{'Dataset':<35} {'Lignes':>12} {'Temps':>10} {'Débit':>15} {'Taille':>12}")
print("-"*75)

for _, row in df_perf.iterrows():
    print(f"{row['dataset']:<35} {row['rows']:>12,} {row['time']:>9.2f}s "
          f"{row['throughput']:>12,.0f} r/s {row['size_mb']:>10.1f} MB")

print("="*75)

# Statistiques globales
total_rows = df_perf['rows'].sum()
total_time = df_perf['time'].sum()
total_size = df_perf['size_mb'].sum()
avg_throughput = total_rows / total_time if total_time > 0 else 0

print(f"\n📊 STATISTIQUES GLOBALES:")
print(f"   • Datasets analysés: {len(df_perf)}")
print(f"   • Total de lignes: {total_rows:,}")
print(f"   • Taille totale: {total_size:.2f} MB")
print(f"   • Temps total: {total_time:.2f}s")
print(f"   • Débit moyen: {avg_throughput:,.0f} lignes/seconde")
print(f"   • Débit en MB/s: {total_size/total_time:.2f} MB/s")

# Analyse cache
cold_times = detailed_df[detailed_df['query_type'] == 'Froide']['time']
warm_times = detailed_df[detailed_df['query_type'] == 'Tiède']['time']
hot_times = detailed_df[detailed_df['query_type'] == 'Chaude']['time']

print(f"\n🔥 ANALYSE CACHE (Requêtes Chaudes vs Froides):")
print(f"   • Temps moyen requête FROIDE: {cold_times.mean():.3f}s")
print(f"   • Temps moyen requête TIÈDE: {warm_times.mean():.3f}s")
print(f"   • Temps moyen requête CHAUDE: {hot_times.mean():.3f}s")
cache_improvement = ((cold_times.mean() - hot_times.mean()) / cold_times.mean() * 100)
print(f"   • Amélioration du cache: {cache_improvement:.1f}%")

# Métriques de dispersion
print(f"\n📈 MÉTRIQUES DE DISPERSION:")
print(f"   • Temps min: {df_perf['time'].min():.3f}s")
print(f"   • Temps max: {df_perf['time'].max():.3f}s")
print(f"   • Temps moyen: {df_perf['time'].mean():.3f}s")
print(f"   • Temps médian: {df_perf['time'].median():.3f}s")
print(f"   • Écart-type: {df_perf['time'].std():.3f}s")
print(f"   • Coefficient de variation: {(df_perf['time'].std() / df_perf['time'].mean() * 100):.1f}%")

# Top/Bottom performers
print(f"\n⚠️  TOP 3 DATASETS LES PLUS LENTS:")
for i, (_, row) in enumerate(df_perf.nlargest(3, 'time').iterrows(), 1):
    print(f"   {i}. {row['dataset']}: {row['time']:.2f}s ({row['rows']:,} lignes)")

print(f"\n⚡ TOP 3 DATASETS LES PLUS RAPIDES (débit):")
for i, (_, row) in enumerate(df_perf.nlargest(3, 'throughput').iterrows(), 1):
    print(f"   {i}. {row['dataset']}: {row['throughput']:,.0f} lignes/s")

print("\n" + "="*75)
print("\n✅ Analyse de performance terminée avec succès!")
print(f"\n📁 Les 8 graphiques sont disponibles dans:")
print(f"   /home/alban/BigData/BigData/graphes/bucket_silver/")
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
print("\n" + "="*75)
