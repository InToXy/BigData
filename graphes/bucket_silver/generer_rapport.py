#!/usr/bin/env python3
"""
Génération d'un rapport HTML interactif des performances MinIO - SILVER LAYER
"""
from datetime import datetime
import os

# Template HTML
html_template = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rapport de Performance - Silver Layer MinIO</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        header {
            background: linear-gradient(135deg, #0f766e 0%, #14b8a6 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f0fdfa;
        }
        
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.15);
        }
        
        .stat-card h3 {
            color: #14b8a6;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        
        .stat-card .value {
            font-size: 2.2em;
            font-weight: bold;
            color: #0f766e;
            margin-bottom: 5px;
        }
        
        .stat-card .label {
            color: #666;
            font-size: 0.9em;
        }
        
        .graphs-section {
            padding: 40px;
        }
        
        .section-title {
            font-size: 2em;
            color: #0f766e;
            margin-bottom: 30px;
            padding-bottom: 15px;
            border-bottom: 3px solid #14b8a6;
        }
        
        .graph-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
            gap: 30px;
            margin-bottom: 40px;
        }
        
        .graph-card {
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .graph-card:hover {
            transform: scale(1.02);
            box-shadow: 0 8px 25px rgba(0,0,0,0.2);
        }
        
        .graph-card h3 {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
            padding: 20px;
            font-size: 1.3em;
        }
        
        .graph-card img {
            width: 100%;
            height: auto;
            display: block;
        }
        
        .graph-card p {
            padding: 20px;
            color: #666;
            line-height: 1.6;
        }
        
        .dashboard-full {
            grid-column: 1 / -1;
        }
        
        footer {
            background: #0f766e;
            color: white;
            text-align: center;
            padding: 30px;
            margin-top: 40px;
        }
        
        .badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
            margin: 5px;
        }
        
        .badge-success {
            background: #10b981;
            color: white;
        }
        
        .badge-warning {
            background: #f59e0b;
            color: white;
        }
        
        .badge-danger {
            background: #ef4444;
            color: white;
        }
        
        .recommendations {
            background: #ecfdf5;
            border-left: 5px solid #14b8a6;
            padding: 20px;
            margin: 20px 40px;
            border-radius: 5px;
        }
        
        .recommendations h3 {
            color: #0f766e;
            margin-bottom: 15px;
        }
        
        .recommendations ul {
            list-style-position: inside;
            color: #0f766e;
        }
        
        .recommendations li {
            margin: 10px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 Rapport de Performance - Data Lake MinIO</h1>
            <p>Analyse de la couche Silver - {date}</p>
        </header>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>📁 Datasets Analysés</h3>
                <div class="value">{nb_datasets}</div>
                <div class="label">Tables Silver</div>
            </div>
            
            <div class="stat-card">
                <h3>📏 Total de Lignes</h3>
                <div class="value">{total_rows}</div>
                <div class="label">Lignes transformées</div>
            </div>
            
            <div class="stat-card">
                <h3>💾 Taille Totale</h3>
                <div class="value">{total_size}</div>
                <div class="label">Données Parquet</div>
            </div>
            
            <div class="stat-card">
                <h3>⚡ Débit Moyen</h3>
                <div class="value">{avg_throughput}</div>
                <div class="label">Lignes/seconde</div>
            </div>
            
            <div class="stat-card">
                <h3>⏱️ Temps Moyen</h3>
                <div class="value">{avg_time}s</div>
                <div class="label">Par requête</div>
            </div>
            
            <div class="stat-card">
                <h3>🔥 Cache Hit</h3>
                <div class="value">{cache_improvement}%</div>
                <div class="label">Amélioration cache</div>
            </div>
        </div>
        
        <div class="recommendations">
            <h3>💡 Recommandations Silver Layer</h3>
            <ul>
                <li><strong>Qualité des transformations</strong> : Vérifier la cohérence des données après ETL</li>
                <li><strong>Performance</strong> : Optimiser les datasets avec forte variabilité</li>
                <li><strong>Cache</strong> : Évaluer l'efficacité du système de mise en cache</li>
                <li><strong>Monitoring</strong> : Surveiller les performances pour détecter les dégradations</li>
            </ul>
        </div>
        
        <div class="graphs-section">
            <h2 class="section-title">📈 Graphiques d'Analyse</h2>
            
            <div class="graph-grid">
                <div class="graph-card">
                    <h3>1️⃣ Temps de Réponse par Dataset</h3>
                    <img src="1_temps_reponse_barres.png" alt="Temps de réponse">
                    <p>Diagramme en barres montrant le temps de réponse de chaque dataset transformé. 
                    Permet d'identifier rapidement les datasets les plus lents.</p>
                </div>
                
                <div class="graph-card">
                    <h3>2️⃣ Évolution Temporelle (Chaude/Froide)</h3>
                    <img src="2_evolution_temporelle_courbes.png" alt="Évolution temporelle">
                    <p>Graphique en courbes comparant les performances des requêtes chaudes (en cache) 
                    vs froides (première lecture). Essentiel pour évaluer l'efficacité du cache.</p>
                </div>
                
                <div class="graph-card">
                    <h3>3️⃣ Distribution des Temps de Réponse</h3>
                    <img src="3_distribution_histogramme.png" alt="Distribution">
                    <p>Histogramme de la distribution des temps avec moyenne et médiane. 
                    Permet de comprendre la variabilité et détecter les anomalies.</p>
                </div>
                
                <div class="graph-card">
                    <h3>4️⃣ Dispersion par Type de Requête</h3>
                    <img src="4_dispersion_boxplot.png" alt="Boxplot requêtes">
                    <p>Boxplot montrant la dispersion des temps pour chaque type de requête. 
                    Identifie les valeurs aberrantes et la stabilité des performances.</p>
                </div>
                
                <div class="graph-card">
                    <h3>5️⃣ Dispersion par Dataset</h3>
                    <img src="4b_dispersion_boxplot_datasets.png" alt="Boxplot datasets">
                    <p>Boxplot comparant la dispersion entre datasets (Top 15). 
                    Évalue la cohérence des performances sur plusieurs lectures.</p>
                </div>
                
                <div class="graph-card">
                    <h3>6️⃣ Corrélation Volume/Temps</h3>
                    <img src="5_correlation_scatter.png" alt="Scatter plot">
                    <p>Scatter plots montrant la corrélation entre volume de données et temps de réponse. 
                    Identifie les goulets d'étranglement et datasets mal optimisés.</p>
                </div>
                
                <div class="graph-card">
                    <h3>7️⃣ Carte Thermique des Latences</h3>
                    <img src="6_heatmap_latence.png" alt="Heatmap">
                    <p>Heatmap visualisant la latence par combinaison dataset × type de requête. 
                    Repère rapidement les patterns de performance.</p>
                </div>
                
                <div class="graph-card">
                    <h3>8️⃣ Débit par Dataset</h3>
                    <img src="7_performance_debit.png" alt="Débit">
                    <p>Barres montrant le débit en lignes/seconde. 
                    Compare l'efficacité de lecture entre datasets.</p>
                </div>
                
                <div class="graph-card dashboard-full">
                    <h3>9️⃣ Dashboard Récapitulatif Complet</h3>
                    <img src="8_dashboard_complet.png" alt="Dashboard">
                    <p>Vue d'ensemble complète avec 5 panneaux : temps, distribution, dispersion, 
                    corrélation et débit. Synthèse globale de la performance Silver Layer.</p>
                </div>
            </div>
        </div>
        
        <footer>
            <p>📊 Rapport généré automatiquement par performance_minio.py</p>
            <p>🏥 Projet CHU - Big Data Healthcare Analytics - Silver Layer</p>
            <p>📅 {date}</p>
        </footer>
    </div>
</body>
</html>
"""

# Générer le rapport
date_str = datetime.now().strftime("%d/%m/%Y à %H:%M")

# Remplacer les placeholders (valeurs par défaut, à mettre à jour après exécution du script)
html_content = html_template.replace("{date}", date_str)
html_content = html_content.replace("{nb_datasets}", "N/A")
html_content = html_content.replace("{total_rows}", "N/A")
html_content = html_content.replace("{total_size}", "N/A")
html_content = html_content.replace("{avg_throughput}", "N/A")
html_content = html_content.replace("{avg_time}", "N/A")
html_content = html_content.replace("{cache_improvement}", "N/A")

# Sauvegarder le fichier
output_path = "/home/alban/BigData/BigData/graphes/bucket_silver/rapport_performance.html"
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ Rapport HTML généré avec succès!")
print(f"📁 Fichier: {output_path}")
print(f"\n🌐 Pour visualiser le rapport, ouvrez le fichier dans un navigateur:")
print(f"   file://{output_path}")
