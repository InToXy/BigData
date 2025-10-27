# 📊 Superset Dashboards

Ce dossier contient les exports JSON des dashboards Superset.

## 📁 Structure

```
dashboards/
├── README.md                                    # Ce fichier
├── healthcare_analytics_dashboard.json         # Dashboard principal
└── exports/                                     # Exports datés
    └── healthcare_analytics_YYYYMMDD.json
```

## 💾 Import d'un Dashboard

1. Accéder à Superset: http://localhost:8088
2. **Dashboards** > **Import dashboard**
3. Sélectionner le fichier JSON
4. Cliquer **Import**

## 🔄 Mise à Jour

Pour mettre à jour un dashboard existant:
1. Exporter le dashboard actuel
2. Sauvegarder dans `exports/` avec la date
3. Remplacer le fichier principal

## 📝 Convention de Nommage

```
dashboard_name_YYYYMMDD.json
```

Exemple: `healthcare_analytics_20251027.json`

## 🔗 Dashboards Disponibles

### Healthcare Analytics Dashboard
**Fichier**: `healthcare_analytics_dashboard.json`
**Description**: Dashboard principal avec 8 indicateurs clés
**Tables utilisées**:
- fact_consultation
- fact_hospitalisation
- fact_deces
- mart_satisfaction_region_2020
- mart_professionnel
- mart_demographie
- mart_deces_localisation_2019

**Indicateurs**:
1. Taux de consultation par établissement
2. Taux de consultation par diagnostic
3. Taux global d'hospitalisation
4. Taux d'hospitalisation par diagnostic
5. Taux d'hospitalisation par sexe/âge
6. Taux de consultation par professionnel
7. Nombre de décès par région (2019)
8. Taux de satisfaction par région (2020)

## 🎨 Personnalisation

Pour modifier un dashboard:
1. Importer le dashboard
2. **Edit Dashboard**
3. Apporter vos modifications
4. **Export** > Sauvegarder dans ce dossier

## 🔐 Backup

Les dashboards sont automatiquement sauvegardés dans la base PostgreSQL Superset.

Pour un backup manuel:
```bash
docker exec chu_superset_db pg_dump -U superset superset > superset_backup_$(date +%Y%m%d).sql
```
