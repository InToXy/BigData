-- ═══════════════════════════════════════════════════════
-- REQUÊTE SQL POUR SUPERSET - Consultations par Commune
-- ═══════════════════════════════════════════════════════
-- Copier-coller cette requête dans SQL Lab

-- Vue d'ensemble : Répartition NULL vs NON-NULL
SELECT 
    'Vue d''ensemble' as type_analyse,
    CASE 
        WHEN commune IS NULL THEN 'Établissements SANS commune' 
        ELSE 'Établissements AVEC commune' 
    END as categorie,
    COUNT(*) as nombre_etablissements,
    SUM(nombre_consultations) as total_consultations,
    ROUND(AVG(nombre_consultations), 2) as moyenne_consultations
FROM kpi_consultation_etablissement
GROUP BY (commune IS NULL);

-- ═══════════════════════════════════════════════════════

-- Top 20 communes par nombre de consultations
SELECT 
    commune,
    COUNT(*) as nb_etablissements,
    SUM(nombre_consultations) as total_consultations,
    ROUND(AVG(nombre_consultations), 2) as moyenne_par_etablissement,
    MAX(nombre_consultations) as max_consultations
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL
GROUP BY commune
ORDER BY total_consultations DESC
LIMIT 20;

-- ═══════════════════════════════════════════════════════

-- Détail des établissements avec commune (pour charts)
SELECT 
    commune,
    raison_sociale_site,
    nombre_consultations,
    nombre_etablissements_distincts
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL
  AND nombre_consultations > 5  -- Filtrer les petits volumes
ORDER BY nombre_consultations DESC
LIMIT 100;

-- ═══════════════════════════════════════════════════════

-- Top établissements par grande ville
SELECT 
    commune,
    raison_sociale_site,
    nombre_consultations
FROM kpi_consultation_etablissement
WHERE commune IN ('Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice', 'Nantes', 'Montpellier', 'Bordeaux')
ORDER BY commune, nombre_consultations DESC;

-- ═══════════════════════════════════════════════════════

-- Analyse par taille de ville (estimation basée sur nb établissements)
SELECT 
    CASE 
        WHEN nb_etab > 5000 THEN 'Très grande ville (>5000 étab.)'
        WHEN nb_etab > 1000 THEN 'Grande ville (1000-5000 étab.)'
        WHEN nb_etab > 100 THEN 'Ville moyenne (100-1000 étab.)'
        ELSE 'Petite ville (<100 étab.)'
    END as categorie_ville,
    COUNT(*) as nb_communes,
    SUM(total_consultations) as consultations_totales
FROM (
    SELECT 
        commune,
        COUNT(*) as nb_etab,
        SUM(nombre_consultations) as total_consultations
    FROM kpi_consultation_etablissement
    WHERE commune IS NOT NULL
    GROUP BY commune
) AS communes_stats
GROUP BY 
    CASE 
        WHEN nb_etab > 5000 THEN 'Très grande ville (>5000 étab.)'
        WHEN nb_etab > 1000 THEN 'Grande ville (1000-5000 étab.)'
        WHEN nb_etab > 100 THEN 'Ville moyenne (100-1000 étab.)'
        ELSE 'Petite ville (<100 étab.)'
    END
ORDER BY consultations_totales DESC;
