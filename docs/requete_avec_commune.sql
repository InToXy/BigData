-- REQUÊTE CORRIGÉE - Afficher SEULEMENT les lignes avec commune
SELECT 
    raison_sociale_site,
    commune,
    nombre_consultations,
    nombre_etablissements_distincts,
    annee
FROM kpi_consultation_etablissement
WHERE commune IS NOT NULL  -- ← FILTRE IMPORTANT
ORDER BY nombre_consultations DESC
LIMIT 100;
