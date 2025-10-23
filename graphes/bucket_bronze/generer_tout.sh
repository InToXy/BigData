#!/bin/bash
# Script de génération complète de l'analyse de performance MinIO

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  🚀 GÉNÉRATION COMPLÈTE - ANALYSE DE PERFORMANCE MINIO        ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Vérifier que MinIO est accessible
echo "1️⃣  Vérification de MinIO..."
if docker ps | grep -q chu_minio; then
    echo "   ✅ MinIO est en cours d'exécution"
else
    echo "   ❌ ERREUR: MinIO n'est pas démarré"
    echo "   💡 Lancez: docker start chu_minio"
    exit 1
fi

# Vérifier les dépendances Python
echo ""
echo "2️⃣  Vérification des dépendances Python..."
python3 -c "import boto3, pyarrow, pandas, matplotlib, seaborn, numpy" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ Toutes les dépendances sont installées"
else
    echo "   ⚠️  Installation des dépendances manquantes..."
    pip3 install --user boto3 pyarrow pandas matplotlib seaborn numpy
fi

# Générer les graphiques
echo ""
echo "3️⃣  Génération des graphiques de performance..."
echo "   ⏳ Analyse en cours (3 passes de lecture)..."
python3 performance_minio.py
if [ $? -eq 0 ]; then
    echo "   ✅ 9 graphiques générés avec succès"
else
    echo "   ❌ ERREUR lors de la génération des graphiques"
    exit 1
fi

# Générer le rapport HTML
echo ""
echo "4️⃣  Génération du rapport HTML..."
python3 generer_rapport.py
if [ $? -eq 0 ]; then
    echo "   ✅ Rapport HTML généré: rapport_performance.html"
else
    echo "   ❌ ERREUR lors de la génération du rapport"
    exit 1
fi

# Résumé des fichiers créés
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ✅ GÉNÉRATION TERMINÉE AVEC SUCCÈS                           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 FICHIERS GÉNÉRÉS :"
echo ""
echo "   📈 Graphiques (9 fichiers PNG) :"
ls -1 [0-9]*.png 2>/dev/null | while read file; do
    size=$(du -h "$file" | cut -f1)
    echo "      ✓ $file ($size)"
done

echo ""
echo "   🌐 Rapport HTML :"
echo "      ✓ rapport_performance.html"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎯 PROCHAINES ÉTAPES :"
echo ""
echo "   Ouvrir le rapport HTML :"
echo "   $ explorer.exe rapport_performance.html"
echo ""
echo "   Ou consulter les graphiques individuellement :"
echo "   $ ls -lh *.png"
echo ""
echo "   Documentation complète :"
echo "   $ cat GUIDE_COMPLET.md"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
