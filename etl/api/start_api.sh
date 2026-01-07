#!/bin/bash
# Script de démarrage de l'API REST

echo "🚀 Démarrage de l'API ObRail Europe..."
echo ""

cd "$(dirname "$0")/.."

# Vérifier que PostgreSQL est accessible
python -c "from api.database import get_db_connection; get_db_connection(); print('✅ Connexion à PostgreSQL OK')" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Erreur : Impossible de se connecter à PostgreSQL"
    echo "   Vérifiez que PostgreSQL est démarré et que la base obrail_europe existe"
    exit 1
fi

echo "✅ Base de données accessible"
echo ""
echo "🌐 Démarrage du serveur FastAPI..."
echo "   - API : http://localhost:8000"
echo "   - Documentation : http://localhost:8000/docs"
echo "   - ReDoc : http://localhost:8000/redoc"
echo ""
echo "Appuyez sur Ctrl+C pour arrêter le serveur"
echo ""

uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

