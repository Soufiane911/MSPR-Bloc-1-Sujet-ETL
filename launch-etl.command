#!/bin/bash

# ============================================
# Launch ETL - macOS Version
# OBRail Europe - MSPR Project
# ============================================

# Couleurs pour les messages
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  OBRail Europe - Lancement ETL${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 1. Vérifier si Docker est installé
echo -e "${YELLOW}🔍 Vérification de Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ ERREUR : Docker n'est pas installé !${NC}"
    echo ""
    echo "Veuillez installer Docker Desktop depuis :"
    echo "https://www.docker.com/products/docker-desktop"
    echo ""
    read -p "Appuyez sur Entrée pour quitter..."
    exit 1
fi

echo -e "${GREEN}✅ Docker est installé${NC}"

# 2. Vérifier si Docker Desktop est en cours d'exécution
echo -e "${YELLOW}🔍 Vérification de Docker Desktop...${NC}"
if ! docker info &> /dev/null; then
    echo -e "${YELLOW}⚠️  Docker Desktop n'est pas démarré. Lancement...${NC}"
    
    # Lancer Docker Desktop sur macOS
    if [ -d "/Applications/Docker.app" ]; then
        open -a Docker
    elif [ -d "$HOME/Applications/Docker.app" ]; then
        open -a "$HOME/Applications/Docker"
    else
        echo -e "${RED}❌ Docker Desktop introuvable !${NC}"
        read -p "Appuyez sur Entrée pour quitter..."
        exit 1
    fi
    
    # Attendre que Docker soit prêt
    echo -e "${YELLOW}⏳ Attente du démarrage de Docker Desktop...${NC}"
    echo "Cela peut prendre 30-60 secondes..."
    echo ""
    
    attempts=0
    max_attempts=60
    while ! docker info &> /dev/null; do
        attempts=$((attempts + 1))
        if [ $attempts -ge $max_attempts ]; then
            echo -e "${RED}❌ Timeout : Docker Desktop ne démarre pas !${NC}"
            read -p "Appuyez sur Entrée pour quitter..."
            exit 1
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    echo -e "${GREEN}✅ Docker Desktop est prêt !${NC}"
else
    echo -e "${GREEN}✅ Docker Desktop est déjà en cours d'exécution${NC}"
fi

# 3. Se positionner dans le répertoire du projet
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 4. Vérifier que docker-compose.yml existe
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ ERREUR : docker-compose.yml introuvable !${NC}"
    echo "Assurez-vous de lancer ce script depuis le répertoire du projet."
    read -p "Appuyez sur Entrée pour quitter..."
    exit 1
fi

# 5. Démarrer la base de données
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Démarrage de l'infrastructure${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

echo -e "${YELLOW}🐳 Démarrage de PostgreSQL...${NC}"
docker-compose up -d database

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ ERREUR lors du démarrage de la base de données !${NC}"
    read -p "Appuyez sur Entrée pour quitter..."
    exit 1
fi

# 6. Attendre que PostgreSQL soit prêt
echo -e "${YELLOW}⏳ Attente de l'initialisation de PostgreSQL (30s)...${NC}"
sleep 30

# 7. Lancer l'ETL
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Exécution du pipeline ETL${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

echo -e "${YELLOW}🚀 Lancement de l'ETL...${NC}"
docker-compose --profile etl run --rm etl python main.py --force

ETL_STATUS=$?

echo ""
echo -e "${BLUE}========================================${NC}"

if [ $ETL_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ ETL exécuté avec succès !${NC}"
    echo ""
    echo -e "${GREEN}Les services sont disponibles :${NC}"
    echo -e "  • API REST : http://localhost:8000"
    echo -e "  • Swagger  : http://localhost:8000/docs"
    echo -e "  • Dashboard: http://localhost:8501"
else
    echo -e "${RED}❌ L'ETL a rencontré une erreur (code: $ETL_STATUS)${NC}"
fi

echo -e "${BLUE}========================================${NC}"
echo ""
read -p "Appuyez sur Entrée pour quitter..."
