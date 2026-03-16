@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: ============================================
:: Launch ETL - Windows Version
:: OBRail Europe - MSPR Project
:: ============================================

echo ========================================
echo   OBRail Europe - Lancement ETL
echo ========================================
echo.

:: 1. Vérifier si Docker est installé
echo [🔍] Vérification de Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [❌] ERREUR : Docker n'est pas installé !
    echo.
    echo Veuillez installer Docker Desktop depuis :
    echo https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

echo [✅] Docker est installé

:: 2. Vérifier si Docker Desktop est en cours d'exécution
echo [🔍] Vérification de Docker Desktop...
docker info >nul 2>&1
if errorlevel 1 (
    echo [⚠️] Docker Desktop n'est pas démarré. Lancement...
    
    :: Lancer Docker Desktop sur Windows
    set "DOCKER_PATH="
    
    :: Chercher Docker Desktop dans les emplacements courants
    if exist "C:\Program Files\Docker\Docker\Docker Desktop.exe" (
        set "DOCKER_PATH=C:\Program Files\Docker\Docker\Docker Desktop.exe"
    ) else if exist "C:\Program Files (x86)\Docker\Docker\Docker Desktop.exe" (
        set "DOCKER_PATH=C:\Program Files (x86)\Docker\Docker\Docker Desktop.exe"
    ) else if exist "%LOCALAPPDATA%\Docker\Docker\Docker Desktop.exe" (
        set "DOCKER_PATH=%LOCALAPPDATA%\Docker\Docker\Docker Desktop.exe"
    ) else if exist "%USERPROFILE%\AppData\Local\Docker\Docker\Docker Desktop.exe" (
        set "DOCKER_PATH=%USERPROFILE%\AppData\Local\Docker\Docker\Docker Desktop.exe"
    )
    
    if "!DOCKER_PATH!"=="" (
        echo [❌] Docker Desktop introuvable !
        pause
        exit /b 1
    )
    
    echo [🚀] Démarrage de Docker Desktop...
    start "" "!DOCKER_PATH!"
    
    :: Attendre que Docker soit prêt
    echo [⏳] Attente du démarrage de Docker Desktop...
    echo Cela peut prendre 30-60 secondes...
    echo.
    
    set /a attempts=0
    :wait_docker
    timeout /t 2 /nobreak >nul
    docker info >nul 2>&1
    if errorlevel 1 (
        set /a attempts+=1
        if !attempts! geq 60 (
            echo.
            echo [❌] Timeout : Docker Desktop ne démarre pas !
            pause
            exit /b 1
        )
        <nul set /p =.
        goto wait_docker
    )
    echo.
    echo [✅] Docker Desktop est prêt !
) else (
    echo [✅] Docker Desktop est déjà en cours d'exécution
)

:: 3. Se positionner dans le répertoire du projet
cd /d "%~dp0"

:: 4. Vérifier que docker-compose.yml existe
if not exist "docker-compose.yml" (
    echo [❌] ERREUR : docker-compose.yml introuvable !
    echo Assurez-vous de lancer ce script depuis le répertoire du projet.
    pause
    exit /b 1
)

:: 5. Démarrer la base de données
echo.
echo ========================================
echo   Démarrage de l'infrastructure
echo ========================================
echo.

echo [🐳] Démarrage de PostgreSQL...
docker-compose up -d database

if errorlevel 1 (
    echo [❌] ERREUR lors du démarrage de la base de données !
    pause
    exit /b 1
)

:: 6. Attendre que PostgreSQL soit prêt
echo [⏳] Attente de l'initialisation de PostgreSQL (30s)...
timeout /t 30 /nobreak >nul

:: 7. Lancer l'ETL
echo.
echo ========================================
echo   Exécution du pipeline ETL
echo ========================================
echo.

echo [🚀] Lancement de l'ETL...
docker-compose --profile etl run --rm etl python main.py --force

set ETL_STATUS=%ERRORLEVEL%

echo.
echo ========================================

if %ETL_STATUS% equ 0 (
    echo [✅] ETL exécuté avec succès !
    echo.
    echo Les services sont disponibles :
    echo   • API REST : http://localhost:8000
    echo   • Swagger  : http://localhost:8000/docs
    echo   • Dashboard: http://localhost:8501
) else (
    echo [❌] L'ETL a rencontré une erreur (code: %ETL_STATUS%)
)

echo ========================================
echo.
pause
