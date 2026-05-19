@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM Real-Time Crypto Market Monitoring System
REM Script de lancement propre
REM ============================================================

set "PROJECT_ROOT=%~dp0"
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

set "CONTAINER_DIR=%PROJECT_ROOT%\container"
set "API_DIR=%PROJECT_ROOT%\api"
set "DASHBOARD_DIR=%PROJECT_ROOT%\dashboard"

echo.
echo ============================================================
echo  REAL-TIME CRYPTO MARKET MONITORING SYSTEM
echo ============================================================
echo.
echo Projet        : %PROJECT_ROOT%
echo Container dir : %CONTAINER_DIR%
echo API dir       : %API_DIR%
echo Dashboard dir : %DASHBOARD_DIR%
echo.

REM ============================================================
REM 1. Verification docker-compose
REM ============================================================

echo [1/10] Verification docker-compose...

if not exist "%CONTAINER_DIR%\docker-compose.yml" (
    echo [ERREUR] docker-compose.yml introuvable dans :
    echo %CONTAINER_DIR%
    pause
    exit /b 1
)

where docker >nul 2>nul
if errorlevel 1 (
    echo [ERREUR] Docker n'est pas installe ou pas disponible dans le PATH.
    pause
    exit /b 1
)

docker compose version >nul 2>nul
if errorlevel 1 (
    echo [ERREUR] La commande "docker compose" n'est pas disponible.
    echo Verifie que Docker Desktop est bien installe et lance.
    pause
    exit /b 1
)

echo [OK] Docker Compose disponible.
echo.

REM ============================================================
REM 2. Lancement Docker Compose
REM ============================================================

echo [2/10] Lancement de Kafka, MongoDB, Kafka UI et Mongo Express...

cd /d "%CONTAINER_DIR%"
docker compose up -d

if errorlevel 1 (
    echo [ERREUR] Docker Compose n'a pas pu demarrer.
    pause
    exit /b 1
)

echo.
echo [OK] Services Docker lances.
echo.

REM ============================================================
REM 3. Attente Kafka healthy
REM ============================================================

echo [3/10] Attente que Kafka soit healthy...

set /a TRY_COUNT=0

:WAIT_KAFKA
set "KAFKA_HEALTH="

for /f "delims=" %%i in ('docker inspect -f "{{.State.Health.Status}}" kafka 2^>nul') do (
    set "KAFKA_HEALTH=%%i"
)

if "!KAFKA_HEALTH!"=="healthy" (
    echo [OK] Kafka est healthy.
    goto KAFKA_READY
)

set /a TRY_COUNT+=1

if !TRY_COUNT! GEQ 40 (
    echo [ERREUR] Kafka n'est pas devenu healthy.
    echo Verifie avec :
    echo docker logs kafka
    pause
    exit /b 1
)

echo Kafka pas encore pret... tentative !TRY_COUNT!/40
timeout /t 3 >nul
goto WAIT_KAFKA

:KAFKA_READY
echo.

REM ============================================================
REM 4. Verification / installation requirements.txt
REM ============================================================

echo [4/10] Verification des dependances Python requirements.txt...

cd /d "%PROJECT_ROOT%"

set "PYTHON_CMD="

where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=python"
) else (
    where py >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_CMD=py -3"
    )
)

if "!PYTHON_CMD!"=="" (
    echo [ERREUR] Python n'est pas installe ou pas disponible dans le PATH.
    pause
    exit /b 1
)

if not exist "%PROJECT_ROOT%\requirements.txt" (
    echo [ERREUR] requirements.txt introuvable :
    echo %PROJECT_ROOT%\requirements.txt
    pause
    exit /b 1
)

echo [INFO] Python detecte :
!PYTHON_CMD! --version

echo.
echo [INFO] Verification de pip...

!PYTHON_CMD! -m pip --version >nul 2>nul
if errorlevel 1 (
    echo [ATTENTION] pip indisponible. Tentative d'installation avec ensurepip...
    !PYTHON_CMD! -m ensurepip --upgrade

    if errorlevel 1 (
        echo [ERREUR] Impossible d'initialiser pip.
        pause
        exit /b 1
    )
)

echo.
echo [INFO] Installation / verification des librairies Python...
echo Si une librairie est deja installee, pip affichera "Requirement already satisfied".
echo Sinon, elle sera installee automatiquement.
echo.

!PYTHON_CMD! -m pip install -r "%PROJECT_ROOT%\requirements.txt"

if errorlevel 1 (
    echo [ERREUR] Installation des dependances Python echouee.
    pause
    exit /b 1
)

echo.
echo [INFO] Verification des conflits de dependances Python...

!PYTHON_CMD! -m pip check
if errorlevel 1 (
    echo.
    echo [ATTENTION] pip check a detecte des conflits dans l'environnement Python.
    echo Le script continue quand meme, mais si un consumer plante, verifie ces conflits.
    echo.
) else (
    echo [OK] Aucune incompatibilite Python detectee.
)

echo [OK] Dependances Python verifiees.
echo.

REM ============================================================
REM 5. Creation / verification des topics Kafka
REM ============================================================

echo [5/10] Creation / verification des topics Kafka...

cd /d "%PROJECT_ROOT%"

if not exist "%PROJECT_ROOT%\scripts\admin\create_topics.py" (
    echo [ERREUR] Script introuvable :
    echo %PROJECT_ROOT%\scripts\admin\create_topics.py
    pause
    exit /b 1
)

!PYTHON_CMD! scripts\admin\create_topics.py

if errorlevel 1 (
    echo [ERREUR] La creation des topics a echoue.
    pause
    exit /b 1
)

echo [OK] Topics Kafka verifies.
echo.

REM ============================================================
REM 6. Verification et lancement API + Dashboard
REM ============================================================

echo [6/10] Verification API Node.js et dashboard...

if not exist "%API_DIR%\server.js" (
    echo [ERREUR] API Node.js introuvable :
    echo %API_DIR%\server.js
    pause
    exit /b 1
)

if not exist "%API_DIR%\package.json" (
    echo [ERREUR] package.json API introuvable :
    echo %API_DIR%\package.json
    pause
    exit /b 1
)

if not exist "%DASHBOARD_DIR%\index.html" (
    echo [ERREUR] Dashboard introuvable :
    echo %DASHBOARD_DIR%\index.html
    pause
    exit /b 1
)

where node >nul 2>nul
if errorlevel 1 (
    echo [ERREUR] Node.js n'est pas installe ou pas disponible dans le PATH.
    pause
    exit /b 1
)

where npm >nul 2>nul
if errorlevel 1 (
    echo [ERREUR] npm n'est pas installe ou pas disponible dans le PATH.
    pause
    exit /b 1
)

echo [INFO] Verification / installation des dependances API Node.js...

cd /d "%API_DIR%"

if not exist "%API_DIR%\node_modules" (
    echo [INFO] node_modules absent. Installation avec npm install...
    call npm install

    if errorlevel 1 (
        echo [ERREUR] npm install a echoue.
        pause
        exit /b 1
    )
) else (
    echo [OK] node_modules deja present.
)

netstat -ano | findstr /R /C:":3000 .*LISTENING" >nul 2>nul
if not errorlevel 1 (
    echo [ERREUR] Le port 3000 est deja utilise.
    echo Ferme l'ancienne fenetre API Node.js ou le processus qui utilise ce port, puis relance ce script.
    pause
    exit /b 1
)

echo [INFO] Lancement API REST + Socket.IO + Dashboard...
start "00 - API REST Socket.IO Dashboard" /D "%API_DIR%" cmd /k "npm start"

echo [INFO] Attente du dashboard sur http://localhost:3000 ...
set /a API_TRY_COUNT=0

:WAIT_API
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing 'http://localhost:3000/'; if ($r.Content -like '*Real-Time Crypto Market Monitoring*') { exit 0 } else { exit 2 } } catch { exit 1 }" >nul 2>nul

if not errorlevel 1 (
    echo [OK] Dashboard disponible.
    goto API_READY
)

set /a API_TRY_COUNT+=1

if !API_TRY_COUNT! GEQ 30 (
    echo [ERREUR] Le dashboard n'a pas repondu correctement sur http://localhost:3000.
    echo Regarde la fenetre "00 - API REST Socket.IO Dashboard" pour le detail de l'erreur.
    pause
    exit /b 1
)

echo Dashboard pas encore pret... tentative !API_TRY_COUNT!/30
timeout /t 2 >nul
goto WAIT_API

:API_READY

echo.
echo API REST       : http://localhost:3000/api/health
echo Dashboard Live : http://localhost:3000
echo.

REM ============================================================
REM 7. Ouverture interfaces utiles avec Chrome sinon Firefox
REM ============================================================

echo [7/10] Ouverture des interfaces en navigation privee...

set "DASHBOARD_URL=http://localhost:3000"
set "KAFKA_UI_URL=http://localhost:8080"
set "MONGO_EXPRESS_URL=http://localhost:8081"
set "API_HEALTH_URL=http://localhost:3000/api/health"

set "BROWSER_EXE="
set "BROWSER_PRIVATE_ARG="

REM Chrome via PATH
where chrome >nul 2>nul
if not errorlevel 1 (
    set "BROWSER_EXE=chrome"
    set "BROWSER_PRIVATE_ARG=--incognito"
    echo [OK] Chrome detecte dans le PATH.
    goto BROWSER_FOUND
)

REM Chrome installation classique Windows
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
    set "BROWSER_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
    set "BROWSER_PRIVATE_ARG=--incognito"
    echo [OK] Chrome detecte dans Program Files.
    goto BROWSER_FOUND
)

if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" (
    set "BROWSER_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
    set "BROWSER_PRIVATE_ARG=--incognito"
    echo [OK] Chrome detecte dans Program Files x86.
    goto BROWSER_FOUND
)

REM Firefox via PATH
where firefox >nul 2>nul
if not errorlevel 1 (
    set "BROWSER_EXE=firefox"
    set "BROWSER_PRIVATE_ARG=-private-window"
    echo [OK] Firefox detecte dans le PATH.
    goto BROWSER_FOUND
)

REM Firefox installation classique Windows
if exist "%ProgramFiles%\Mozilla Firefox\firefox.exe" (
    set "BROWSER_EXE=%ProgramFiles%\Mozilla Firefox\firefox.exe"
    set "BROWSER_PRIVATE_ARG=-private-window"
    echo [OK] Firefox detecte dans Program Files.
    goto BROWSER_FOUND
)

if exist "%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe" (
    set "BROWSER_EXE=%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe"
    set "BROWSER_PRIVATE_ARG=-private-window"
    echo [OK] Firefox detecte dans Program Files x86.
    goto BROWSER_FOUND
)

:BROWSER_FOUND

if "!BROWSER_EXE!"=="" (
    echo [ATTENTION] Chrome et Firefox introuvables.
    echo Les interfaces ne seront pas ouvertes automatiquement.
    echo.
    echo Ouvre manuellement :
    echo Dashboard     : !DASHBOARD_URL!
    echo Kafka UI      : !KAFKA_UI_URL!
    echo Mongo Express : !MONGO_EXPRESS_URL!
    echo API Health    : !API_HEALTH_URL!
) else (
    echo [INFO] Navigateur utilise : !BROWSER_EXE!
    echo.

    start "" "!BROWSER_EXE!" !BROWSER_PRIVATE_ARG! "!DASHBOARD_URL!"
    timeout /t 1 >nul

    start "" "!BROWSER_EXE!" !BROWSER_PRIVATE_ARG! "!KAFKA_UI_URL!"
    timeout /t 1 >nul

    start "" "!BROWSER_EXE!" !BROWSER_PRIVATE_ARG! "!MONGO_EXPRESS_URL!"
    timeout /t 1 >nul

    start "" "!BROWSER_EXE!" !BROWSER_PRIVATE_ARG! "!API_HEALTH_URL!"
)

echo.
echo Kafka UI      : !KAFKA_UI_URL!
echo Mongo Express : !MONGO_EXPRESS_URL!
echo Login Mongo Express : admin / admin
echo Dashboard     : !DASHBOARD_URL!
echo API Health    : !API_HEALTH_URL!
echo.

REM ============================================================
REM 8. Lancement des consumers
REM ============================================================

echo [8/10] Lancement des consumers...

cd /d "%PROJECT_ROOT%"

if exist "%PROJECT_ROOT%\scripts\consumer\kafka_to_mongo.py" (
    start "01 - Consumer Trades to MongoDB" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\consumer\kafka_to_mongo.py"
) else (
    echo [ATTENTION] kafka_to_mongo.py introuvable.
)

timeout /t 2 >nul

if exist "%PROJECT_ROOT%\scripts\consumer\metrics_consumer.py" (
    start "02 - Consumer Metrics Calculator" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\consumer\metrics_consumer.py"
) else (
    echo [ATTENTION] metrics_consumer.py introuvable.
)

timeout /t 2 >nul

if exist "%PROJECT_ROOT%\scripts\consumer\metrics_to_mongo.py" (
    start "03 - Consumer Metrics to MongoDB" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\consumer\metrics_to_mongo.py"
) else (
    echo [ATTENTION] metrics_to_mongo.py introuvable.
)

timeout /t 2 >nul

if exist "%PROJECT_ROOT%\scripts\consumer\alerts_consumer.py" (
    start "04 - Consumer Alerts Calculator" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\consumer\alerts_consumer.py"
) else (
    echo [ATTENTION] alerts_consumer.py introuvable.
)

timeout /t 2 >nul

if exist "%PROJECT_ROOT%\scripts\consumer\alerts_to_mongo.py" (
    start "05 - Consumer Alerts to MongoDB" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\consumer\alerts_to_mongo.py"
) else (
    echo [ATTENTION] alerts_to_mongo.py introuvable.
)

timeout /t 3 >nul

REM ============================================================
REM 9. Lancement des producers
REM ============================================================

echo [9/10] Lancement des producers...

if exist "%PROJECT_ROOT%\scripts\producer\binance.py" (
    start "06 - Producer Binance" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\producer\binance.py"
) else (
    echo [ERREUR] binance.py introuvable.
    pause
    exit /b 1
)

timeout /t 2 >nul

if exist "%PROJECT_ROOT%\scripts\producer\coinbase.py" (
    start "07 - Producer Coinbase" /D "%PROJECT_ROOT%" cmd /k "!PYTHON_CMD! scripts\producer\coinbase.py"
) else (
    echo [INFO] coinbase.py introuvable. Producer Coinbase ignore.
)

REM ============================================================
REM 10. Resume
REM ============================================================

echo.
echo [10/10] Systeme lance.
echo.
echo ============================================================
echo  SYSTEME LANCE
echo ============================================================
echo.
echo Interfaces :
echo - Kafka UI      : http://localhost:8080
echo - Mongo Express : http://localhost:8081
echo - Dashboard     : http://localhost:3000
echo - API Health    : http://localhost:3000/api/health
echo.
echo Fenetres de verification ouvertes :
echo - 00 API REST Socket.IO Dashboard
echo - 01 Consumer Trades to MongoDB
echo - 02 Consumer Metrics Calculator
echo - 03 Consumer Metrics to MongoDB
echo - 04 Consumer Alerts Calculator
echo - 05 Consumer Alerts to MongoDB
echo - 06 Producer Binance
echo - 07 Producer Coinbase si disponible
echo.
echo Collections MongoDB attendues :
echo - crypto.trades
echo - crypto.metrics
echo - crypto.alerts
echo.
echo Topics Kafka attendus :
echo - crypto.trades.raw
echo - crypto.metrics
echo - crypto.alerts
echo - crypto.trades.clean
echo.
echo Pour arreter les scripts : CTRL + C dans chaque fenetre.
echo Pour arreter Docker sans supprimer les donnees :
echo cd container
echo docker compose down
echo.
pause