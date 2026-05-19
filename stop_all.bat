@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM Real-Time Crypto Market Monitoring System
REM Script d'arret complet sans suppression des donnees
REM ============================================================

set "PROJECT_ROOT=%~dp0"
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

set "CONTAINER_DIR=%PROJECT_ROOT%\container"

echo.
echo ============================================================
echo  STOP - REAL-TIME CRYPTO MARKET MONITORING SYSTEM
echo ============================================================
echo.
echo Projet : %PROJECT_ROOT%
echo Container dir : %CONTAINER_DIR%
echo.

REM ============================================================
REM 1. Arret des scripts Python du projet
REM ============================================================

echo [1/4] Arret des producers et consumers Python...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$patterns = @( ^
'binance.py', ^
'coinbase.py', ^
'kafka_to_mongo.py', ^
'metrics_consumer.py', ^
'metrics_to_mongo.py', ^
'alerts_consumer.py', ^
'alerts_to_mongo.py' ^
); ^
$processes = Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python' -and $_.CommandLine }; ^
foreach ($p in $processes) { ^
  foreach ($pattern in $patterns) { ^
    if ($p.CommandLine -like ('*' + $pattern + '*')) { ^
      Write-Host ('Arret Python PID=' + $p.ProcessId + ' : ' + $pattern); ^
      Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue; ^
      break; ^
    } ^
  } ^
}"

echo [OK] Scripts Python arretes si presents.
echo.

REM ============================================================
REM 2. Arret de l'API Node.js
REM ============================================================

echo [2/4] Arret de l'API Node.js Socket.IO...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$processes = Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'node' -and $_.CommandLine -like '*server.js*' }; ^
foreach ($p in $processes) { ^
  Write-Host ('Arret Node API PID=' + $p.ProcessId); ^
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue; ^
}"

REM Securite : si le port 3000 est encore utilise, on tue le processus concerne
for /f "tokens=5" %%a in ('netstat -ano ^| findstr /R /C:":3000 .*LISTENING"') do (
    echo Arret du processus utilisant le port 3000 : PID %%a
    taskkill /PID %%a /F >nul 2>nul
)

echo [OK] API Node.js arretee si presente.
echo.

REM ============================================================
REM 3. Fermeture des fenetres CMD lancees par start_all.bat
REM ============================================================

echo [3/4] Fermeture des fenetres de verification...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$titles = @( ^
'00 - API REST Socket.IO Dashboard', ^
'01 - Consumer Trades to MongoDB', ^
'02 - Consumer Metrics Calculator', ^
'03 - Consumer Metrics to MongoDB', ^
'04 - Consumer Alerts Calculator', ^
'05 - Consumer Alerts to MongoDB', ^
'06 - Producer Binance', ^
'07 - Producer Coinbase' ^
); ^
foreach ($title in $titles) { ^
  Get-Process | Where-Object { $_.MainWindowTitle -like ($title + '*') } | ForEach-Object { ^
    Write-Host ('Fermeture fenetre : ' + $_.MainWindowTitle); ^
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue; ^
  } ^
}"

echo [OK] Fenetres fermees si presentes.
echo.

REM ============================================================
REM 4. Arret Docker Compose sans supprimer les volumes
REM ============================================================

echo [4/4] Arret de Kafka, MongoDB, Kafka UI et Mongo Express...

if not exist "%CONTAINER_DIR%\docker-compose.yml" (
    echo [ATTENTION] docker-compose.yml introuvable dans :
    echo %CONTAINER_DIR%
    echo Docker Compose non arrete.
    pause
    exit /b 0
)

cd /d "%CONTAINER_DIR%"
docker compose down

if errorlevel 1 (
    echo [ERREUR] docker compose down a echoue.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  SYSTEME ARRETE
echo ============================================================
echo.
echo Les conteneurs Docker sont arretes.
echo Les volumes ne sont PAS supprimes.
echo MongoDB conserve donc les donnees.
echo Kafka conserve aussi ses donnees locales.
echo.
echo Pour tout supprimer, il faudrait utiliser manuellement :
echo docker compose down -v
echo.
pause