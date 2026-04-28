@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"

title Voniko-Web Deployment

echo.
echo  +==============================================================+
echo  ^|           VONIKO-WEB --- ONE-CLICK DEPLOY                  ^|
echo  ^|  Frontend :3000  ^|  Backend :3001  ^|  HW :8765  ^|  AI :8001  ^|
echo  +==============================================================+
echo.

:: -------------------------------------------------------
::  STEP 0: Ensure PM2 is installed
:: -------------------------------------------------------
where pm2 >nul 2>&1
if errorlevel 1 (
    echo  [INSTALL] PM2 not found. Installing pm2 globally...
    call npm install -g pm2
    if errorlevel 1 (
        echo  [ERROR] Failed to install pm2. Please run: npm install -g pm2
        pause
        exit /b 1
    )
    echo  [OK] PM2 installed.
) else (
    echo  [OK] PM2 already installed.
)
echo.

:: -------------------------------------------------------
::  STEP 1: Clean up old PM2 processes and occupied ports
:: -------------------------------------------------------
echo [1/6] Cleaning up old PM2 processes and ports...

call pm2 stop all 2>nul
call pm2 delete all 2>nul
call pm2 kill 2>nul
echo  [OK] PM2 cleaned.

echo  [..] Releasing port 3000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3000 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo  [..] Releasing port 3001...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3001 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo  [..] Releasing port 8765...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo  [..] Releasing port 8001...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8001 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo  [OK] Ports released.
echo.

:: -------------------------------------------------------
::  STEP 2: Install dependencies if needed
:: -------------------------------------------------------
echo [2/6] Checking dependencies...

if not exist "backend\node_modules" (
    echo  [INSTALL] Installing backend dependencies...
    cd backend
    call npm install
    call npm audit fix
    echo  [..] Rebuilding native modules after install...
    call npm rebuild
    if errorlevel 1 (
        echo  [WARN] npm rebuild failed. Backend may not start correctly.
    ) else (
        echo  [OK] Native modules rebuilt.
    )
    cd ..
    echo  [OK] Backend dependencies installed.
) else (
    echo  [OK] Backend node_modules already present.
    echo  [..] Rebuilding native modules for this machine's Node.js...
    cd backend
    call npm rebuild
    if errorlevel 1 (
        echo  [WARN] npm rebuild failed. Backend may not start correctly.
    ) else (
        echo  [OK] Native modules rebuilt.
    )
    cd ..
)

if not exist "frontend\node_modules" (
    echo  [INSTALL] Installing frontend dependencies...
    cd frontend
    call npm install
    call npm audit fix
    cd ..
    echo  [OK] Frontend dependencies installed.
) else (
    echo  [OK] Frontend node_modules already present.
)

echo  [..] Checking Python for count-batteries-service...
python --version >nul 2>&1
if errorlevel 1 (
    echo  [WARN] Python not found. count-batteries-service will not start.
    echo         Please install Python 3.9+ and re-run start.bat.
) else (
    if not exist "count-batteries-service\venv" (
        echo  [INSTALL] Creating Python venv for count-batteries-service...
        python -m venv "count-batteries-service\venv"
        if errorlevel 1 (
            echo  [WARN] Failed to create venv. count-batteries-service may not start.
        ) else (
            echo  [OK] Python venv created.
        )
    ) else (
        echo  [OK] count-batteries-service venv already present.
    )
    if exist "count-batteries-service\venv" (
        echo  [INSTALL] Installing / updating count-batteries-service Python packages...
        call "count-batteries-service\venv\Scripts\pip.exe" install -r "count-batteries-service\requirements.txt" --quiet
        if errorlevel 1 (
            echo  [WARN] pip install failed. count-batteries-service may not start.
        ) else (
            echo  [OK] count-batteries-service Python packages ready.
        )
    )
)

:: -------------------------------------------------------
::  STEP 3: Create .env if missing
:: -------------------------------------------------------
echo [3/6] Checking .env config...

if not exist "backend\.env" (
    copy "backend\.env.example" "backend\.env" >nul
    echo  [OK] .env created from .env.example. Edit it if needed.
) else (
    echo  [OK] .env already exists.
)
echo.

:: -------------------------------------------------------
::  STEP 4: Build frontend if dist not present
:: -------------------------------------------------------
echo [4/6] Checking frontend build...

if not exist "frontend\dist" (
    echo  [BUILD] Building frontend for production...
    cd frontend
    call npm run build
    cd ..
    echo  [OK] Frontend built to frontend\dist
) else (
    echo  [OK] frontend\dist already exists. (Delete it to rebuild)
)
echo.

:: -------------------------------------------------------
::  STEP 5: Create logs directory
:: -------------------------------------------------------
echo [5/6] Preparing logs directory...
if not exist "logs" mkdir logs
echo  [OK] logs\ ready.
echo.

:: -------------------------------------------------------
::  STEP 6: Start PM2
:: -------------------------------------------------------
echo [6/6] Starting services with PM2...

if exist "frontend\dist" (
    echo  [MODE] Production -- backend serves static frontend files
    call pm2 start ecosystem.config.js --only voniko-backend,voniko-count-batteries
) else (
    echo  [MODE] Development -- running all services
    call pm2 start ecosystem.config.js
)

call pm2 save --force >nul 2>&1
echo  [OK] PM2 processes saved.
echo.

:: -------------------------------------------------------
::  POST-START: Health check for voniko-backend
:: -------------------------------------------------------
echo  [..] Waiting 8 seconds for backend to stabilise...
timeout /t 8 /nobreak >nul

set BSTATUS=offline
where curl >nul 2>&1
if errorlevel 1 (
    echo  [WARN] curl not found; skipping HTTP health check.
    echo         Run: pm2 logs voniko-backend --lines 50  to inspect the backend.
    set BSTATUS=unknown
) else (
    curl -s --max-time 10 http://localhost:3001/api/health 2>nul | findstr /i "\"ok\"" >nul 2>&1
    if not errorlevel 1 set BSTATUS=online
)

echo  [..] Backend status: !BSTATUS!

echo !BSTATUS! | findstr /i "online" >nul
if errorlevel 1 (
    echo.
    echo  [ERROR] voniko-backend is NOT running correctly.
    echo  [ERROR] Status: !BSTATUS!
    echo  [ERROR] Last 30 lines of backend error log:
    echo  -----------------------------------------------
    if exist "logs\backend-error.log" (
        powershell -Command "Get-Content 'logs\backend-error.log' -Tail 30"
    ) else (
        echo  ^(log file not found yet -- run: pm2 logs voniko-backend --lines 50^)
    )
    echo  -----------------------------------------------
    echo  [HINT] Common causes:
    echo         1. Native module mismatch (better-sqlite3) -- re-run start.bat (npm rebuild
    echo            runs automatically whether node_modules is new or already present)
    echo         2. Missing or invalid backend\.env  (check JWT_SECRET etc.)
    echo         3. Port 3001 still occupied -- re-run start.bat
    echo         4. Run: pm2 logs voniko-backend --lines 100  for full error details
    echo.
) else (
    echo  [OK] Backend is online.
)
echo.

echo  +======================================================+
echo  ^|  SYSTEM STATUS                                      ^|
echo  +======================================================+
echo.
call pm2 list
echo.
echo  +======================================================+
echo  ^|  ACCESS INFORMATION                                 ^|
echo  ^|                                                     ^|
echo  ^|  Web App  : http://localhost:3001                   ^|
echo  ^|  API      : http://localhost:3001/api/health        ^|
echo  ^|  Hardware : http://127.0.0.1:8765/docs              ^|
echo  ^|  AI Count : http://127.0.0.1:8001/health            ^|
echo  ^|                                                     ^|
echo  ^|  Login    : admin / Admin@123456                    ^|
echo  ^|                                                     ^|
echo  ^|  Logs     : pm2 logs                                ^|
echo  ^|  Monitor  : pm2 monit                               ^|
echo  +======================================================+
echo.
pause
