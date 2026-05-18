@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"

:: ============================================================
::  Cau hinh may tram DMP — Chi can sua cac dong duoi day
:: ============================================================
::
::  Tat ca duong dan database/template duoc cau hinh tap trung tai day.
::  Khi can doi duong dan tren mot may tram, CHI sua cac dong duoi —
::  KHONG can tim/sua trong source code.
::
::  Vi du:
::      Tren may DMP1:                Tren may DMP2:
::      DMP_DATA_DIR    = C:\DMP      DMP_DATA_DIR    = D:\DMP
::      DM2000_DATA_DIR = D:\DM2000   DM2000_DATA_DIR = D:\DM2000
::      DM3000_DATA_DIR = D:\DM3000   DM3000_DATA_DIR = D:\DM3000
::

set DMP_STATION_NAME=DMP1
:: Dung localhost khi backend chay tren cung may - tranh bi mat ket noi khi IP thay doi sau khi reset may:
:: set VONIKO_SERVER_URL=http://localhost:3001
set VONIKO_SERVER_URL=http://10.4.1.31:3001
set DMP_STATION_PORT=8766

:: ─── DMP (live discharge data) ───────────────────────────────
set DMP_DATA_DIR=C:\DMP\Data
:: set DMP_TEMPLATES_DIR=./dmp_templates
:: set DMP_PERF_TEMPLATES_DIR=./dmp_perf_templates

:: ─── DM2000 (historic discharge — Ohm) ───────────────────────
set DM2000_DATA_DIR=D:\DM2000\dmdatabase
:: set DM2000_TEMPLATES_DIR=./dm2000_templates
:: set DM2000_PERF_TEMPLATES_DIR=./dm2000_perf_templates

:: ─── DM3000 (historic discharge — mA) ────────────────────────
set DM3000_DATA_DIR=D:\DM3000\dmdatabase
:: set DM3000_TEMPLATES_DIR=./dm3000_templates
:: set DM3000_PERF_TEMPLATES_DIR=./dm3000_perf_templates

:: ============================================================
::  (Khong can sua gi them phia duoi)
:: ============================================================

title DMP Service - %DMP_STATION_NAME%

echo.
echo  +===================================================+
echo  ^|       VONIKO — KHOI DONG TRAM DU LIEU DMP        ^|
echo  ^|  Tram  : %DMP_STATION_NAME%
echo  ^|  Server: %VONIKO_SERVER_URL%
echo  ^|  Port  : %DMP_STATION_PORT%
echo  ^|  DMP   : %DMP_DATA_DIR%
echo  ^|  DM2000: %DM2000_DATA_DIR%
echo  ^|  DM3000: %DM3000_DATA_DIR%
echo  +===================================================+
echo.

:: Buoc 1: Kiem tra Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [LOI] Khong tim thay Python. Vui long cai dat Python 3.9+.
    pause
    exit /b 1
)
echo [OK] Python da san sang.

:: Buoc 2: Tao venv neu chua co
if not exist "%~dp0venv" (
    echo [INSTALL] Tao moi truong ao Python...
    python -m venv "%~dp0venv"
    if errorlevel 1 (
        echo [LOI] Khong tao duoc venv.
        pause
        exit /b 1
    )
    echo [OK] Moi truong ao da tao.
)

:: Buoc 3: Kich hoat venv va cai thu vien
call "%~dp0venv\Scripts\activate.bat"

echo [INSTALL] Kiem tra / cap nhat thu vien Python...
pip install -r "%~dp0requirements.txt" --quiet
if errorlevel 1 (
    echo [LOI] Cai dat thu vien that bai.
    pause
    exit /b 1
)
echo [OK] Thu vien Python da san sang.

:: Buoc 4: Mo Windows Firewall cho port DMP
echo [FW] Kiem tra firewall port %DMP_STATION_PORT%...
netsh advfirewall firewall show rule name="Voniko DMP %DMP_STATION_PORT%" >nul 2>&1
if errorlevel 1 (
    echo [FW] Them quy tac firewall...
    netsh advfirewall firewall add rule ^
        name="Voniko DMP %DMP_STATION_PORT%" ^
        dir=in action=allow protocol=TCP ^
        localport=%DMP_STATION_PORT% >nul
    netsh advfirewall firewall add rule ^
        name="Voniko DMP %DMP_STATION_PORT% OUT" ^
        dir=out action=allow protocol=TCP ^
        localport=%DMP_STATION_PORT% >nul
    echo [OK] Firewall da mo port %DMP_STATION_PORT%.
) else (
    echo [OK] Firewall port %DMP_STATION_PORT% da duoc mo truoc do.
)

:: Buoc 5: Khoi dong voi PM2
echo.
echo [PM2] Kiem tra PM2...
call pm2 --version >nul 2>&1
if errorlevel 1 (
    echo [INSTALL] Cai PM2...
    call npm install -g pm2
)

echo [PM2] Dung process cu neu co...
call pm2 delete dmp-service >nul 2>&1
call pm2 delete dmp-watchdog >nul 2>&1

echo [PM2] Khoi dong DMP Service...
call pm2 start "%~dp0venv\Scripts\pythonw.exe" ^
    --name "dmp-service" ^
    --restart-delay 3000 ^
    -- -m uvicorn dmp_service:app ^
    --host 0.0.0.0 ^
    --port %DMP_STATION_PORT% ^
    --app-dir "%~dp0"

echo [PM2] Khoi dong DMP Watchdog (tu dong khoi dong lai khi server bi treo)...
call pm2 start "%~dp0venv\Scripts\pythonw.exe" ^
    --name "dmp-watchdog" ^
    --restart-delay 5000 ^
    -- "%~dp0dmp_watchdog.py"

call pm2 save

:: Tao Task Scheduler
schtasks /query /tn "PM2-DMPService" >nul 2>&1
if errorlevel 1 (
    echo [TASK] Tao Windows Task tu dong khoi dong...
    schtasks /create /tn "PM2-DMPService" ^
        /tr "pm2 resurrect" ^
        /sc onlogon ^
        /rl highest ^
        /f >nul
    echo [OK] Task da tao.
) else (
    echo [OK] Task da ton tai.
)
call pm2 save

echo.
echo [OK] DMP Service va Watchdog dang chay voi PM2.
echo      Ten tram : %DMP_STATION_NAME%
echo      Server   : %VONIKO_SERVER_URL%
echo      DMP Dir  : %DMP_DATA_DIR%
echo      DM2000   : %DM2000_DATA_DIR%
echo      DM3000   : %DM3000_DATA_DIR%
echo.
echo  Lenh quan ly:
echo    pm2 logs dmp-service      (xem log service)
echo    pm2 logs dmp-watchdog     (xem log watchdog)
echo    pm2 restart dmp-service   (khoi dong lai service)
echo    pm2 stop dmp-service      (dung service)
echo    pm2 monit                 (giam sat)
echo.
pause
