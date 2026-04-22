@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"

:: ============================================================
::  Cau hinh may tram DMP — Chi can sua cac dong duoi day
:: ============================================================

set DMP_STATION_NAME=DMP1
set VONIKO_SERVER_URL=http://10.4.1.31:3001
set DMP_DATA_DIR=C:\DMP\Data
set DMP_STATION_PORT=8766

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
echo  ^|  Data  : %DMP_DATA_DIR%
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
if not exist "%~dp0dmp-services\venv" (
    echo [INSTALL] Tao moi truong ao Python...
    python -m venv "%~dp0dmp-services\venv"
    if errorlevel 1 (
        echo [LOI] Khong tao duoc venv.
        pause
        exit /b 1
    )
    echo [OK] Moi truong ao da tao.
)

:: Buoc 3: Kich hoat venv va cai thu vien
call "%~dp0dmp-services\venv\Scripts\activate.bat"

echo [INSTALL] Kiem tra / cap nhat thu vien Python...
pip install -r "%~dp0dmp-services\requirements.txt" --quiet
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

echo [PM2] Khoi dong DMP Service...
call pm2 start "%~dp0dmp-services\venv\Scripts\pythonw.exe" ^
    --name "dmp-service" ^
    --restart-delay 3000 ^
    --max-restarts 10 ^
    -- -m uvicorn dmp_service:app ^
    --host 0.0.0.0 ^
    --port %DMP_STATION_PORT% ^
    --app-dir "%~dp0dmp-services"

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
echo [OK] DMP Service dang chay voi PM2.
echo      Ten tram : %DMP_STATION_NAME%
echo      Server   : %VONIKO_SERVER_URL%
echo      Data Dir : %DMP_DATA_DIR%
echo.
echo  Lenh quan ly:
echo    pm2 logs dmp-service      (xem log)
echo    pm2 restart dmp-service   (khoi dong lai)
echo    pm2 stop dmp-service      (dung)
echo    pm2 monit                 (giam sat)
echo.
pause
