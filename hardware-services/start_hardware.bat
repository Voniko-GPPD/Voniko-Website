@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"

:: ============================================================
::  Cau hinh may tram — Chi can sua 2 dong duoi day
:: ============================================================

::  Ten hien thi cua tram nay (bat ky ten gi, khong dau cung duoc)
set STATION_NAME=Tram Test - Khong chon vao day
set VONIKO_SERVER_URL=http://10.4.1.31:3001
::http://10.4.1.11:3001

:: ============================================================
::  (Khong can sua gi them phia duoi)
:: ============================================================

title Battery Service - %STATION_NAME%

echo.
echo  +===================================================+
echo  ^|       VONIKO — KHOI DONG TRAM KIEM TRA PIN       ^|
echo  ^|  Tram  : %STATION_NAME%
echo  ^|  Server: %VONIKO_SERVER_URL%
echo  ^|  Port  : 8765
echo  +===================================================+
echo.

:: -------------------------------------------------------
:: Buoc 1: Kiem tra Python
:: -------------------------------------------------------
python --version >nul 2>&1
if errorlevel 1 (
    echo [LOI] Khong tim thay Python. Vui long cai dat Python 3.9+.
    pause
    exit /b 1
)
echo [OK] Python da san sang.

:: -------------------------------------------------------
:: Buoc 2: Tao venv neu chua co
:: -------------------------------------------------------
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

:: -------------------------------------------------------
:: Buoc 3: Kich hoat venv va cai thu vien
:: -------------------------------------------------------
call "%~dp0venv\Scripts\activate.bat"

echo [INSTALL] Kiem tra / cap nhat thu vien Python...
pip install -r "%~dp0requirements.txt" --quiet
if errorlevel 1 (
    echo [LOI] Cai dat thu vien that bai.
    pause
    exit /b 1
)
echo [OK] Thu vien Python da san sang.

:: -------------------------------------------------------
:: Buoc 4: Mo Windows Firewall cho port 8765
:: -------------------------------------------------------
echo [FW] Kiem tra firewall port 8765...
netsh advfirewall firewall show rule name="Voniko Battery 8765" >nul 2>&1
if errorlevel 1 (
    echo [FW] Them quy tac firewall cho port 8765...
    netsh advfirewall firewall add rule ^
        name="Voniko Battery 8765" ^
        dir=in ^
        action=allow ^
        protocol=TCP ^
        localport=8765 >nul
    echo [OK] Firewall da mo port 8765.
    netsh advfirewall firewall add rule ^
        name="Voniko Battery 8765 OUT" ^
        dir=out ^
        action=allow ^
        protocol=TCP ^
        localport=8765 >nul
) else (
    echo [OK] Firewall port 8765 da duoc mo truoc do.
)

:: -------------------------------------------------------
:: Buoc 5: Khoi dong voi PM2
:: -------------------------------------------------------
echo.
echo [PM2] Kiem tra PM2...
call pm2 --version >nul 2>&1
if errorlevel 1 (
    echo [INSTALL] Cai PM2...
    call npm install -g pm2
)

echo [PM2] Dung process cu neu co...
call pm2 delete battery-service >nul 2>&1

echo [PM2] Khoi dong Battery Service...
call pm2 start "%~dp0venv\Scripts\pythonw.exe" ^
    --name "battery-service" ^
    --restart-delay 3000 ^
    --max-restarts 10 ^
    -- -m uvicorn battery_service:app ^
    --host 0.0.0.0 ^
    --port 8765 ^
    --app-dir "%~dp0"

:: Luu PM2 startup
call pm2 save

:: Cai PM2 startup (tu dong bat khi Windows khoi dong)
:: Tao Task Scheduler tu dong chay PM2 khi Windows khoi dong
schtasks /query /tn "PM2-BatteryService" >nul 2>&1
if errorlevel 1 (
    echo [TASK] Tao Windows Task tu dong khoi dong...
    schtasks /create /tn "PM2-BatteryService" ^
        /tr "pm2 resurrect" ^
        /sc onlogon ^
        /rl highest ^
        /f >nul
    echo [OK] Task da tao. PM2 se tu khoi dong sau khi dang nhap.
) else (
    echo [OK] Task da ton tai.
)
:: Luu PM2 startup
call pm2 save

echo.
echo [OK] Battery Service dang chay voi PM2.
echo      Ten tram : %STATION_NAME%
echo      Server   : %VONIKO_SERVER_URL%
echo.
echo  Lenh quan ly:
echo    pm2 logs battery-service    (xem log)
echo    pm2 restart battery-service (khoi dong lai)
echo    pm2 stop battery-service    (dung)
echo    pm2 monit                   (giam sat)
echo.
pause
