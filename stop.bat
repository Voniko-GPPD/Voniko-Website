@echo off
chcp 65001 >nul
title Voniko-Web --- STOP ALL

echo.
echo  +==========================================+
echo  ^|  VONIKO-WEB --- STOPPING ALL SERVICES  ^|
echo  +==========================================+
echo.

echo [1/2] Stopping PM2 processes...
call pm2 stop all 2>nul
call pm2 kill 2>nul
echo  [OK] PM2 stopped.
echo.

echo [2/2] Releasing ports 3000, 3001, 8765, 8001...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3000 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3001 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8001 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo  [OK] Ports released.
echo.

echo  [DONE] All services stopped.
echo.
pause
