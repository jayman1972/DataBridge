@echo off
REM Data Bridge Startup Script (Using ngrok)
REM Run from projects\DataBridge - unified service for market-dashboard and wealth-scope-ui

echo ========================================
echo Data Bridge Service Startup (Ngrok)
echo ========================================
echo.
echo This service provides:
echo   - /health - Health check
echo   - /bloomberg-update - Bloomberg data fetch
echo   - /sggg/portfolio - SGGG/PSC portfolio (requires OpenVPN + DSN=PSC_VIEWER)
echo   - /economic-calendar - Economic calendar
echo.

REM Change to the script directory (projects\DataBridge)
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if required packages are installed (pyodbc optional - only needed for SGGG)
echo Checking Python dependencies...
python -c "import flask, flask_cors, supabase, blpapi" >nul 2>&1
if errorlevel 1 (
    echo.
    echo WARNING: Some Python packages may be missing
    echo Installing from requirements.txt...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install packages
        pause
        exit /b 1
    )
)

REM Check for bloomberg-service.env (DataBridge, market-dashboard, or wealth-scope root)
set "ENV_FILE=%~dp0bloomberg-service.env"
set "PARENT_ENV=%~dp0..\bloomberg-service.env"
set "MARKET_ENV=%~dp0..\market-dashboard\bloomberg-service.env"
if not exist "%ENV_FILE%" (
    if exist "%MARKET_ENV%" (
        echo Using config from: market-dashboard\bloomberg-service.env
    ) else if exist "%PARENT_ENV%" (
        echo Using config from: projects\bloomberg-service.env
    ) else (
        if exist "%~dp0bloomberg-service.env.example" (
            echo.
            echo Creating bloomberg-service.env from example...
            copy "%~dp0bloomberg-service.env.example" "%~dp0bloomberg-service.env" >nul
            echo.
            echo IMPORTANT: Edit bloomberg-service.env and add your SUPABASE_SERVICE_ROLE_KEY
            echo Get it from: Supabase Dashboard -^> Project Settings -^> API -^> service_role key
            echo Project: https://aphjduxfgsrqswonmgyb.supabase.co
            echo.
            pause
            exit /b 1
        ) else (
            echo ERROR: bloomberg-service.env not found. Create it in market-dashboard\ or data-bridge\
            echo   SUPABASE_URL=https://aphjduxfgsrqswonmgyb.supabase.co
            echo   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
            echo.
            pause
            exit /b 1
        )
    )
) else (
    findstr /C:"your_service_role_key_here" "%ENV_FILE%" >nul 2>&1
    if not errorlevel 1 (
        REM data-bridge has placeholder - check if parent has valid config
        if exist "%MARKET_ENV%" (
            findstr /C:"your_service_role_key_here" "%MARKET_ENV%" >nul 2>&1
            if errorlevel 1 (
                echo Using config from: market-dashboard\bloomberg-service.env
                goto :env_ok
            )
        )
        if exist "%PARENT_ENV%" (
            findstr /C:"your_service_role_key_here" "%PARENT_ENV%" >nul 2>&1
            if errorlevel 1 (
                echo Using config from: projects\bloomberg-service.env
                goto :env_ok
            )
        )
        goto :env_error
    )
)
goto :env_ok
:env_error
echo ERROR: bloomberg-service.env still has placeholder key. Edit it and add your real SUPABASE_SERVICE_ROLE_KEY.
echo Get it from: Supabase Dashboard -^> Project Settings -^> API -^> service_role key
echo.
pause
exit /b 1
:env_ok

set PORT=5000

echo.
echo Configuration:
echo   Port: %PORT%
echo   Supabase config: bloomberg-service.env
echo.
echo SGGG requirements: OpenVPN connected, ODBC DSN=PSC_VIEWER, pyodbc installed
echo ========================================
echo.

REM Check if Data Bridge is already running (idempotent)
set "BRIDGE_PID="
for /f "tokens=1-5" %%A in ('netstat -ano ^| findstr /R /C":%PORT% " ^| findstr LISTENING') do (
    set "BRIDGE_PID=%%E"
)

if defined BRIDGE_PID (
    echo Detected existing Data Bridge on port %PORT% (PID %BRIDGE_PID%)
    echo Skipping launch of a second service.
) else (
    echo Starting Data Bridge service...
    set DATA_BRIDGE_DEBUG=1
    start "Data Bridge Service" cmd /k "cd /d %~dp0 && set DATA_BRIDGE_DEBUG=1 && python data_bridge.py"
    timeout /t 3 /nobreak >nul
)

REM Check for ngrok (local ngrok.exe or in PATH)
set "NGROK_CMD=%~dp0ngrok.exe"
if not exist "%NGROK_CMD%" (
    where ngrok >nul 2>&1
    if errorlevel 1 (
        echo.
        echo WARNING: ngrok not found. Either:
        echo   1. Copy ngrok.exe to: %~dp0
        echo   2. Install ngrok and add to PATH: https://ngrok.com/download
        echo   3. Run manually in another terminal: ngrok http 5000
        echo.
        pause
        exit /b 0
    )
    set "NGROK_CMD=ngrok"
)

echo Starting ngrok tunnel...
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "start-tunnel-ngrok.ps1" -NgrokPath "%NGROK_CMD%"
pause
