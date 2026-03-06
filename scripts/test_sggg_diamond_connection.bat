@echo off
cd /d "%~dp0.."
python scripts\test_sggg_diamond_connection.py
if errorlevel 1 exit /b 1
exit /b 0
