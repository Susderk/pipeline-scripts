@echo off
REM approve_for_prod.bat — Genehmigt Staging-Testlauf für Produktionscode
REM Schreibt Genehmigung in .approval mit Timestamp

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set APPROVAL_FILE=%SCRIPT_DIR%.approval

echo.
echo =========================================================
echo  APPROVAL GATE: Freigabe für Produktionscode (Step 10-11)
echo =========================================================
echo.
echo Genehmigung wird erteilt...
echo.

REM Aktuelles Datum und Zeit in ISO 8601-Format
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c-%%a-%%b)
for /f "tokens=1-2 delims=/:" %%a in ('time /t') do (set mytime=%%a:%%b)

echo %mydate% %mytime% Approval granted by user > "%APPROVAL_FILE%"

echo ✅ Genehmigung erteilt: %mydate% %mytime%
echo.
echo Datei: %APPROVAL_FILE%
echo.
echo Starten Sie jetzt den Workflow erneut:
echo   python Start_Scripts.py --staging
echo.
echo =========================================================
pause
