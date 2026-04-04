@echo off
REM clear_approval.bat — Löscht die Genehmigungsdatei (.approval)
REM Setzt den Approval-Gate zurück

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set APPROVAL_FILE=%SCRIPT_DIR%.approval

echo.
echo =========================================================
echo  APPROVAL GATE: Genehmigung zurücksetzen
echo =========================================================
echo.

if exist "%APPROVAL_FILE%" (
    del "%APPROVAL_FILE%"
    echo ✅ Genehmigung gelöscht: %APPROVAL_FILE%
) else (
    echo ℹ️  Keine Genehmigung vorhanden.
)

echo.
echo =========================================================
pause
