@echo off
REM ============================================================================
REM Start_Evening.bat — Launcher fuer Vorabend-GUI der DPS-Pipeline.
REM Oeffnet Start_Evening_GUI.py mit dem Python aus PATH.
REM
REM Wechselt zuerst ins Skript-Verzeichnis, damit relative Pfade
REM (config.evening.yaml, fixtures/, prompts/) funktionieren.
REM
REM Letzte Aenderung: 2026-04-26 (Indi, Pipeline-Split-Patch).
REM ============================================================================

cd /d "%~dp0"

"D:\WTF\OneDrive\Anwendungen\Python\python.exe" Start_Evening_GUI.py

REM Konsole offen halten, damit Ingo Fehler/Logs sehen kann.
pause
