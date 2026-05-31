@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

if not defined PORT set PORT=2012

:: ── Virtual environment ───────────────────────────────────────────────────
if not exist ".venv\" (
    echo [run] Creating virtual environment ^(.venv^)
    python -m venv .venv
    if errorlevel 1 (
        echo [run] ERROR: Failed to create virtual environment. Is Python installed?
        exit /b 1
    )
)

call .venv\Scripts\activate.bat

:: ── Dependencies ──────────────────────────────────────────────────────────
if /i not "%SKIP_PIP_INSTALL%"=="1" (
    echo [run] Installing/updating dependencies
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
)

:: ── Kill existing process on port ─────────────────────────────────────────
for /f "tokens=5" %%P in (
    'netstat -ano ^| findstr ":%PORT% " ^| findstr LISTENING 2^>nul'
) do (
    echo [run] Stopping existing process on port %PORT%: %%P
    taskkill /PID %%P /F >nul 2>&1
)

:: ── Launch ────────────────────────────────────────────────────────────────
echo [run] Starting app on http://localhost:%PORT%
python app.py
