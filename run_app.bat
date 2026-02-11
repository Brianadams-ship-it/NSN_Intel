@echo off
setlocal
cd /d "%~dp0"

echo ===============================
echo NSN Intelligence Suite - Web App
echo ===============================

if not exist .venv_app (
  echo Creating virtual environment...
  py -m venv .venv_app
)
call .venv_app\Scripts\activate

if not exist .venv_app\.deps_installed (
  echo Installing dependencies...
  python -m pip install --upgrade pip
  pip install -r requirements_app.txt
  echo done > .venv_app\.deps_installed
) else (
  echo Dependencies already installed.
)

echo.
echo Starting NSN Intelligence Suite...
echo Open http://localhost:5000 in your browser
echo Press Ctrl+C to stop the server.
echo.
python app.py
pause
