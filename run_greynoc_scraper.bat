@echo off
setlocal
cd /d "%~dp0"

if exist STOP_GREYNOC_SCRAPER.flag del STOP_GREYNOC_SCRAPER.flag

where python >nul 2>nul
if errorlevel 1 (
    echo Python was not found. Install Python 3.10+ and check "Add Python to PATH".
    pause
    exit /b 1
)

if not exist .venv (
    echo Creating virtual environment...
    python -m venv .venv
)

call .venv\Scripts\activate.bat

echo Installing requirements...
python -m pip install --upgrade pip
pip install -r requirements.txt

echo Starting GreyNOC PDF Scraper GUI...
python greynoc_pdf_scraper_gui.py

pause
