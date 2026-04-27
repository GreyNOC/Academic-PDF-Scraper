@echo off
setlocal
cd /d "%~dp0"

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

echo Installing build requirements...
python -m pip install --upgrade pip
pip install -r requirements.txt

echo Cleaning old build output...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist GreyNOCPDFScraper.spec del GreyNOCPDFScraper.spec

echo Building GreyNOCPDFScraper.exe...
pyinstaller ^
  --noconfirm ^
  --windowed ^
  --name GreyNOCPDFScraper ^
  --add-data ".env.example;." ^
  --add-data "keyword_profiles.json;." ^
  greynoc_pdf_scraper_gui.py

if errorlevel 1 (
    echo Build failed.
    pause
    exit /b 1
)

echo.
echo EXE created:
echo %CD%\dist\GreyNOCPDFScraper\GreyNOCPDFScraper.exe
echo.
pause
