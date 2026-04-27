@echo off
setlocal
cd /d "%~dp0"

if not exist "dist\GreyNOCPDFScraper\GreyNOCPDFScraper.exe" (
    echo EXE not found. Running compile_exe.bat first...
    call compile_exe.bat
)

if not exist "dist\GreyNOCPDFScraper\GreyNOCPDFScraper.exe" (
    echo EXE still not found. Installer build stopped.
    pause
    exit /b 1
)

set ISCC=
if exist "%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe" set ISCC=%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe
if exist "%ProgramFiles%\Inno Setup 6\ISCC.exe" set ISCC=%ProgramFiles%\Inno Setup 6\ISCC.exe

if "%ISCC%"=="" (
    echo Inno Setup 6 was not found.
    echo Install Inno Setup 6, then run this BAT again.
    pause
    exit /b 1
)

echo Building Windows installer...
"%ISCC%" "installer\GreyNOCPDFScraper.iss"

if errorlevel 1 (
    echo Installer build failed.
    pause
    exit /b 1
)

echo.
echo Installer created:
echo %CD%\installer\output\GreyNOCPDFScraperSetup.exe
echo.
pause
