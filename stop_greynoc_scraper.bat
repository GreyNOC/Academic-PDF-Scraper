@echo off
cd /d "%~dp0"
echo STOP > STOP_GREYNOC_SCRAPER.flag
echo Stop signal created. The scraper will stop after the current check/download finishes.
pause
