@echo off
title Installing Missing Libraries
color 0B

:: 1. Ensure we are in the current folder
cd /d "%~dp0"

:: 2. Create/Activate the Virtual Environment
echo Setting up Python environment...
python -m venv .venv
call .venv\Scripts\activate

:: 3. Install the specific libraries needed
echo ---------------------------------------------------
echo      DOWNLOADING LIBRARIES (This takes a moment)
echo ---------------------------------------------------
pip install paramiko pandas python-dotenv lxml

echo.
echo ---------------------------------------------------
echo      INSTALLATION COMPLETE!
echo ---------------------------------------------------
pause
