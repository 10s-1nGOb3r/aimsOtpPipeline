@echo off
title OTP Pipeline Runner
:: Navigate to the folder where the bat file is saved
cd /d "%~dp0"

echo ---------------------------------------
echo  STEP 1: Checking Python Environment
echo ---------------------------------------

:: Try to find where python is on THIS specific machine
set PYTHON_EXE=python
where %PYTHON_EXE% >nul 2>nul
if %errorlevel% neq 0 (
    echo Python not found in PATH. Checking local AppData...
    :: This %LocalAppData% variable works on ANY Windows user account
    set PYTHON_EXE="%LocalAppData%\Programs\Python\Python314\python.exe"
)

:: Verify if we found a working Python
%PYTHON_EXE% --version >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not found at %PYTHON_EXE%
    echo Please install Python 3.x and check 'Add to PATH'.
    pause
    exit /b
)

echo Using: %PYTHON_EXE%

echo.
echo ---------------------------------------
echo  STEP 2: Checking Requirements
echo ---------------------------------------
%PYTHON_EXE% -m pip install pandas numpy

echo.
echo ---------------------------------------
echo  STEP 3: Running OTP Calculation
echo ---------------------------------------
%PYTHON_EXE% otp_calculation.py

echo.
echo ---------------------------------------
echo  PROCESS FINISHED
echo ---------------------------------------
pause
