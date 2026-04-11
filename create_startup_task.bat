@echo off
setlocal

REM Create a startup task so app starts when Windows logs in.
REM Run this after EXE build:
REM   create_startup_task.bat "C:\path\to\dist\ComponentEngineer.exe"

set EXE_PATH=%~1
if "%EXE_PATH%"=="" (
  echo Usage: create_startup_task.bat "C:\full\path\to\ComponentEngineer.exe"
  exit /b 1
)

if not exist "%EXE_PATH%" (
  echo ERROR: EXE not found: %EXE_PATH%
  exit /b 1
)

set TASK_NAME=ComponentEngineerAutoStart

schtasks /Create /F /SC ONLOGON /RL HIGHEST /TN "%TASK_NAME%" /TR "\"%EXE_PATH%\""
if %errorlevel% neq 0 (
  echo Failed to create task.
  exit /b %errorlevel%
)

echo Startup task created: %TASK_NAME%
echo It will run at user logon.
