@echo off
setlocal

REM Build Component Engineer EXE with icon + bundled logo for Windows startup use
REM Usage:
REM   1) Install deps: pip install -r packages.txt pyinstaller pillow
REM   2) (Optional) Create app.ico from logo.png: python make_icon.py
REM   3) Run: build_windows_exe.bat

set APP_NAME=ComponentEngineer
set MAIN_FILE=main_app.py
set ICON_FILE=app.ico

if not exist %MAIN_FILE% (
  echo ERROR: %MAIN_FILE% not found.
  exit /b 1
)

if not exist %ICON_FILE% (
  echo WARNING: %ICON_FILE% not found. Building without custom icon.
  set ICON_ARG=
) else (
  set ICON_ARG=--icon %ICON_FILE%
)

pyinstaller --noconfirm --clean --onefile --windowed ^
  --name %APP_NAME% ^
  %ICON_ARG% ^
  --add-data "logo.png;." ^
  --add-data "components.db;." ^
  %MAIN_FILE%

if %errorlevel% neq 0 (
  echo Build failed.
  exit /b %errorlevel%
)

echo.
echo Build successful: dist\%APP_NAME%.exe
echo.
