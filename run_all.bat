@echo off
setlocal

:: Определяем путь к корню проекта
set PROJECT_ROOT=%~dp0

echo ====================================================
echo   ConstructMaterialAI Launcher
echo ====================================================

:: 1. Запуск Backend
echo [1/3] Запуск Backend сервера...
start "Backend Server" cmd /k "cd /d %PROJECT_ROOT%backend && go run cmd/service/main.go"

:: 2. Запуск ML / Data Service (FastAPI)
echo [2/3] Запуск ML / Data Service...
if exist "%PROJECT_ROOT%data-service\.venv\Scripts\activate.bat" (
    :: Если есть виртуальное окружение, используем его
    start "ML Data Service (FastAPI)" cmd /k "cd /d %PROJECT_ROOT%data-service && .venv\Scripts\activate.bat && uvicorn api.api:app --reload"
) else (
    :: Иначе пробуем просто через uvicorn
    start "ML Data Service (FastAPI)" cmd /k "cd /d %PROJECT_ROOT%data-service && uvicorn api.api:app --reload"
)

:: 3. Запуск Desktop App
echo [3/3] Запуск Desktop приложения...
start "Desktop App" cmd /k "cd /d %PROJECT_ROOT%desktop && go run ."

echo.
echo ====================================================
echo   Все сервисы запущены в отдельных окнах
echo ====================================================
pause
