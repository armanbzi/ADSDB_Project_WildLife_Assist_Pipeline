@echo off
REM Setup script for WildLife Data Management Pipeline
REM For Windows environments

echo ==========================================
echo WildLife Pipeline Setup Script (Windows)
echo ==========================================

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed. Please install Python 3.9 or higher.
    pause
    exit /b 1
)

echo ✓ Python found

REM Install dependencies
echo Installing Python dependencies...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo Error: Failed to install dependencies
    pause
    exit /b 1
)

echo ✓ Dependencies installed

REM Create necessary directories
echo Creating necessary directories...
if not exist logs mkdir logs
if not exist data mkdir data
if not exist temp mkdir temp

echo ✓ Directories created

REM Set up environment variables template
echo Creating environment template...
(
echo # WildLife Pipeline Environment Variables
echo # Copy this file to .env and fill in your values
echo.
echo # MinIO Configuration
echo MINIO_ENDPOINT=localhost:9000
echo MINIO_ACCESS_KEY=admin
echo MINIO_SECRET_KEY=password123
echo.
echo # SonarQube Configuration ^(optional^)
echo SONAR_HOST_URL=http://localhost:9002
echo SONAR_LOGIN=your_sonar_token_here
echo.
echo # HuggingFace Configuration ^(optional^)
echo HUGGINGFACE_API_TOKEN=your_hf_token_here
) > .env.template

echo ✓ Environment template created

REM Create Docker Compose file for services
echo Creating Docker Compose configuration...
(
echo version: '3.8'
echo.
echo services:
echo   minio:
echo     image: minio/minio:latest
echo     ports:
echo       - "9000:9000"
echo       - "9001:9001"
echo     environment:
echo       MINIO_ROOT_USER: admin
echo       MINIO_ROOT_PASSWORD: password123
echo     command: server /data --console-address ":9001"
echo     volumes:
echo       - minio_data:/data
echo     healthcheck:
echo       test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
echo       interval: 30s
echo       timeout: 20s
echo       retries: 3
echo.
echo   sonarqube:
echo     image: sonarqube:latest
echo     ports:
echo       - "9002:9000"
echo     environment:
echo       SONAR_ES_BOOTSTRAP_CHECKS_DISABLE: true
echo     volumes:
echo       - sonarqube_data:/opt/sonarqube/data
echo       - sonarqube_logs:/opt/sonarqube/logs
echo       - sonarqube_extensions:/opt/sonarqube/extensions
echo     healthcheck:
echo       test: ["CMD", "curl", "-f", "http://localhost:9000/api/system/status"]
echo       interval: 30s
echo       timeout: 10s
echo       retries: 5
echo.
echo volumes:
echo   minio_data:
echo   sonarqube_data:
echo   sonarqube_logs:
echo   sonarqube_extensions:
) > docker-compose.yml

echo ✓ Docker Compose configuration created

REM Create run script
echo Creating run script...
(
echo @echo off
echo REM Run WildLife Pipeline
echo.
echo REM Check if .env file exists
echo if not exist .env ^(
echo     echo Warning: .env file not found. Using default configuration.
echo     echo Copy .env.template to .env and configure your settings.
echo ^)
echo.
echo REM Load environment variables
echo if exist .env ^(
echo     for /f "usebackq tokens=1,2 delims==" %%a in ^(".env"^) do ^(
echo         if not "%%a"=="" if not "%%a:~0,1%"=="#" set "%%a=%%b"
echo     ^)
echo ^)
echo.
echo REM Run the pipeline
echo echo Starting WildLife Pipeline...
echo python orchestrate.py %*
) > run_pipeline.bat

echo ✓ Run script created

REM Create CI/CD run script
echo Creating CI/CD run script...
(
echo @echo off
echo REM Run WildLife Pipeline in CI/CD mode
echo.
echo REM Load environment variables
echo if exist .env ^(
echo     for /f "usebackq tokens=1,2 delims==" %%a in ^(".env"^) do ^(
echo         if not "%%a"=="" if not "%%a:~0,1%"=="#" set "%%a=%%b"
echo     ^)
echo ^)
echo.
echo REM Set default environment variables if not set
echo if "%MINIO_ENDPOINT%"=="" set MINIO_ENDPOINT=localhost:9000
echo if "%MINIO_ACCESS_KEY%"=="" set MINIO_ACCESS_KEY=admin
echo if "%MINIO_SECRET_KEY%"=="" set MINIO_SECRET_KEY=password123
echo.
echo REM Run in non-interactive mode
echo echo Starting WildLife Pipeline in CI/CD mode...
echo python orchestrate.py --non-interactive --choice %PIPELINE_CHOICE%
) > run_cicd.bat

echo ✓ CI/CD run script created

REM Create .gitignore
echo Creating .gitignore...
(
echo # Environment files
echo .env
echo .env.local
echo .env.*.local
echo.
echo # Logs
echo *.log
echo logs/
echo error.log
echo pipeline.log
echo.
echo # Configuration files
echo minio_config.json
echo sonar-project.properties
echo.
echo # Data directories
echo data/
echo temp/
echo *.csv
echo *.json
echo.
echo # Python
echo __pycache__/
echo *.py[cod]
echo *$py.class
echo *.so
echo .Python
echo build/
echo develop-eggs/
echo dist/
echo downloads/
echo eggs/
echo .eggs/
echo lib/
echo lib64/
echo parts/
echo sdist/
echo var/
echo wheels/
echo *.egg-info/
echo .installed.cfg
echo *.egg
echo.
echo # Virtual environments
echo venv/
echo env/
echo ENV/
echo.
echo # IDE
echo .vscode/
echo .idea/
echo *.swp
echo *.swo
echo.
echo # OS
echo .DS_Store
echo Thumbs.db
echo.
echo # ChromaDB
echo exploitation_db/
echo *.sqlite3
echo *.bin
echo *.pickle
) > .gitignore

echo ✓ .gitignore created

echo.
echo ==========================================
echo Setup completed successfully!
echo ==========================================
echo.
echo Next steps:
echo 1. Copy .env.template to .env and configure your settings:
echo    copy .env.template .env
echo.
echo 2. Start required services ^(optional^):
echo    docker-compose up -d
echo.
echo 3. Run the pipeline:
echo    run_pipeline.bat
echo.
echo 4. For CI/CD environments:
echo    run_cicd.bat
echo.
echo 5. For manual testing:
echo    python orchestrate.py
echo.
echo Required services:
echo - MinIO ^(for data storage^)
echo - SonarQube ^(for code quality analysis, optional^)
echo.
echo Environment variables can be set in:
echo - .env file ^(for local development^)
echo - System environment variables ^(for CI/CD^)
echo.
pause
