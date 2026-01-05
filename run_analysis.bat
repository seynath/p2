@echo off
REM ============================================================================
REM Sri Lanka Weather Analytics - Windows Batch Runner
REM ============================================================================
REM This script runs all analytics components on Windows
REM Prerequisites: Python 3.x, Java 11+, Maven installed and in PATH
REM ============================================================================

setlocal enabledelayedexpansion

echo ============================================================================
echo        Sri Lanka Weather Analytics - Complete Analysis Runner
echo ============================================================================
echo.

REM Check prerequisites
echo Checking prerequisites...
echo.

set "PREREQS_MET=1"

REM Check Python
python --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=*" %%i in ('python --version') do set PYTHON_VERSION=%%i
    echo [OK] Python found: !PYTHON_VERSION!
) else (
    echo [ERROR] Python not found. Please install Python 3.x
    set "PREREQS_MET=0"
)

REM Check Java
java -version >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Java found
) else (
    echo [ERROR] Java not found. Please install Java 11+
    set "PREREQS_MET=0"
)

REM Check Maven
mvn -version >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Maven found
) else (
    echo [ERROR] Maven not found. Please install Apache Maven
    set "PREREQS_MET=0"
)

echo.

if "%PREREQS_MET%"=="0" (
    echo Please install missing prerequisites and try again.
    echo.
    echo Installation guides:
    echo   Python: https://www.python.org/downloads/
    echo   Java: https://adoptium.net/
    echo   Maven: https://maven.apache.org/download.cgi
    echo.
    pause
    exit /b 1
)

REM ============================================================================
REM Step 1: Generate Analysis Outputs
REM ============================================================================
echo ============================================================================
echo Step 1: Generating Analysis Outputs
echo ============================================================================
echo.

echo Running Python analysis script...
python scripts\generate_outputs.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Python analysis failed!
    pause
    exit /b 1
)

echo.
echo [SUCCESS] Analysis outputs generated!
echo.

REM ============================================================================
REM Step 2: Build MapReduce Jobs
REM ============================================================================
echo ============================================================================
echo Step 2: Building MapReduce Jobs
echo ============================================================================
echo.

echo Compiling Java MapReduce code with Maven...
cd mapreduce

call mvn clean package -DskipTests

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Maven build failed!
    cd ..
    pause
    exit /b 1
)

cd ..

echo.
echo [SUCCESS] MapReduce jobs compiled!
echo JAR file: mapreduce\target\weather-analytics-1.0-SNAPSHOT.jar
echo.

REM ============================================================================
REM Step 3: Run Spark Analysis (Optional)
REM ============================================================================
echo ============================================================================
echo Step 3: Spark Analysis (Optional)
echo ============================================================================
echo.

set /p RUN_SPARK="Do you want to run Spark analysis? (y/n): "

if /i "%RUN_SPARK%"=="y" (
    echo.
    echo Installing Python dependencies...
    
    if exist "spark\venv" (
        echo Virtual environment already exists, activating...
        call spark\venv\Scripts\activate.bat
    ) else (
        echo Creating virtual environment...
        python -m venv spark\venv
        call spark\venv\Scripts\activate.bat
        pip install -r spark\requirements.txt
    )
    
    echo.
    echo Running Spark analysis scripts...
    
    python spark\spark_data_loader.py
    python spark\weekly_max_temperature_analysis.py
    python spark\shortwave_radiation_analysis.py
    python spark\ml_dataset_preparation.py
    
    call deactivate
    
    echo.
    echo [SUCCESS] Spark analysis completed!
) else (
    echo Skipping Spark analysis.
)

echo.

REM ============================================================================
REM Step 4: Open Dashboard
REM ============================================================================
echo ============================================================================
echo Step 4: View Dashboard
echo ============================================================================
echo.

set /p OPEN_DASHBOARD="Do you want to open the dashboard in your browser? (y/n): "

if /i "%OPEN_DASHBOARD%"=="y" (
    echo Opening dashboard...
    start "" "dashboard\index.html"
)

echo.

REM ============================================================================
REM Summary
REM ============================================================================
echo ============================================================================
echo                           Analysis Complete!
echo ============================================================================
echo.
echo Generated files:
echo   - MapReduce JAR: mapreduce\target\weather-analytics-1.0-SNAPSHOT.jar
echo   - Dashboard: dashboard\index.html
echo   - Documentation: docs\
echo.
echo Next steps:
echo   1. Review the analysis outputs above
echo   2. Open dashboard\index.html in your browser
echo   3. Check docs\ for detailed documentation
echo.
echo For Hadoop/Hive deployment:
echo   - Upload JAR to Hadoop cluster
echo   - Run Hive queries from hive\ directory
echo   - See docs\TASK2_MAPREDUCE_HIVE_ANALYSIS.md for details
echo.

pause
