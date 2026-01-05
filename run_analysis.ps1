# ============================================================================
# Sri Lanka Weather Analytics - Windows PowerShell Runner
# ============================================================================
# This script runs all analytics components on Windows
# Prerequisites: Python 3.x, Java 11+, Maven installed and in PATH
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "       Sri Lanka Weather Analytics - Complete Analysis Runner" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Function to check if command exists
function Test-Command {
    param($Command)
    try {
        if (Get-Command $Command -ErrorAction Stop) {
            return $true
        }
    }
    catch {
        return $false
    }
}

# Check prerequisites
Write-Host "Checking prerequisites..." -ForegroundColor Yellow
Write-Host ""

$allPrereqsMet = $true

if (Test-Command python) {
    $pythonVersion = python --version
    Write-Host "[OK] Python found: $pythonVersion" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Python not found. Please install Python 3.x" -ForegroundColor Red
    $allPrereqsMet = $false
}

if (Test-Command java) {
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    Write-Host "[OK] Java found: $javaVersion" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Java not found. Please install Java 11+" -ForegroundColor Red
    $allPrereqsMet = $false
}

if (Test-Command mvn) {
    $mavenVersion = mvn -version | Select-Object -First 1
    Write-Host "[OK] Maven found: $mavenVersion" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Maven not found. Please install Apache Maven" -ForegroundColor Red
    $allPrereqsMet = $false
}

Write-Host ""

if (-not $allPrereqsMet) {
    Write-Host "Please install missing prerequisites and try again." -ForegroundColor Red
    Write-Host ""
    Write-Host "Installation guides:" -ForegroundColor Yellow
    Write-Host "  Python: https://www.python.org/downloads/" -ForegroundColor White
    Write-Host "  Java: https://adoptium.net/" -ForegroundColor White
    Write-Host "  Maven: https://maven.apache.org/download.cgi" -ForegroundColor White
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# ============================================================================
# Step 1: Generate Analysis Outputs
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "Step 1: Generating Analysis Outputs" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Running Python analysis script..." -ForegroundColor Yellow
python scripts/generate_outputs.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] Python analysis failed!" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "[SUCCESS] Analysis outputs generated!" -ForegroundColor Green
Write-Host ""

# ============================================================================
# Step 2: Build MapReduce Jobs
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "Step 2: Building MapReduce Jobs" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Compiling Java MapReduce code with Maven..." -ForegroundColor Yellow
Set-Location mapreduce

mvn clean package -DskipTests

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] Maven build failed!" -ForegroundColor Red
    Set-Location ..
    Read-Host "Press Enter to exit"
    exit 1
}

Set-Location ..

Write-Host ""
Write-Host "[SUCCESS] MapReduce jobs compiled!" -ForegroundColor Green
Write-Host "JAR file: mapreduce/target/weather-analytics-1.0-SNAPSHOT.jar" -ForegroundColor White
Write-Host ""

# ============================================================================
# Step 3: Run Spark Analysis (Optional)
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "Step 3: Spark Analysis (Optional)" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

$runSpark = Read-Host "Do you want to run Spark analysis? (y/n)"

if ($runSpark -eq "y" -or $runSpark -eq "Y") {
    Write-Host ""
    Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
    
    if (Test-Path "spark/venv") {
        Write-Host "Virtual environment already exists, activating..." -ForegroundColor Yellow
        & spark/venv/Scripts/Activate.ps1
    } else {
        Write-Host "Creating virtual environment..." -ForegroundColor Yellow
        python -m venv spark/venv
        & spark/venv/Scripts/Activate.ps1
        pip install -r spark/requirements.txt
    }
    
    Write-Host ""
    Write-Host "Running Spark analysis scripts..." -ForegroundColor Yellow
    
    python spark/spark_data_loader.py
    python spark/weekly_max_temperature_analysis.py
    python spark/shortwave_radiation_analysis.py
    python spark/ml_dataset_preparation.py
    
    deactivate
    
    Write-Host ""
    Write-Host "[SUCCESS] Spark analysis completed!" -ForegroundColor Green
} else {
    Write-Host "Skipping Spark analysis." -ForegroundColor Yellow
}

Write-Host ""

# ============================================================================
# Step 4: Open Dashboard
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "Step 4: View Dashboard" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

$openDashboard = Read-Host "Do you want to open the dashboard in your browser? (y/n)"

if ($openDashboard -eq "y" -or $openDashboard -eq "Y") {
    $dashboardPath = Join-Path $PSScriptRoot "dashboard/index.html"
    Write-Host "Opening dashboard: $dashboardPath" -ForegroundColor Yellow
    Start-Process $dashboardPath
}

Write-Host ""

# ============================================================================
# Summary
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "                          Analysis Complete!" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Generated files:" -ForegroundColor Yellow
Write-Host "  - MapReduce JAR: mapreduce/target/weather-analytics-1.0-SNAPSHOT.jar" -ForegroundColor White
Write-Host "  - Dashboard: dashboard/index.html" -ForegroundColor White
Write-Host "  - Documentation: docs/" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Review the analysis outputs above" -ForegroundColor White
Write-Host "  2. Open dashboard/index.html in your browser" -ForegroundColor White
Write-Host "  3. Check docs/ for detailed documentation" -ForegroundColor White
Write-Host ""
Write-Host "For Hadoop/Hive deployment:" -ForegroundColor Yellow
Write-Host "  - Upload JAR to Hadoop cluster" -ForegroundColor White
Write-Host "  - Run Hive queries from hive/ directory" -ForegroundColor White
Write-Host "  - See docs/TASK2_MAPREDUCE_HIVE_ANALYSIS.md for details" -ForegroundColor White
Write-Host ""

Read-Host "Press Enter to exit"
