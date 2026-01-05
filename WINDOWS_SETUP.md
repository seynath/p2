# Sri Lanka Weather Analytics - Windows Setup Guide

This guide will help you run the complete weather analytics platform on Windows.

## Prerequisites

Before running the analysis, you need to install:

### 1. Python 3.x
- Download from: https://www.python.org/downloads/
- **Important**: During installation, check "Add Python to PATH"
- Verify installation: Open Command Prompt and run `python --version`

### 2. Java 11 or higher
- Download from: https://adoptium.net/
- Choose "Windows x64" installer
- Verify installation: Open Command Prompt and run `java -version`

### 3. Apache Maven
- Download from: https://maven.apache.org/download.cgi
- Extract to `C:\Program Files\Maven`
- Add to PATH:
  1. Search "Environment Variables" in Windows
  2. Click "Environment Variables"
  3. Under "System variables", find "Path" and click "Edit"
  4. Click "New" and add: `C:\Program Files\Maven\bin`
  5. Click "OK" on all windows
- Verify installation: Open **new** Command Prompt and run `mvn -version`

## Quick Start

### Option 1: PowerShell (Recommended)

1. Open PowerShell as Administrator
2. Navigate to project directory:
   ```powershell
   cd path\to\sri-lanka-weather-analytics
   ```
3. Run the script:
   ```powershell
   .\run_analysis.ps1
   ```

**Note**: If you get an execution policy error, run:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Option 2: Batch File

1. Open Command Prompt
2. Navigate to project directory:
   ```cmd
   cd path\to\sri-lanka-weather-analytics
   ```
3. Run the script:
   ```cmd
   run_analysis.bat
   ```

## What the Script Does

The script will automatically:

1. ✅ Check if all prerequisites are installed
2. 📊 Generate analysis outputs from the weather data
3. 🔨 Compile MapReduce Java code with Maven
4. ⚡ (Optional) Run Spark analysis scripts
5. 🌐 (Optional) Open the dashboard in your browser

## Manual Steps (If Automated Script Fails)

### Step 1: Generate Analysis Outputs
```cmd
python scripts\generate_outputs.py
```

### Step 2: Build MapReduce Jobs
```cmd
cd mapreduce
mvn clean package -DskipTests
cd ..
```

### Step 3: Run Spark Analysis (Optional)
```cmd
cd spark
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python spark_data_loader.py
python weekly_max_temperature_analysis.py
python shortwave_radiation_analysis.py
python ml_dataset_preparation.py
deactivate
cd ..
```

### Step 4: View Dashboard
Open `dashboard\index.html` in your web browser (Chrome, Firefox, or Edge)

## Project Structure

```
sri-lanka-weather-analytics/
├── dataset/                    # Weather data CSV files
├── mapreduce/                  # Hadoop MapReduce Java code
│   ├── src/                    # Source code
│   ├── target/                 # Compiled JAR (after build)
│   └── pom.xml                 # Maven configuration
├── hive/                       # Hive SQL queries
├── spark/                      # Spark Python scripts
├── dashboard/                  # Web dashboard
│   ├── index.html             # Main dashboard page
│   ├── css/                   # Stylesheets
│   └── js/                    # JavaScript charts
├── docs/                       # Documentation
├── scripts/                    # Analysis scripts
├── run_analysis.ps1           # PowerShell runner
├── run_analysis.bat           # Batch file runner
└── WINDOWS_SETUP.md           # This file
```

## Troubleshooting

### Python not found
- Make sure Python is installed and added to PATH
- Restart Command Prompt/PowerShell after installation
- Try `py` instead of `python`

### Java not found
- Make sure Java is installed
- Check JAVA_HOME environment variable is set
- Restart Command Prompt/PowerShell after installation

### Maven not found
- Make sure Maven bin directory is in PATH
- Restart Command Prompt/PowerShell after adding to PATH
- Verify with: `mvn -version`

### Maven build fails
- Make sure you have internet connection (Maven downloads dependencies)
- Try running: `mvn clean install -U`
- Check Java version is 11 or higher

### PowerShell execution policy error
Run PowerShell as Administrator and execute:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Spark analysis fails
- Make sure Python virtual environment is activated
- Install dependencies: `pip install -r spark\requirements.txt`
- Check Python version is 3.8 or higher

## Output Files

After running the analysis, you'll find:

- **MapReduce JAR**: `mapreduce\target\weather-analytics-1.0-SNAPSHOT.jar`
- **Dashboard**: `dashboard\index.html` (open in browser)
- **Documentation**: `docs\` folder
- **Analysis Results**: Console output from `generate_outputs.py`

## Next Steps

1. **View Dashboard**: Open `dashboard\index.html` in your browser
2. **Review Documentation**: Check `docs\` folder for detailed analysis
3. **Deploy to Hadoop**: Upload JAR to Hadoop cluster (see deployment guide)
4. **Run Hive Queries**: Execute queries from `hive\` directory on Hive

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review documentation in `docs\` folder
3. Verify all prerequisites are correctly installed

## System Requirements

- **OS**: Windows 10 or higher
- **RAM**: 4GB minimum (8GB recommended for Spark)
- **Disk Space**: 2GB free space
- **Internet**: Required for Maven dependency downloads
