#!/bin/bash

# Sri Lanka Weather Analytics - Deliverable Packaging Script
# This script creates a ZIP archive containing all project deliverables
# excluding datasets, build artifacts, and virtual environments

# Set variables
PROJECT_NAME="sri-lanka-weather-analytics"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="deliverables"
ZIP_NAME="${PROJECT_NAME}_${TIMESTAMP}.zip"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Create temporary staging directory
STAGING_DIR=$(mktemp -d)
PACKAGE_DIR="$STAGING_DIR/$PROJECT_NAME"
mkdir -p "$PACKAGE_DIR"

echo "Packaging Sri Lanka Weather Analytics deliverables..."
echo "=================================================="

# 1. Package MapReduce Java source files (no JARs)
echo "1. Packaging MapReduce Java source files..."
mkdir -p "$PACKAGE_DIR/mapreduce"
cp -r mapreduce/src "$PACKAGE_DIR/mapreduce/"
cp mapreduce/pom.xml "$PACKAGE_DIR/mapreduce/"
echo "   - Java source files copied"
echo "   - pom.xml copied"

# 2. Package Hive scripts
echo "2. Packaging Hive scripts..."
mkdir -p "$PACKAGE_DIR/hive"
cp hive/*.hql "$PACKAGE_DIR/hive/" 2>/dev/null || echo "   - No .hql files found"
echo "   - Hive scripts copied"

# 3. Package Pig scripts
echo "3. Packaging Pig scripts..."
mkdir -p "$PACKAGE_DIR/pig"
# Copy any .pig files if they exist
find pig -name "*.pig" -exec cp {} "$PACKAGE_DIR/pig/" \; 2>/dev/null
# Copy .gitkeep to preserve directory structure
cp pig/.gitkeep "$PACKAGE_DIR/pig/" 2>/dev/null
echo "   - Pig scripts copied (if any)"

# 4. Package Spark notebooks and Python scripts
echo "4. Packaging Spark notebooks and scripts..."
mkdir -p "$PACKAGE_DIR/spark"
# Copy Jupyter notebooks
cp spark/*.ipynb "$PACKAGE_DIR/spark/" 2>/dev/null
# Copy Python scripts
cp spark/*.py "$PACKAGE_DIR/spark/" 2>/dev/null
# Copy requirements.txt
cp spark/requirements.txt "$PACKAGE_DIR/spark/" 2>/dev/null
echo "   - Jupyter notebooks copied"
echo "   - Python scripts copied"
echo "   - requirements.txt copied"

# 5. Package Dashboard HTML/CSS/JS files
echo "5. Packaging Dashboard files..."
mkdir -p "$PACKAGE_DIR/dashboard"
cp -r dashboard/index.html "$PACKAGE_DIR/dashboard/"
cp -r dashboard/css "$PACKAGE_DIR/dashboard/"
cp -r dashboard/js "$PACKAGE_DIR/dashboard/"
mkdir -p "$PACKAGE_DIR/dashboard/assets/images"
cp dashboard/assets/images/.gitkeep "$PACKAGE_DIR/dashboard/assets/images/" 2>/dev/null
echo "   - HTML files copied"
echo "   - CSS files copied"
echo "   - JS files copied"

# 6. Package analysis scripts and results
echo "6. Packaging analysis scripts..."
mkdir -p "$PACKAGE_DIR/scripts"
cp scripts/*.py "$PACKAGE_DIR/scripts/" 2>/dev/null
cp scripts/*.json "$PACKAGE_DIR/scripts/" 2>/dev/null
cp scripts/*.md "$PACKAGE_DIR/scripts/" 2>/dev/null
echo "   - Analysis scripts copied"

# 7. Create README for the package
echo "7. Creating package README..."
cat > "$PACKAGE_DIR/README.md" << 'EOF'
# Sri Lanka Weather Analytics Platform

## Deliverable Contents

This package contains the complete source code and scripts for the Sri Lanka Weather Analytics Platform.

### Directory Structure

```
sri-lanka-weather-analytics/
├── mapreduce/           # Hadoop MapReduce Java source files
│   ├── src/             # Java source code
│   └── pom.xml          # Maven build configuration
├── hive/                # Hive query scripts
│   ├── create_tables.hql
│   ├── load_data.hql
│   ├── top_temperate_cities.hql
│   └── seasonal_evapotranspiration.hql
├── pig/                 # Apache Pig scripts
├── spark/               # Apache Spark notebooks and scripts
│   ├── *.ipynb          # Jupyter notebooks
│   ├── *.py             # Python scripts
│   └── requirements.txt # Python dependencies
├── dashboard/           # Visualization dashboard
│   ├── index.html       # Main dashboard page
│   ├── css/             # Stylesheets
│   └── js/              # JavaScript files
└── scripts/             # Analysis and utility scripts
```

### Requirements

- **MapReduce**: Java 8+, Maven 3.6+, Hadoop 3.x
- **Hive**: Apache Hive 3.x
- **Spark**: Apache Spark 3.x, Python 3.8+
- **Dashboard**: Modern web browser

### Building MapReduce Jobs

```bash
cd mapreduce
mvn clean package
```

### Running Spark Analysis

```bash
cd spark
pip install -r requirements.txt
# Run notebooks in Jupyter or execute Python scripts directly
```

### Viewing Dashboard

Open `dashboard/index.html` in a web browser.

## Note

This package does not include:
- Dataset files (weatherData.csv, locationData.csv)
- Compiled JAR files
- Python virtual environments
- Build artifacts
EOF
echo "   - README.md created"

# 8. Create the ZIP archive
echo "8. Creating ZIP archive..."
cd "$STAGING_DIR"
zip -r "$ZIP_NAME" "$PROJECT_NAME" -x "*.pyc" -x "*__pycache__*" -x "*.class"
mv "$ZIP_NAME" "$OLDPWD/$OUTPUT_DIR/"
cd "$OLDPWD"

# 9. Cleanup
rm -rf "$STAGING_DIR"

echo ""
echo "=================================================="
echo "Packaging complete!"
echo "Output: $OUTPUT_DIR/$ZIP_NAME"
echo ""

# List contents of the ZIP
echo "Archive contents:"
unzip -l "$OUTPUT_DIR/$ZIP_NAME"

echo ""
echo "Archive size: $(du -h "$OUTPUT_DIR/$ZIP_NAME" | cut -f1)"
