#!/bin/bash
# Setup script for WildLife Data Management Pipeline
# For third-person environments and CI/CD

set -e  # Exit on any error

echo "=========================================="
echo "WildLife Pipeline Setup Script"
echo "=========================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3.9 or higher."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.9"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "Error: Python $REQUIRED_VERSION or higher is required. Found: $PYTHON_VERSION"
    exit 1
fi

echo "✓ Python $PYTHON_VERSION found"

# Install pip if not present
if ! command -v pip3 &> /dev/null; then
    echo "Installing pip..."
    python3 -m ensurepip --upgrade
fi

echo "✓ pip found"

# Install dependencies
echo "Installing Python dependencies..."
pip3 install --upgrade pip
pip3 install -r requirements.txt

echo "✓ Dependencies installed"

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p logs
mkdir -p data
mkdir -p temp

echo "✓ Directories created"

# Set up environment variables template
echo "Creating environment template..."
cat > .env.template << EOF
# WildLife Pipeline Environment Variables
# Copy this file to .env and fill in your values

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123

# SonarQube Configuration (optional)
SONAR_HOST_URL=http://localhost:9002
SONAR_LOGIN=your_sonar_token_here

# HuggingFace Configuration (optional)
HUGGINGFACE_API_TOKEN=your_hf_token_here
EOF

echo "✓ Environment template created"

# Create Docker Compose file for services
echo "Creating Docker Compose configuration..."
cat > docker-compose.yml << EOF
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password123
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

  sonarqube:
    image: sonarqube:latest
    ports:
      - "9002:9000"
    environment:
      SONAR_ES_BOOTSTRAP_CHECKS_DISABLE: true
    volumes:
      - sonarqube_data:/opt/sonarqube/data
      - sonarqube_logs:/opt/sonarqube/logs
      - sonarqube_extensions:/opt/sonarqube/extensions
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/api/system/status"]
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  minio_data:
  sonarqube_data:
  sonarqube_logs:
  sonarqube_extensions:
EOF

echo "✓ Docker Compose configuration created"

# Create run script
echo "Creating run script..."
cat > run_pipeline.sh << 'EOF'
#!/bin/bash
# Run WildLife Pipeline

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Warning: .env file not found. Using default configuration."
    echo "Copy .env.template to .env and configure your settings."
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Run the pipeline
echo "Starting WildLife Pipeline..."
python3 orchestrate.py "$@"
EOF

chmod +x run_pipeline.sh

echo "✓ Run script created"

# Create CI/CD run script
echo "Creating CI/CD run script..."
cat > run_cicd.sh << 'EOF'
#!/bin/bash
# Run WildLife Pipeline in CI/CD mode

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Set default environment variables if not set
export MINIO_ENDPOINT=${MINIO_ENDPOINT:-"localhost:9000"}
export MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-"admin"}
export MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-"password123"}

# Run in non-interactive mode
echo "Starting WildLife Pipeline in CI/CD mode..."
python3 orchestrate.py --non-interactive --choice ${PIPELINE_CHOICE:-1}
EOF

chmod +x run_cicd.sh

echo "✓ CI/CD run script created"

# Create .gitignore
echo "Creating .gitignore..."
cat > .gitignore << EOF
# Environment files
.env
.env.local
.env.*.local

# Logs
*.log
logs/
error.log
pipeline.log

# Configuration files
minio_config.json
sonar-project.properties

# Data directories
data/
temp/
*.csv
*.json

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# ChromaDB
exploitation_db/
*.sqlite3
*.bin
*.pickle
EOF

echo "✓ .gitignore created"

echo ""
echo "=========================================="
echo "Setup completed successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Copy .env.template to .env and configure your settings:"
echo "   cp .env.template .env"
echo ""
echo "2. Start required services (optional):"
echo "   docker-compose up -d"
echo ""
echo "3. Run the pipeline:"
echo "   ./run_pipeline.sh"
echo ""
echo "4. For CI/CD environments:"
echo "   ./run_cicd.sh"
echo ""
echo "5. For manual testing:"
echo "   python3 orchestrate.py"
echo ""
echo "Required services:"
echo "- MinIO (for data storage)"
echo "- SonarQube (for code quality analysis, optional)"
echo ""
echo "Environment variables can be set in:"
echo "- .env file (for local development)"
echo "- System environment variables (for CI/CD)"
echo ""
