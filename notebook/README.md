# WildLife Data Management Pipeline

A comprehensive DataOps orchestration system for wildlife data processing, featuring continuous integration, quality control, and monitoring capabilities. This system processes wildlife images and metadata from the TreeOfLife-200M dataset through a multi-zone data management pipeline.

## 🚀 Features

- **Complete Pipeline Orchestration**: Automated execution of the entire data management backbone
- **User-Friendly Interface**: Interactive menu system for easy operation
- **Quality Control**: Built-in SonarQube code analysis and quality reporting
- **System Monitoring**: Real-time performance and resource monitoring during script execution
- **Error Handling**: Comprehensive error tracking and reporting
- **Flexible Execution**: Run complete workflows or individual scripts
- **MinIO Integration**: Distributed object storage for scalable data management
- **Vector Database**: ChromaDB integration for advanced similarity search and AI tasks

## 📋 Pipeline Workflow

### Data Processing Zones (Recommended Workflow):

1. **Temporal Landing** - Initial data ingestion from TreeOfLife-200M dataset
   - Streams wildlife data (snake families) from HuggingFace
   - User-configurable sample limits and species constraints
   - Stores raw images and metadata in MinIO temporal-zone

2. **Persistent Landing** - Data persistence layer
   - Ensures data durability and availability

3. **Formatted Metadata** - Metadata standardization and cleaning
   - Standardizes data formats and structures

4. **Formatted Images** - Image processing and standardization
   - Image format validation and optimization

5. **Trusted Metadata** - Data quality assurance for metadata
   - Validates and cleans metadata for reliability

6. **Trusted Images** - Image quality validation
   - Validates image integrity and quality

7. **Exploitation Metadata** - Advanced metadata processing
   - Creates vector embeddings for metadata using ChromaDB

8. **Exploitation Images** - Advanced image processing
   - Generates image embeddings using OpenCLIP

9. **Exploitation Multimodal** - Multimodal data integration
   - Combines text and image embeddings for multimodal search

### AI/ML Task Options:
- **Same Modality Search** - Similarity search within data types (text-to-text, image-to-image)
- **Multimodal Similarity** - Cross-modal similarity analysis (text-to-image, image-to-text)
- **Generative Task** - AI-powered data generation using OpenAI API

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd WildLife/notebook
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup MinIO** (if not already running):
   ```bash
   # Start MinIO server
   minio server /data --console-address ":9001"
   ```

4. **Setup HuggingFace** (for dataset access):
   ```bash
   # Login to HuggingFace for dataset access
   huggingface-cli login
   ```

## 🚀 Usage

### Quick Start
```bash
python orchestrate.py
```

### Configuration
The orchestrator will prompt you for:
- **MinIO Endpoint**: Your MinIO server address (e.g., `localhost:9000`)
- **Access Key**: MinIO access key
- **Secret Key**: MinIO secret key

### Menu Options

1. **Complete Data Pipeline (Store All Data)**: Execute the complete pipeline in the optimal sequence
2. **Individual Scripts (Choose specific scripts)**: Execute a single script from the pipeline or AI/ML tasks
3. **Quality Control & Code Analysis**: Analyze code quality using SonarQube and generate reports
4. **View Pipeline Status**: Check current pipeline execution status and monitoring data
5. **Exit**: Close the application

### Temporal Landing Configuration
When running Temporal Landing, you'll be prompted for:
- **MAX_SAMPLES**: Maximum number of samples to process from the 200M dataset
- **MAX_PER_SPECIES**: Maximum number of images per species
- **MAX_SPECIES_PER_FAMILY**: Maximum number of species per family

## 📊 Quality Control

The system includes comprehensive quality control features:

- **SonarQube Integration**: Automated detection of code quality issues, security vulnerabilities, and technical debt
- **Performance Monitoring**: Real-time system resource monitoring (CPU, RAM, DISK) during script execution
- **Error Tracking**: Comprehensive error logging and reporting
- **Code Analysis Reports**: Detailed analysis results with severity categorization

## 🔧 Development

### Project Structure
```
WildLife/notebook/
├── orchestrate.py              # Main orchestration script
├── requirements.txt            # Python dependencies
├── README.md                  # This file
├── minio_config.json          # MinIO configuration (auto-generated)
├── pipeline.log               # Execution logs (auto-generated)
├── sonar-project.properties   # SonarQube configuration (auto-generated)
├── Temporal-Zone/             # Temporal data processing
│   ├── scripts/
│   │   ├── Temporal_Landing.py    # Data ingestion from TreeOfLife-200M
│   │   └── Persistent_Landing.py  # Data persistence
│   └── notebooks/
├── Formatted-Zone/            # Data formatting
│   ├── scripts/
│   │   ├── Formatted_Metadata.py  # Metadata standardization
│   │   └── Formatted_images.py    # Image processing
│   └── notebooks/
├── Trusted_Zone/              # Data quality assurance
│   ├── scripts/
│   │   ├── Trusted_Metadata.py   # Metadata validation
│   │   └── Trusted_Images.py     # Image validation
│   └── notebooks/
├── Exploitation-Zone/         # Advanced processing & vector embeddings
│   ├── scripts/
│   │   ├── Exploitation_Metadata.py    # Metadata embeddings
│   │   ├── Exploitation_Images.py      # Image embeddings
│   │   └── Exploitation_Multimodal.py  # Multimodal integration
│   ├── notebooks/
│   └── exploitation_db/       # ChromaDB vector database
└── Multi-Modal Tasks/         # AI/ML tasks
    ├── scripts/
    │   ├── Same_Modality_Search.py      # Single-modality similarity
    │   ├── Multimodal_Similarity_Task.py # Cross-modality similarity
    │   └── Generative_Task.py          # AI generation tasks
    ├── notebooks/
    └── query_images/           # Sample query images for testing
```

### Adding New Scripts
1. Convert your notebook to a Python script
2. Place it in the appropriate zone's `scripts/` directory
3. Update the `workflow_scripts` or `task_scripts` dictionary in `orchestrate.py`

## 📈 Monitoring

The system provides comprehensive monitoring:

- **System Metrics**: CPU, memory, and disk usage displayed every 30 seconds during script execution
- **Process Monitoring**: Individual script performance tracking
- **Error Tracking**: Detailed error logging with timestamps
- **Performance Reports**: Execution time and resource usage analysis

## 🧪 Quality Control & Testing

Run comprehensive code analysis:
```bash
python orchestrate.py
# Select option 3: Quality Control & Code Analysis
```

This will:
- Analyze all Python files using SonarQube
- Generate detailed quality reports
- Identify security vulnerabilities and code smells
- Provide web dashboard access for detailed analysis

## 📝 Logs & Configuration

- **Pipeline Logs**: `pipeline.log` - Complete execution log with timestamps
- **MinIO Config**: `minio_config.json` - Configuration storage (auto-generated)
- **SonarQube Config**: `sonar-project.properties` - Code analysis configuration (auto-generated)

## 🔗 External Dependencies

- **MinIO**: Distributed object storage for data persistence
- **ChromaDB**: Vector database for embeddings and similarity search
- **HuggingFace**: TreeOfLife-200M dataset access
- **SonarQube**: Code quality analysis (optional, requires Docker)
- **OpenAI**: AI generation capabilities (for Generative Task)

## 🤝 Contributing

1. Follow the existing code structure and naming conventions
2. Add appropriate error handling and logging
3. Include comprehensive docstrings for functions
4. Update documentation as needed
5. Ensure all scripts work with the orchestration system

## 📄 License

This project is part of the WildLife Data Management system developed for academic purposes.

## 👨‍💻 Author

**Arman Bazarchi** - Data Management Pipeline Orchestrator

---

*This orchestration system provides a complete DataOps environment for the WildLife Data Management Pipeline, ensuring reliable, monitored, and quality-controlled data processing operations with advanced AI/ML capabilities.*