
"""
WildLife Data Management Pipeline Orchestrator
==============================================
- Arman Bazarchi

This orchestrator provides a comprehensive interface for managing the complete Data Management 
Backbone pipeline. It handles script execution, system monitoring, and code quality analysis
through SonarQube integration.

Key Features:
- Complete pipeline orchestration with real-time monitoring
- Running full workflow scripts to store data completely
- Individual script execution with system resource tracking
- SonarQube code quality analysis with detailed reporting
- saves only error logs in a file in root project

"""

import os
import sys
import time
import json
import psutil
import subprocess
import threading
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Configure error logging to save only errors in root project directory
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('error.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



class PipelineOrchestrator:
    """
    Main orchestrator class for the WildLife Data Management Pipeline.
    
    This class manages the complete data processing workflow from temporal landing
    through exploitation zones, providing real-time monitoring and quality control.
    It integrates MinIO for distributed storage, ChromaDB for vector operations,
    and SonarQube for code quality analysis.
    """
    SONAR_CONFIG_FILE = "sonar-project.properties"
    
    def __init__(self):
        """
        Initialize the orchestrator with script mappings and monitorings.
        Sets up the complete workflow definition including:
        """
        self.minio_config = {}
        self.scripts_dir = Path(__file__).parent
        
        self.workflow_scripts = {
            "temporal_landing": "Temporal-Zone/scripts/Temporal_Landing.py",
            "persistent_landing": "Temporal-Zone/scripts/Persistent_Landing.py", 
            "formatted_metadata": "Formatted-Zone/scripts/Formatted_Metadata.py",
            "formatted_images": "Formatted-Zone/scripts/Formatted_images.py",
            "trusted_metadata": "Trusted_Zone/scripts/Trusted_Metadata.py",
            "trusted_images": "Trusted_Zone/scripts/Trusted_Images.py",
            "exploitation_metadata": "Exploitation-Zone/scripts/Exploitation_Metadata.py",
            "exploitation_images": "Exploitation-Zone/scripts/Exploitation_Images.py",
            "exploitation_multimodal": "Exploitation-Zone/scripts/Exploitation_Multimodal.py"
        }
        
        # Define task scripts for advanced operations
        self.task_scripts = {
            "same_modality_search": "Multi-Modal Tasks/scripts/Same_Modality_Search.py",
            "multimodal_similarity": "Multi-Modal Tasks/scripts/Multimodal_Similarity_Task.py",
            "generative_task": "Multi-Modal Tasks/scripts/Generative_Task.py"
        }
        
        # Recommended execution order for complete pipeline
        self.recommended_workflow = [
            "temporal_landing", "persistent_landing", "formatted_metadata",
            "formatted_images", "trusted_metadata", "trusted_images",
            "exploitation_metadata", "exploitation_images", "exploitation_multimodal"
        ]
        
        # Initialize monitoring data structure for system resource tracking
        self.monitoring_data = {
            "start_time": None,
            "end_time": None,
            "processes": {},
            "system_metrics": [],
            "errors": []
        }

    def get_minio_config(self) -> Dict[str, str]:
        # Configure MinIO connection parameters.
        import json

        print("\n" + "="*60)
        print(" MinIO Configuration Setup")
        print("="*60)
        
        # Check for environment variables first (for CI/CD and automated runs)
        endpoint = os.getenv('MINIO_ENDPOINT')
        access_key = os.getenv('MINIO_ACCESS_KEY')
        secret_key = os.getenv('MINIO_SECRET_KEY')
        
        if endpoint and access_key and secret_key:
            print(" MinIO configuration found in environment variables")
            config = {
                "endpoint": endpoint,
                "access_key": access_key,
                "secret_key": secret_key
            }
            # Persist configuration for use by individual pipeline scripts
            with open("minio_config.json", "w") as f:
                json.dump(config, f, indent=2)
            print(" MinIO configuration saved to minio_config.json")
            return config
        
        # Interactive mode for manual setup
        # Collect endpoint with validation to ensure connectivity
        while True:
            endpoint = input("Enter MinIO endpoint (e.g., localhost:9000): ").strip()
            if endpoint:
                break
            print(" Endpoint cannot be empty. Please enter a valid MinIO endpoint.")
        
        # Collect access credentials with validation
        while True:
            access_key = input("Enter MinIO access key: ").strip()
            if access_key:
                break
            print(" Access key cannot be empty. Please enter a valid access key.")
        
        while True:
            secret_key = input("Enter MinIO secret key: ").strip()
            if secret_key:
                break
            print(" Secret key cannot be empty. Please enter a valid secret key.")
        
        # Build configuration dictionary
        config = {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key
        }
        
        # Persist configuration for use by individual pipeline scripts
        with open("minio_config.json", "w") as f:
            json.dump(config, f, indent=2)
        
        print(" MinIO configuration saved to minio_config.json")
        return config

    def get_sonar_config(self) -> Tuple[str, str]:
        """
        Configure SonarQube URL and authentication token for code quality analysis.
        Returns:
            Tuple of (sonar_url, sonar_token) for SonarQube configuration
        """
        print("\n" + "="*60)
        print(" SonarQube Configuration")
        print("="*60)
        
        # Check for consolidated configuration first
        sonar_config_str = os.getenv('SONAR_CONFIG')
        if sonar_config_str:
            try:
                parts = sonar_config_str.split(',')
                sonar_url = parts[0].strip()
                sonar_token = parts[1].strip() if len(parts) > 1 else ''
                
                if sonar_url:
                    print(" SonarQube configuration found in consolidated environment variable")
                    return sonar_url, sonar_token
            except Exception as e:
                print(f" Warning: Invalid SONAR_CONFIG format: {sonar_config_str}")
        
        # Check for existing URL and token in environment variables
        sonar_url = os.getenv('SONAR_HOST_URL')
        sonar_token = os.getenv('SONAR_LOGIN')
        
        if sonar_url and sonar_token:
            print(" SonarQube URL and token found in individual environment variables")
            return sonar_url, sonar_token
        
        # Request URL from user
        while True:
            url = input("Enter SonarQube URL (e.g., http://localhost:9002): ").strip()
            if url:
                break
            print(" URL cannot be empty. Please enter a valid SonarQube URL.")
        
        # Request token from user with explanation of benefits
        print(" SonarQube token is optional but recommended for better security.")
        token = input("Enter SonarQube token (or press Enter to use anonymous access): ").strip()
        
        if token:
            # Save URL and token to .env file
            env_file_path = ".env"
            with open(env_file_path, "a") as f:
                f.write(f"\nSONAR_HOST_URL={url}\n")
                f.write(f"SONAR_LOGIN={token}\n")
            
            os.environ['SONAR_HOST_URL'] = url
            os.environ['SONAR_LOGIN'] = token
            print(f" SonarQube URL and token saved to {env_file_path}")
            return url, token
        else:
            # Save only URL to .env file
            env_file_path = ".env"
            with open(env_file_path, "a") as f:
                f.write(f"\nSONAR_HOST_URL={url}\n")
            
            print(f" SonarQube URL saved to {env_file_path}")
            print(" Using anonymous access to SonarQube")
            return url, None

    def display_menu(self):
       # Display the main orchestration menu with available operations.
        
        print("\n" + "="*60)
        print(" WildLife Data Management Pipeline Orchestrator")
        print("="*60)
        print("1. Complete Data Pipeline (Store All Data)")
        print("2. Individual Scripts (Choose specific scripts)")
        print("3. Quality Control & Code Analysis")
        print("4. View Pipeline Status")
        print("5. Exit")
        print("="*60)

    def run_script(self, script_path: str, script_name: str) -> Tuple[bool, str]:
        """
        Execute a single pipeline script with comprehensive monitoring and error handling.
        
        This method provides the core script execution functionality with:
        - Real-time system resource monitoring
        - Environment variable configuration for MinIO and coverage tracking
        
        Args:
            script_path: Relative path to the script file
            script_name: Human-readable name for logging and display
            
        Returns:
            Tuple of (success_status, message) indicating execution result
        """
        full_path = self.scripts_dir / script_path
        
        # Validate script existence before execution
        if not full_path.exists():
            error_msg = f"Script not found: {full_path}"
            logger.error(error_msg)
            return False, error_msg
        
        start_time = time.time()
        
        # Initialize monitoring control mechanism
        monitoring_active = threading.Event()
        monitoring_active.set()  # Start monitoring immediately
        
        try:
            # Configure environment variables for script execution
            env = os.environ.copy()
            env.update({
                "MINIO_ENDPOINT": self.minio_config["endpoint"],
                "MINIO_ACCESS_KEY": self.minio_config["access_key"],
                "MINIO_SECRET_KEY": self.minio_config["secret_key"],
                "CI": "true"  # Set CI environment variable for non-interactive mode detection
            })
            
            # Launch monitoring thread for real-time system metrics
            monitor_thread = threading.Thread(
                target=self._monitor_process, 
                args=(script_name, start_time, monitoring_active)
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # Execute the target script with configured environment
            print(f"Running: {full_path}")
            result = subprocess.run(
                [sys.executable, str(full_path)],
                env=env,
                text=True
            )
            
            # Stop monitoring when script execution completes
            monitoring_active.clear()
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Process execution results and update monitoring data
            if result.returncode == 0:
                return True, f"Success: {script_name} completed in {duration:.2f}s"
            else:
                error_msg = f"Script failed with return code {result.returncode}: {result.stderr}"
                logger.error(f"{script_name} failed: {error_msg}")
                self.monitoring_data["errors"].append({
                    "script": script_name,
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat()
                })
                return False, error_msg
                
        except Exception as e:
            monitoring_active.clear()  # Ensure monitoring stops on error
            error_msg = f"Unexpected error running {script_name}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def _monitor_process(self, script_name: str, start_time: float, monitoring_active: threading.Event):
        """
        Monitor system resources during script execution with periodic display.
        
        This method tracks CPU, memory, and disk usage while scripts are running,
        displaying system metrics every 30 seconds to avoid console spam while
        still providing periodic feedback on system performance.
        
        Args:
            script_name: Name of the script being monitored
            start_time: Timestamp when monitoring began
            monitoring_active: Threading event to control monitoring lifecycle
        """
        last_display_time = time.time()
        
        while monitoring_active.is_set():
            try:
                # Collect comprehensive system metrics using psutil
                cpu_percent = psutil.cpu_percent()
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                # Build detailed metrics dictionary for analysis
                metrics = {
                    "timestamp": datetime.now().isoformat(),
                    "script": script_name,
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_used_gb": memory.used / (1024**3),
                    "disk_percent": disk.percent,
                    "disk_free_gb": disk.free / (1024**3)
                }
                
                # Store metrics for historical analysis
                self.monitoring_data["system_metrics"].append(metrics)
                
                # Only display monitoring information every 30 seconds
                current_time = time.time()
                if current_time - last_display_time >= 30:
                    status = (
                        f"CPU: {cpu_percent:5.1f}%  "
                        f"RAM: {memory.percent:5.1f}%  "
                        f"DISK: {disk.percent:5.1f}%"
                    )
                    # Right-align within ~100 characters for consistent display
                    print(status.rjust(100))
                    last_display_time = current_time

                # Check monitoring continuation every 5 seconds (for more responsive stopping)
                if not monitoring_active.wait(5):  # Wait up to 5 seconds or until cleared
                    break
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                break

    def run_complete_data_pipeline(self):
        """
        Execute the complete data processing pipeline with monitoring.
        
        This method orchestrates the entire data workflow from temporal landing
        through exploitation zones, providing real-time monitoring and progress
        tracking for each stage of the pipeline.
        
        Returns:
            Boolean indicating overall pipeline success
        """
        print("\n Starting Complete Data Pipeline...")
        print("Workflow: Temporal → Persistent → Formatted → Trusted → Exploitation")
        
        # Initialize pipeline monitoring
        self.monitoring_data["start_time"] = datetime.now().isoformat()
        
        # Execute workflow scripts in recommended order
        for i, script_name in enumerate(self.recommended_workflow, 1):
            print(f"\n Step {i}/{len(self.recommended_workflow)}: {script_name}")
            script_path = self.workflow_scripts[script_name]
            
            # Execute script with monitoring
            success, message = self.run_script(script_path, script_name)
            
            if not success:
                print(f" Workflow stopped at {script_name}: {message}")
                return False
            
            print(f" {script_name} completed successfully")
            time.sleep(2)  # Brief pause between scripts for system stability
        
        print("\n Complete data pipeline finished successfully!")
        print(" All data has been processed and stored in the pipeline")
        
        # Finalize monitoring data
        self.monitoring_data["end_time"] = datetime.now().isoformat()
        
        return True

    def run_individual_script(self, sub_choice=None):
        """Run individual scripts (workflow or tasks)"""
        if sub_choice:
            # Non-interactive mode - execute specific script
            print(f" Executing script choice {sub_choice}")
            return self._handle_script_choice(int(sub_choice))
        
        # Interactive mode
        while True:
            # Display available scripts
            self._display_available_scripts()
            
            # Get user choice
            choice = self._get_user_script_choice()
            
            # Handle user choice
            if self._handle_script_choice(choice):
                break

    def _display_available_scripts(self):
        # Display available scripts to user 

        print("\n Available Scripts:")
        print("="*40)
        
        # Show workflow scripts
        print(" Workflow Scripts (Data Processing):")
        for i, (name, path) in enumerate(self.workflow_scripts.items(), 1):
            print(f"{i:2d}. {name.replace('_', ' ').title()}")
        
        # Show task scripts
        print("\n Task Scripts :")
        task_start = len(self.workflow_scripts) + 1
        for i, (name, path) in enumerate(self.task_scripts.items(), task_start):
            print(f"{i:2d}. {name.replace('_', ' ').title()}")
        
        print(f"\n{len(self.workflow_scripts) + len(self.task_scripts) + 1:2d}. Back to Main Menu")

    def _get_user_script_choice(self):
        # Get user script choice

        try:
            choice = int(input(f"\nSelect script (1-{len(self.workflow_scripts) + len(self.task_scripts) + 1}): ")) - 1
            return choice
        except ValueError:
            print(" Please enter a valid number")
            return None

    def _handle_script_choice(self, choice):
        # Handle user script choice and return True if should exit
        if choice is None:
            return False
        
        # Check if back option selected
        if choice == len(self.workflow_scripts) + len(self.task_scripts):
            print(" Returning to main menu...")
            return True
        
        # Check if workflow script selected
        elif 0 <= choice < len(self.workflow_scripts):
            return self._run_workflow_script(choice)
            
        # Check if task script selected
        elif len(self.workflow_scripts) <= choice < len(self.workflow_scripts) + len(self.task_scripts):
            return self._run_task_script(choice)
        else:
            print(" Invalid selection")
            return False

    def _run_workflow_script(self, choice):
        # Run selected workflow script
        script_names = list(self.workflow_scripts.keys())
        script_name = script_names[choice]
        script_path = self.workflow_scripts[script_name]
        
        success, message = self.run_script(script_path, script_name)
        print(f"\n{'success' if success else 'error'} {message}")
        return False

    def _run_task_script(self, choice):
        # Run selected task script
        task_idx = choice - len(self.workflow_scripts)
        script_names = list(self.task_scripts.keys())
        script_name = script_names[task_idx]
        script_path = self.task_scripts[script_name]
        
        success, message = self.run_script(script_path, script_name)
        print(f"\n{'success' if success else 'error'} {message}")
        return False

    def _check_docker_availability(self):
        # Check if Docker is available
        docker_check = subprocess.run(['docker', '--version'], 
                                    capture_output=True, text=True, timeout=10)
        
        if docker_check.returncode != 0:
            print(" Docker not found. Please install Docker Desktop.")
            print(" You can download it from: https://www.docker.com/products/docker-desktop/")
            return False
        return True


    def _create_sonar_config(self):
        # Create SonarQube configuration file - only if it doesn't exist
        
        # Check if config file already exists
        if os.path.exists(self.SONAR_CONFIG_FILE):
            print(f" Using existing {self.SONAR_CONFIG_FILE}")
            return
        
        # Create only if file doesn't exist
        # Use Docker mount path for sources (inside container)
        # check and analyse only python files under project folder.
        sonar_config = """sonar.projectKey=wildlife-pipeline
sonar.projectName=WildLife Data Management Pipeline
sonar.projectVersion=1.0
sonar.sources=/usr/src
sonar.python.version=3.8
sonar.inclusions=**/*.py
sonar.exclusions=**/__pycache__/**,**/.*,**/node_modules/**,**/venv/**,**/env/**,**/*.ipynb,**/*.log,**/*.txt,**/*.json,**/*.md,**/*.zip,**/*.bin,**/*.sqlite3,**/*.pickle
"""
        
        
        # Write config file (only if it doesn't exist)
        with open(self.SONAR_CONFIG_FILE, "w") as f:
            f.write(sonar_config)
        print(f" Created {self.SONAR_CONFIG_FILE}")

    def _build_docker_command(self, sonar_url: str, sonar_token: str):
        # Build Docker command for SonarQube analysis
        docker_cmd = [
            'docker', 'run', '--rm',
            '-v', f'{self.scripts_dir}:/usr/src',
            '-v', f'{self.scripts_dir}/{self.SONAR_CONFIG_FILE}:/usr/src/{self.SONAR_CONFIG_FILE}',
            '-e', f'SONAR_HOST_URL={sonar_url}'
        ]
        
        # Add token as environment variable if provided
        if sonar_token:
            docker_cmd.extend(['-e', f'SONAR_TOKEN={sonar_token}'])
        
        # Add the image
        docker_cmd.append('sonarsource/sonar-scanner-cli')
        
        return docker_cmd

    def _display_analysis_results(self, result, sonar_url=None, sonar_token=None):
        
        # Display comprehensive SonarQube analysis results with detailed reporting.
       
        print("\n SonarQube Analysis Results:")
        print("="*60)
        
        if result.returncode == 0:
            print(" Analysis completed successfully!")
            print(f" Web Dashboard: {sonar_url}")
            
            # Wait for SonarQube dashboard to update, then fetch results
            print(" Waiting 20 seconds for SonarQube dashboard to update...")
            import time
            time.sleep(20)
            
            print(" Fetching SonarQube analysis results...")
            issues = self._get_sonar_issues("wildlife-pipeline", sonar_url, sonar_token)
            
            if issues:
                self._display_sonar_issues(issues)
            else:
                print(" No issues found or unable to fetch results.")
                print(f" Please check the SonarQube dashboard: {sonar_url}/dashboard?id=wildlife-pipeline")
            
            # Display scanner warnings if present
            if result.stderr:
                print("\n Scanner Warnings:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        print(f"  {line.strip()}")
        else:
            print(" Analysis failed:")
            print(f"  {result.stderr}")

    def run_quality_control(self):
        """
        Execute comprehensive code quality analysis using SonarQube.
        
        This method performs static code analysis on the entire pipeline codebase,
        including security vulnerability detection, code smell identification,
        and technical debt assessment. It integrates with Docker for containerized
        analysis and provides detailed reporting.
        """
        print("\n Running SonarQube Code Analysis via Docker...")
        
        try:
            # Validate Docker availability for containerized analysis
            if not self._check_docker_availability():
                return
            
            print(" Docker found. Starting SonarQube analysis...")
            print(" Analyzing Python files only in the WildLife project...")
            
            # Configure SonarQube URL and authentication
            sonar_url, sonar_token = self.get_sonar_config()
            
            if sonar_token:
                print(f" Using SonarQube URL: {sonar_url}")
                print(f" Using SonarQube token: {sonar_token[:8]}...")
            else:
                print(f" Using SonarQube URL: {sonar_url}")
                print(" No SonarQube token provided - using anonymous access")
            
            
            # SonarQube server should be running externally
            print(" Make sure your SonarQube server is running and accessible.")
            
            # Create SonarQube configuration file
            self._create_sonar_config()
            
            # Execute SonarQube analysis using Docker containerization
            print(" Analyzing code with SonarQube via Docker...")
            docker_cmd = self._build_docker_command(sonar_url, sonar_token)
            
            # Run analysis with comprehensive monitoring
            print(" Starting code analysis with monitoring...")
            result = subprocess.run(docker_cmd, 
                                  capture_output=True, text=True, timeout=600)
            
            # Display comprehensive analysis results
            self._display_analysis_results(result, sonar_url, sonar_token)
                
        except subprocess.TimeoutExpired:
            print("  SonarQube analysis timed out after 10 minutes")
        except FileNotFoundError:
            print(" Docker not found in PATH")
            print(" Please install Docker Desktop:")
            print("  - You can download from: https://www.docker.com/products/docker-desktop/")
            print("  - Start Docker Desktop before running analysis")
        except Exception as e:
            print(f" Error running SonarQube analysis: {e}")
            logger.error(f"SonarQube analysis error: {e}")
        
        finally:
            # Preserve configuration file for future analysis runs
            pass

    def _get_sonar_issues(self, project_key, sonar_url, sonar_token):
        # Retrieve issue data from SonarQube REST API.
       
        import time, requests

        # Convert host.docker.internal to localhost for API calls from host
        api_url = sonar_url.replace('host.docker.internal', 'localhost')
        url = f"{api_url}/api/issues/search?projects={project_key}&resolved=false&types=BUG,CODE_SMELL,VULNERABILITY"
        headers = {'Cache-Control': 'no-cache'}
        if sonar_token:
            headers['Authorization'] = f"Bearer {sonar_token}"

        print(" Fetching analysis results from SonarQube API...")
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data.get("issues", [])
        except requests.RequestException as e:
            print(f"  Could not fetch issues from SonarQube API: {e}")
            print("    View results in the SonarQube web dashboard instead.")
            logger.error(f"SonarQube API error: {e}")
            return []

    def _display_sonar_issues(self, issues):
       # Display comprehensive SonarQube analysis results and top issues.
        
        print(f"\n Detailed Analysis Results ({len(issues)} issues found):")
        print("="*60)
        
        # Categorize issues by severity level
        severity_counts = {}
        for issue in issues:
            severity = issue.get('severity', 'UNKNOWN')
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        # Display summary organized by severity
        print(" Issues by Severity:")
        for severity, count in sorted(severity_counts.items()):
            print(f"  {severity}: {count}")
        
        # Display detailed information for top 10 issues
        print("\n Top 10 Issues:")
        for i, issue in enumerate(issues[:10], 1):
            severity = issue.get('severity', 'UNKNOWN')
            component = issue.get('component', '').split(':')[-1]  # Extract filename
            message = issue.get('message', 'No message')
            line = issue.get('line', 'N/A')
            
            print(f"  {i:2d}. [{severity}] {component}:{line}")
            print(f"      {message}")
            print()
        
        if len(issues) > 10:
            print(f"  ... and {len(issues) - 10} more issues (see web dashboard for full details)")


    def show_pipeline_status(self):
        """
        Display current pipeline execution status and performance metrics.
        
        This method provides a comprehensive overview of pipeline execution
        including timing information, monitoring data, and error statistics
        for operational monitoring and troubleshooting.
        """
        print("\n Pipeline Status")
        print("="*50)
        
        if self.monitoring_data["start_time"]:
            print(f" Started: {self.monitoring_data['start_time']}")
        
        if self.monitoring_data["end_time"]:
            print(f" Finished: {self.monitoring_data['end_time']}")
        else:
            print(" Pipeline is running...")
        
        print(f" Total monitoring samples: {len(self.monitoring_data['system_metrics'])}")
        print(f" Total errors: {len(self.monitoring_data['errors'])}")

    def run(self, non_interactive=False, auto_choice=None, auto_sub_choice=None):
        # Main orchestration
        
        # This method serves as the primary entry point for the pipeline orchestrator,
        # handling user interaction, and workflow execution.

        print(" Welcome to WildLife Data Management Pipeline Orchestrator")
        
        # Initialize MinIO configuration for distributed storage
        self.minio_config = self.get_minio_config()
        
        # Non-interactive mode for CI/CD
        if non_interactive:
            print(" Running in non-interactive mode for CI/CD")
            if auto_choice:
                choice = str(auto_choice)
            else:
                choice = "1"  # Default to complete pipeline
            
            print(f" Executing option {choice}")
            if choice == "2" and auto_sub_choice:
                print(f" Executing sub-option {auto_sub_choice}")
                self._execute_choice(choice, auto_sub_choice)
            else:
                self._execute_choice(choice)
            return
        
        # Main user interaction loop
        while True:
            self.display_menu()
            
            try:
                choice = input("\nSelect option (1-5): ").strip()
                if self._execute_choice(choice):
                    break
                    
            except KeyboardInterrupt:
                print("\n\n Goodbye!")
                break
            except Exception as e:
                print(f" Error: {e}")
                logger.error(f"Main loop error: {e}")
    
    def _execute_choice(self, choice, sub_choice=None):
        """Execute the selected menu choice and return True if should exit."""
        try:
            if choice == "1":
                # Execute complete data processing pipeline
                self.run_complete_data_pipeline()
            elif choice == "2":
                # Execute individual scripts with monitoring
                if sub_choice:
                    self.run_individual_script(sub_choice)
                else:
                    self.run_individual_script()
            elif choice == "3":
                # Perform comprehensive code quality analysis
                self.run_quality_control()
            elif choice == "4":
                # Display current pipeline status and metrics
                self.show_pipeline_status()
            elif choice == "5":
                print("\n Goodbye!")
                return True
            else:
                print(" Invalid option. Please select 1-5.")
        except Exception as e:
            print(f" Error executing choice {choice}: {e}")
            logger.error(f"Choice execution error: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments for CI/CD
    parser = argparse.ArgumentParser(description='WildLife Data Management Pipeline Orchestrator')
    parser.add_argument('--non-interactive', action='store_true', 
                       help='Run in non-interactive mode for CI/CD')
    parser.add_argument('--choice', type=int, choices=[1,2,3,4,5],
                       help='Predefined choice for non-interactive mode (1-5)')
    parser.add_argument('--sub-choice', type=int, choices=list(range(1, 13)),
                       help='Sub-choice for option 2 (individual scripts) (1-12)')
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    orchestrator.run(non_interactive=args.non_interactive, auto_choice=args.choice, auto_sub_choice=args.sub_choice)