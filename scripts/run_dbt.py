#!/usr/bin/env python3
"""
Run dbt commands for data transformation
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dbt_run.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DbtRunner:
    def __init__(self, project_dir="medical_warehouse"):
        """Initialize dbt runner"""
        self.project_dir = Path(project_dir)
        load_dotenv()
        
        # Set environment variables for dbt
        os.environ['POSTGRES_HOST'] = os.getenv('POSTGRES_HOST', 'localhost')
        os.environ['POSTGRES_PORT'] = os.getenv('POSTGRES_PORT', '5432')
        os.environ['POSTGRES_USER'] = os.getenv('POSTGRES_USER', 'postgres')
        os.environ['POSTGRES_PASSWORD'] = os.getenv('POSTGRES_PASSWORD', 'yene1995')
        os.environ['POSTGRES_DB'] = os.getenv('POSTGRES_DB', 'telegram_warehouse')
    
    def run_command(self, command, args=None):
        """Run a dbt command"""
        cmd = ["dbt", command]
        if args:
            cmd.extend(args)
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"Command output: {result.stdout}")
            if result.stderr:
                logger.warning(f"Command stderr: {result.stderr}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False
        except FileNotFoundError:
            logger.error("dbt not found. Please install with: pip install dbt-postgres")
            return False
    
    def deps(self):
        """Install dbt dependencies"""
        logger.info("Installing dbt dependencies...")
        return self.run_command("deps")
    
    def seed(self):
        """Load seed data"""
        logger.info("Loading seed data...")
        return self.run_command("seed")
    
    def run(self, models=None, full_refresh=False):
        """Run dbt models"""
        logger.info("Running dbt models...")
        args = []
        if full_refresh:
            args.append("--full-refresh")
        if models:
            args.extend(["--models", models])
        
        return self.run_command("run", args)
    
    def test(self, models=None):
        """Run dbt tests"""
        logger.info("Running dbt tests...")
        args = []
        if models:
            args.extend(["--models", models])
        
        return self.run_command("test", args)
    
    def docs_generate(self):
        """Generate dbt documentation"""
        logger.info("Generating dbt documentation...")
        return self.run_command("docs", ["generate"])
    
    def docs_serve(self):
        """Serve dbt documentation"""
        logger.info("Serving dbt documentation...")
        # This runs in background
        import threading
        
        def serve_docs():
            subprocess.run(
                ["dbt", "docs", "serve"],
                cwd=self.project_dir,
                capture_output=True,
                text=True
            )
        
        thread = threading.Thread(target=serve_docs)
        thread.daemon = True
        thread.start()
        logger.info("dbt docs server started at http://localhost:8080")
        return True
    
    def run_all(self):
        """Run complete dbt pipeline"""
        logger.info("Starting complete dbt pipeline...")
        
        steps = [
            ("Installing dependencies", self.deps),
            ("Running models", self.run),
            ("Running tests", self.test),
            ("Generating documentation", self.docs_generate),
        ]
        
        success = True
        for step_name, step_func in steps:
            logger.info(f"Step: {step_name}")
            if not step_func():
                logger.error(f"Failed at step: {step_name}")
                success = False
                break
        
        if success:
            logger.info("dbt pipeline completed successfully")
            print("✓ dbt transformation completed successfully")
        else:
            logger.error("dbt pipeline failed")
            print("✗ dbt transformation failed")
        
        return success

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run dbt commands")
    parser.add_argument("command", nargs="?", choices=[
        "deps", "run", "test", "docs", "all", "serve"
    ], default="all", help="dbt command to run")
    parser.add_argument("--models", help="Specific models to run")
    parser.add_argument("--full-refresh", action="store_true", 
                       help="Full refresh of models")
    
    args = parser.parse_args()
    
    runner = DbtRunner()
    
    if args.command == "deps":
        success = runner.deps()
    elif args.command == "run":
        success = runner.run(models=args.models, full_refresh=args.full_refresh)
    elif args.command == "test":
        success = runner.test(models=args.models)
    elif args.command == "docs":
        success = runner.docs_generate()
    elif args.command == "serve":
        success = runner.docs_serve()
        if success:
            input("Press Enter to stop the server...")
        return 0 if success else 1
    elif args.command == "all":
        success = runner.run_all()
    else:
        print(f"Unknown command: {args.command}")
        return 1
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())