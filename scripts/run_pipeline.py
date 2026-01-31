#!/usr/bin/env python3
"""
Run the complete Telegram Medical Data Warehouse pipeline
"""

import sys
import subprocess
from pathlib import Path
import time
import webbrowser
import threading

def check_dagster():
    """Check if Dagster is installed"""
    try:
        import dagster
        return True
    except ImportError:
        return False

def run_dagster_ui():
    """Run Dagster UI in background"""
    print("Starting Dagster UI...")
    
    # Start Dagster UI
    process = subprocess.Popen(
        ["dagster", "dev", "-f", "pipeline.py", "-h", "0.0.0.0", "-p", "3000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for UI to start
    time.sleep(3)
    
    # Open browser
    webbrowser.open("http://localhost:3000")
    
    print("Dagster UI started at http://localhost:3000")
    print("Press Ctrl+C to stop")
    
    try:
        process.wait()
    except KeyboardInterrupt:
        process.terminate()
        print("\nDagster UI stopped")

def run_pipeline_manual():
    """Run pipeline manually"""
    print("\nRunning pipeline manually...")
    
    try:
        # Import and run pipeline
        from pipeline import telegram_data_pipeline
        
        result = telegram_data_pipeline.execute_in_process()
        
        if result.success:
            print("\n✓ Pipeline executed successfully!")
            return True
        else:
            print("\n✗ Pipeline execution failed")
            return False
            
    except Exception as e:
        print(f"\n✗ Pipeline execution error: {e}")
        return False

def main():
    """Main function"""
    print("=" * 70)
    print("TELEGRAM MEDICAL DATA WAREHOUSE - PIPELINE ORCHESTRATION")
    print("=" * 70)
    
    # Check Dagster
    if not check_dagster():
        print("\n✗ Dagster not installed")
        print("\nPlease install with:")
        print("pip install dagster dagster-webserver dagster-postgres")
        return 1
    
    print("\n✓ Dagster is available")
    
    # Options
    print("\n" + "=" * 70)
    print("OPTIONS:")
    print("=" * 70)
    print("1. Start Dagster UI (with web interface)")
    print("2. Run pipeline manually (command line)")
    print("3. Run complete pipeline step-by-step")
    print("4. Exit")
    
    try:
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == "1":
            print("\nStarting Dagster UI...")
            run_dagster_ui()
        elif choice == "2":
            success = run_pipeline_manual()
            return 0 if success else 1
        elif choice == "3":
            print("\nRunning complete pipeline step-by-step...")
            return run_step_by_step()
        elif choice == "4":
            print("\nExiting...")
            return 0
        else:
            print("\nInvalid choice")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        return 0

def run_step_by_step():
    """Run pipeline step by step"""
    steps = [
        ("Initialize Database", "python scripts/init_database.py"),
        ("Scrape Telegram Data", "python scripts/run_scraper.py --days-back 7"),
        ("Load to PostgreSQL", "python src/load_to_postgres.py"),
        ("Run dbt Transformations", "python scripts/run_dbt.py all"),
        ("Run YOLO Detection", "python src/yolo_detect.py"),
        ("Load YOLO Results", "python src/load_yolo_results.py --input data/processed/yolo_results/latest.csv"),
        ("Start API Server", "python scripts/run_api.py"),
    ]
    
    print("\n" + "="*70)
    print("STEP-BY-STEP PIPELINE EXECUTION")
    print("="*70)
    
    for i, (name, command) in enumerate(steps, 1):
        print(f"\nStep {i}: {name}")
        print(f"Command: {command}")
        
        proceed = input("Run this step? (y/n/skip): ").strip().lower()
        
        if proceed == 'y':
            print(f"\nExecuting: {command}")
            try:
                result = subprocess.run(
                    command.split(),
                    check=True,
                    capture_output=True,
                    text=True
                )
                print(f"✓ {name} completed successfully")
                if result.stdout:
                    print(f"Output: {result.stdout[:500]}...")
            except subprocess.CalledProcessError as e:
                print(f"✗ {name} failed: {e}")
                print(f"Error output: {e.stderr}")
                retry = input("Retry? (y/n): ").strip().lower()
                if retry == 'y':
                    continue
                else:
                    print("Skipping to next step...")
        elif proceed == 'skip':
            print(f"⏭ Skipping {name}")
            continue
        else:
            print("Exiting pipeline...")
            break
    
    print("\n" + "="*70)
    print("PIPELINE EXECUTION COMPLETE")
    print("="*70)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())