"""
Dagster pipeline for Telegram Medical Data Warehouse
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from dagster import (
    job,
    op,
    sensor,
    RunRequest,
    SkipReason,
    schedule,
    ScheduleDefinition,
    Definitions,
    DefaultSensorStatus,
    RunConfig,
    fs_io_manager,
    mem_io_manager
)
from dagster_postgres import postgres_resource

# Local imports
from src.scraper import TelegramScraper
from src.load_to_postgres import DatabaseLoader
from src.yolo_detect import YOLODetector
from src.load_yolo_results import YOLOResultsLoader
from scripts.run_dbt import DbtRunner
from scripts.init_database import DatabaseInitializer

@op
def init_database(context) -> str:
    """Initialize database schemas and tables"""
    context.log.info("Initializing database...")
    
    try:
        initializer = DatabaseInitializer()
        initializer.run()
        return "Database initialized successfully"
    except Exception as e:
        context.log.error(f"Database initialization failed: {e}")
        raise

@op
def scrape_telegram_data(context, days_back: int = 7) -> Dict[str, Any]:
    """Scrape data from Telegram channels"""
    context.log.info(f"Starting Telegram scraping (last {days_back} days)...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        scraper = TelegramScraper(
            api_id=int(os.getenv("API_ID")),
            api_hash=os.getenv("API_HASH"),
            phone=os.getenv("PHONE_NUMBER")
        )
        scraper.run(days_back)
        
        # Count scraped files
        messages_dir = Path("data/raw/telegram_messages")
        json_files = list(messages_dir.rglob("*.json"))
        
        images_dir = Path("data/raw/images")
        image_files = list(images_dir.rglob("*.jpg")) + list(images_dir.rglob("*.png"))
        
        result = {
            "status": "success",
            "json_files": len(json_files),
            "image_files": len(image_files),
            "timestamp": datetime.now().isoformat()
        }
        
        context.log.info(f"Scraping completed: {len(json_files)} JSON files, {len(image_files)} images")
        return result
        
    except Exception as e:
        context.log.error(f"Telegram scraping failed: {e}")
        raise

@op
def load_raw_to_postgres(context) -> Dict[str, Any]:
    """Load scraped data into PostgreSQL"""
    context.log.info("Loading raw data to PostgreSQL...")
    
    try:
        loader = DatabaseLoader()
        loader.run(batch_size=100)
        
        # Get row count
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'telegram_warehouse'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages")
        row_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        result = {
            "status": "success",
            "rows_loaded": row_count,
            "timestamp": datetime.now().isoformat()
        }
        
        context.log.info(f"Data loading completed: {row_count} rows loaded")
        return result
        
    except Exception as e:
        context.log.error(f"Data loading failed: {e}")
        raise

@op
def run_dbt_transformations(context) -> Dict[str, Any]:
    """Run dbt data transformations"""
    context.log.info("Running dbt transformations...")
    
    try:
        runner = DbtRunner()
        
        # Run dbt pipeline
        success = runner.run_all()
        
        if success:
            result = {
                "status": "success",
                "models_transformed": "staging and marts",
                "timestamp": datetime.now().isoformat()
            }
            context.log.info("dbt transformations completed successfully")
            return result
        else:
            raise Exception("dbt transformations failed")
            
    except Exception as e:
        context.log.error(f"dbt transformations failed: {e}")
        raise

@op
def run_yolo_enrichment(context) -> Dict[str, Any]:
    """Run YOLO object detection on images"""
    context.log.info("Running YOLO object detection...")
    
    try:
        # Check if there are images to process
        images_dir = Path("data/raw/images")
        image_files = list(images_dir.rglob("*.jpg")) + list(images_dir.rglob("*.png"))
        
        if not image_files:
            context.log.warning("No images found for YOLO processing")
            return {
                "status": "skipped",
                "reason": "No images found",
                "timestamp": datetime.now().isoformat()
            }
        
        # Run YOLO detection
        detector = YOLODetector()
        
        # Create output path
        output_dir = Path("data/processed/yolo_results")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_csv = output_dir / f"detections_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        results = detector.process_images_directory(images_dir, output_csv)
        
        # Analyze results
        if results:
            analysis = detector.analyze_results(results)
            result = {
                "status": "success",
                "images_processed": len(results),
                "output_file": str(output_csv),
                "analysis": analysis.get('summary', {}),
                "timestamp": datetime.now().isoformat()
            }
            context.log.info(f"YOLO processing completed: {len(results)} images processed")
            return result
        else:
            return {
                "status": "skipped",
                "reason": "No valid images processed",
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        context.log.error(f"YOLO processing failed: {e}")
        raise

@op
def load_yolo_results(context, yolo_result: Dict[str, Any]) -> Dict[str, Any]:
    """Load YOLO detection results into database"""
    context.log.info("Loading YOLO results into database...")
    
    try:
        if yolo_result.get('status') != 'success':
            context.log.warning("Skipping YOLO results loading - no results available")
            return {
                "status": "skipped",
                "reason": yolo_result.get('reason', 'No YOLO results'),
                "timestamp": datetime.now().isoformat()
            }
        
        output_file = Path(yolo_result.get('output_file', ''))
        if not output_file.exists():
            context.log.error(f"YOLO output file not found: {output_file}")
            raise FileNotFoundError(f"YOLO output file not found: {output_file}")
        
        # Load results
        loader = YOLOResultsLoader()
        success = loader.run(output_file)
        
        if success:
            result = {
                "status": "success",
                "results_loaded": "YOLO detection results",
                "timestamp": datetime.now().isoformat()
            }
            context.log.info("YOLO results loaded into database successfully")
            return result
        else:
            raise Exception("Failed to load YOLO results")
            
    except Exception as e:
        context.log.error(f"YOLO results loading failed: {e}")
        raise

@op
def run_analytics_api(context) -> Dict[str, Any]:
    """Start the analytics API server (in background)"""
    context.log.info("Starting analytics API server...")
    
    try:
        # Check if API dependencies are installed
        try:
            import fastapi
            import uvicorn
        except ImportError:
            context.log.warning("FastAPI not installed, skipping API server")
            return {
                "status": "skipped",
                "reason": "FastAPI dependencies not installed",
                "timestamp": datetime.now().isoformat()
            }
        
        # Note: In production, you might want to run this as a separate service
        # For the pipeline, we'll just verify the API can be imported
        from api.main import app
        
        result = {
            "status": "ready",
            "api_endpoints": [
                "/api/reports/top-products",
                "/api/channels/{name}/activity",
                "/api/search/messages",
                "/api/reports/visual-content",
                "/api/analytics/summary"
            ],
            "docs_url": "http://localhost:8000/docs",
            "timestamp": datetime.now().isoformat()
        }
        
        context.log.info("Analytics API is ready to start")
        context.log.info("Run separately: python scripts/run_api.py")
        
        return result
        
    except Exception as e:
        context.log.error(f"API setup failed: {e}")
        raise

@op
def generate_report(context, *args) -> Dict[str, Any]:
    """Generate pipeline execution report"""
    context.log.info("Generating pipeline execution report...")
    
    try:
        # Collect results from all steps
        steps = []
        for i, arg in enumerate(args):
            if isinstance(arg, dict):
                steps.append({
                    "step": i + 1,
                    "status": arg.get('status', 'unknown'),
                    "timestamp": arg.get('timestamp', ''),
                    "details": {k: v for k, v in arg.items() if k not in ['status', 'timestamp']}
                })
        
        # Create summary
        successful_steps = [s for s in steps if s['status'] in ['success', 'ready', 'skipped']]
        
        report = {
            "pipeline": "telegram_medical_warehouse",
            "execution_id": context.run_id,
            "timestamp": datetime.now().isoformat(),
            "total_steps": len(steps),
            "successful_steps": len(successful_steps),
            "all_successful": len(successful_steps) == len(steps),
            "steps": steps,
            "summary": f"Pipeline executed {len(successful_steps)}/{len(steps)} steps successfully"
        }
        
        # Save report
        reports_dir = Path("reports/pipeline")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = reports_dir / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        context.log.info(f"Pipeline report saved to {report_file}")
        
        # Print summary
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Execution ID: {context.run_id}")
        print(f"Timestamp: {report['timestamp']}")
        print(f"Steps: {report['successful_steps']}/{report['total_steps']} successful")
        print(f"Status: {'SUCCESS' if report['all_successful'] else 'PARTIAL SUCCESS'}")
        print("\nStep Details:")
        for step in steps:
            status_icon = "✓" if step['status'] in ['success', 'ready'] else "⚠" if step['status'] == 'skipped' else "✗"
            print(f"  {status_icon} Step {step['step']}: {step['status']}")
        print("="*60)
        
        return report
        
    except Exception as e:
        context.log.error(f"Report generation failed: {e}")
        raise

@job
def telegram_data_pipeline():
    """
    Complete Telegram Medical Data Warehouse Pipeline
    
    Steps:
    1. Initialize database
    2. Scrape Telegram data
    3. Load raw data to PostgreSQL
    4. Run dbt transformations
    5. Run YOLO object detection
    6. Load YOLO results to database
    7. Start analytics API
    8. Generate execution report
    """
    # Step 1: Initialize database
    init_result = init_database()
    
    # Step 2: Scrape Telegram data (can run independently)
    scrape_result = scrape_telegram_data()
    
    # Step 3: Load raw data (depends on scraping)
    load_result = load_raw_to_postgres()
    
    # Step 4: Run dbt transformations (depends on loading)
    dbt_result = run_dbt_transformations()
    
    # Step 5: Run YOLO enrichment (can run in parallel with dbt)
    yolo_result = run_yolo_enrichment()
    
    # Step 6: Load YOLO results (depends on YOLO)
    yolo_load_result = load_yolo_results(yolo_result)
    
    # Step 7: Start analytics API (depends on dbt)
    api_result = run_analytics_api()
    
    # Step 8: Generate report (depends on all previous steps)
    report = generate_report(
        init_result,
        scrape_result,
        load_result,
        dbt_result,
        yolo_result,
        yolo_load_result,
        api_result
    )
    
    return report

# Schedules
@schedule(
    job=telegram_data_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    execution_timezone="Africa/Addis_Ababa"
)
def daily_pipeline_schedule(context):
    """Daily pipeline schedule"""
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunConfig(
        ops={
            "scrape_telegram_data": {"config": {"days_back": 1}},
        }
    )

# Sensors
@sensor(
    job=telegram_data_pipeline,
    default_status=DefaultSensorStatus.RUNNING
)
def new_data_sensor(context):
    """
    Sensor that triggers pipeline when new data is available
    """
    # Check for new JSON files in data lake
    data_dir = Path("data/raw/telegram_messages")
    if not data_dir.exists():
        return SkipReason("Data directory does not exist")
    
    # Get last run time from cursor
    last_run_time = context.cursor or "1970-01-01"
    
    # Find new files
    new_files = []
    for json_file in data_dir.rglob("*.json"):
        file_time = datetime.fromtimestamp(json_file.stat().st_mtime)
        if file_time > datetime.fromisoformat(last_run_time):
            new_files.append(json_file)
    
    if new_files:
        context.update_cursor(datetime.now().isoformat())
        return RunRequest(
            run_key=f"new_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            run_config={
                "ops": {
                    "scrape_telegram_data": {"config": {"days_back": 1}},
                }
            }
        )
    else:
        return SkipReason("No new data files found")

# Definitions
defs = Definitions(
    jobs=[telegram_data_pipeline],
    schedules=[daily_pipeline_schedule],
    sensors=[new_data_sensor],
    resources={
        "io_manager": fs_io_manager,
        "postgres": postgres_resource.configured({
            "postgres_db": {
                "host": {"env": "POSTGRES_HOST"},
                "port": {"env": "POSTGRES_PORT"},
                "database": {"env": "POSTGRES_DB"},
                "user": {"env": "POSTGRES_USER"},
                "password": {"env": "POSTGRES_PASSWORD"},
            }
        })
    }
)