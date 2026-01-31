#!/usr/bin/env python3
"""
Script to run the Telegram scraper and database loader
"""

import sys
import subprocess
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_scraper(days_back: int = 7):
    """Run the Telegram scraper"""
    logger.info("Starting Telegram scraper...")
    
    try:
        from src.scraper import TelegramScraper
        from dotenv import load_dotenv
        import os
        
        load_dotenv()
        
        scraper = TelegramScraper(
            api_id=int(os.getenv("API_ID")),
            api_hash=os.getenv("API_HASH"),
            phone=os.getenv("PHONE_NUMBER")
        )
        scraper.run(days_back)
        
        logger.info("Telegram scraper completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Scraper failed: {str(e)}")
        return False

def run_db_loader(batch_size: int = 100):
    """Run the database loader"""
    logger.info("Starting database loader...")
    
    try:
        from src.load_to_postgres import DatabaseLoader
        
        loader = DatabaseLoader()
        loader.run(batch_size)
        
        logger.info("Database loader completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database loader failed: {str(e)}")
        return False

def main():
    """Main function to run the complete pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Telegram data pipeline")
    parser.add_argument("--days-back", type=int, default=7, 
                       help="Number of days to scrape (default: 7)")
    parser.add_argument("--batch-size", type=int, default=100,
                       help="Batch size for database loading (default: 100)")
    parser.add_argument("--skip-scrape", action="store_true",
                       help="Skip scraping step")
    parser.add_argument("--skip-load", action="store_true",
                       help="Skip database loading step")
    
    args = parser.parse_args()
    
    logger.info(f"Starting pipeline with arguments: {args}")
    
    success = True
    
    # Run scraper
    if not args.skip_scrape:
        if not run_scraper(args.days_back):
            success = False
            logger.error("Scraping step failed")
    
    # Run database loader
    if success and not args.skip_load:
        if not run_db_loader(args.batch_size):
            success = False
            logger.error("Database loading step failed")
    
    if success:
        logger.info("Pipeline completed successfully")
        return 0
    else:
        logger.error("Pipeline failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())