#!/usr/bin/env python3
"""
Load YOLO detection results into PostgreSQL database
"""

import os
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_db_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YOLOResultsLoader:
    def __init__(self):
        """Initialize database connection"""
        load_dotenv()
        
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'telegram_warehouse'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        
        self.connection = None
        
        # SQL statements
        self.create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.image_detections (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            channel_name VARCHAR(100) NOT NULL,
            image_path VARCHAR(500) NOT NULL,
            detected_objects JSONB,
            detection_count INTEGER DEFAULT 0,
            image_category VARCHAR(50),
            processing_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT unique_detection UNIQUE (message_id, channel_name, image_path)
        );
        
        CREATE INDEX IF NOT EXISTS idx_detection_message ON raw.image_detections(message_id);
        CREATE INDEX IF NOT EXISTS idx_detection_channel ON raw.image_detections(channel_name);
        CREATE INDEX IF NOT EXISTS idx_detection_category ON raw.image_detections(image_category);
        """
        
        self.insert_sql = """
        INSERT INTO raw.image_detections 
        (message_id, channel_name, image_path, detected_objects, 
         detection_count, image_category, processing_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (message_id, channel_name, image_path) 
        DO UPDATE SET
            detected_objects = EXCLUDED.detected_objects,
            detection_count = EXCLUDED.detection_count,
            image_category = EXCLUDED.image_category,
            processing_date = EXCLUDED.processing_date
        """
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def create_table(self):
        """Create image detections table if it doesn't exist"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(self.create_table_sql)
            self.connection.commit()
            logger.info("Created raw.image_detections table")
            cursor.close()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error creating table: {str(e)}")
            raise
    
    def load_csv_results(self, csv_path: Path) -> List[Dict[str, Any]]:
        """
        Load detection results from CSV file
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            List of detection records
        """
        try:
            if not csv_path.exists():
                logger.error(f"CSV file not found: {csv_path}")
                return []
            
            records = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Parse detected objects
                    detected_objects = []
                    object_names = row.get('detected_objects', '').split(';')
                    confidence_scores = row.get('confidence_scores', '').split(';')
                    
                    for obj_name, confidence in zip(object_names, confidence_scores):
                        if obj_name and confidence:
                            detected_objects.append({
                                'object': obj_name.strip(),
                                'confidence': float(confidence.strip())
                            })
                    
                    record = {
                        'message_id': int(row.get('message_id', 0)),
                        'channel_name': row.get('channel_name', ''),
                        'image_path': row.get('image_path', ''),
                        'detected_objects': json.dumps(detected_objects),
                        'detection_count': int(row.get('detection_count', 0)),
                        'image_category': row.get('image_category', 'other'),
                        'processing_date': row.get('processing_time', datetime.now().isoformat())
                    }
                    records.append(record)
            
            logger.info(f"Loaded {len(records)} records from {csv_path}")
            return records
            
        except Exception as e:
            logger.error(f"Error loading CSV results: {str(e)}")
            return []
    
    def load_json_results(self, json_path: Path) -> List[Dict[str, Any]]:
        """
        Load detection results from JSON file
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            List of detection records
        """
        try:
            if not json_path.exists():
                logger.error(f"JSON file not found: {json_path}")
                return []
            
            with open(json_path, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            records = []
            for result in results:
                record = {
                    'message_id': result.get('message_id', 0),
                    'channel_name': result.get('channel_name', ''),
                    'image_path': result.get('image_path', ''),
                    'detected_objects': json.dumps(result.get('detections', [])),
                    'detection_count': result.get('detection_count', 0),
                    'image_category': result.get('image_category', 'other'),
                    'processing_date': result.get('processing_time', datetime.now().isoformat())
                }
                records.append(record)
            
            logger.info(f"Loaded {len(records)} records from {json_path}")
            return records
            
        except Exception as e:
            logger.error(f"Error loading JSON results: {str(e)}")
            return []
    
    def insert_records(self, records: List[Dict[str, Any]]):
        """Insert records into database"""
        if not records:
            logger.warning("No records to insert")
            return
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare batch data
            batch_data = []
            for record in records:
                batch_data.append((
                    record['message_id'],
                    record['channel_name'],
                    record['image_path'],
                    record['detected_objects'],
                    record['detection_count'],
                    record['image_category'],
                    record['processing_date']
                ))
            
            # Execute batch insert
            execute_batch(cursor, self.insert_sql, batch_data)
            self.connection.commit()
            
            logger.info(f"Inserted {len(records)} records into database")
            cursor.close()
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error inserting records: {str(e)}")
            raise
    
    def run(self, input_path: Path):
        """
        Run the YOLO results loader
        
        Args:
            input_path: Path to input file (CSV or JSON)
        """
        try:
            logger.info(f"Starting YOLO results loading from {input_path}")
            
            self.connect()
            self.create_table()
            
            # Load results based on file extension
            if input_path.suffix.lower() == '.csv':
                records = self.load_csv_results(input_path)
            elif input_path.suffix.lower() == '.json':
                records = self.load_json_results(input_path)
            else:
                logger.error(f"Unsupported file format: {input_path.suffix}")
                return False
            
            # Insert records
            if records:
                self.insert_records(records)
                logger.info(f"Successfully loaded {len(records)} YOLO detection results")
                return True
            else:
                logger.warning("No records were loaded")
                return False
            
        except Exception as e:
            logger.error(f"YOLO results loading failed: {str(e)}")
            return False
        finally:
            self.disconnect()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Load YOLO detection results into database")
    parser.add_argument("--input", type=str, required=True,
                       help="Path to input file (CSV or JSON)")
    
    args = parser.parse_args()
    
    try:
        loader = YOLOResultsLoader()
        success = loader.run(Path(args.input))
        
        if success:
            print("✓ YOLO results loaded into database successfully")
            return 0
        else:
            print("✗ Failed to load YOLO results")
            return 1
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())