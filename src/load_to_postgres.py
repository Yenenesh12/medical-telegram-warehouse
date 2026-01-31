import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/db_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
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
        self.base_data_dir = Path("data/raw/telegram_messages")
        
        # SQL statements
        self.create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            channel_name VARCHAR(100) NOT NULL,
            message_date TIMESTAMP WITH TIME ZONE NOT NULL,
            message_text TEXT,
            has_media BOOLEAN DEFAULT FALSE,
            image_path VARCHAR(500),
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            scraped_at TIMESTAMP WITH TIME ZONE,
            raw_data JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT unique_message UNIQUE (message_id, channel_name)
        );
        
        CREATE INDEX IF NOT EXISTS idx_channel_name ON raw.telegram_messages(channel_name);
        CREATE INDEX IF NOT EXISTS idx_message_date ON raw.telegram_messages(message_date);
        CREATE INDEX IF NOT EXISTS idx_has_media ON raw.telegram_messages(has_media);
        """
        
        self.insert_sql = """
        INSERT INTO raw.telegram_messages 
        (message_id, channel_name, message_date, message_text, has_media, 
         image_path, views, forwards, scraped_at, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (message_id, channel_name) 
        DO UPDATE SET
            message_text = EXCLUDED.message_text,
            views = EXCLUDED.views,
            forwards = EXCLUDED.forwards,
            scraped_at = EXCLUDED.scraped_at,
            raw_data = EXCLUDED.raw_data
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
    
    def create_schema_and_table(self):
        """Create raw schema and table if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            # Create raw schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            
            # Create table
            cursor.execute(self.create_table_sql)
            
            self.connection.commit()
            logger.info("Created raw schema and telegram_messages table")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error creating schema/table: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def find_json_files(self) -> List[Path]:
        """Find all JSON files in the data lake"""
        json_files = list(self.base_data_dir.rglob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files")
        return json_files
    
    def process_json_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Process a single JSON file and extract messages"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            # Add metadata
            for message in messages:
                message['file_path'] = str(file_path)
                message['processed_at'] = datetime.now().isoformat()
            
            logger.debug(f"Processed {len(messages)} messages from {file_path}")
            return messages
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return []
    
    def prepare_batch_data(self, messages: List[Dict[str, Any]]) -> List[tuple]:
        """Prepare data for batch insertion"""
        batch_data = []
        
        for msg in messages:
            # Prepare values for insertion
            values = (
                msg.get('message_id'),
                msg.get('channel_name'),
                msg.get('message_date'),
                msg.get('message_text', ''),
                msg.get('has_media', False),
                msg.get('image_path'),
                msg.get('views', 0),
                msg.get('forwards', 0),
                msg.get('scraped_at'),
                json.dumps(msg)  # Store raw data as JSONB
            )
            batch_data.append(values)
        
        return batch_data
    
    def load_batch(self, batch_data: List[tuple]):
        """Load a batch of messages into the database"""
        if not batch_data:
            return
        
        try:
            cursor = self.connection.cursor()
            execute_batch(cursor, self.insert_sql, batch_data)
            self.connection.commit()
            logger.info(f"Loaded {len(batch_data)} messages into database")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading batch: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def run(self, batch_size: int = 100):
        """Main method to run the database loading process"""
        try:
            # Connect to database
            self.connect()
            
            # Create schema and table
            self.create_schema_and_table()
            
            # Find and process JSON files
            json_files = self.find_json_files()
            
            total_messages = 0
            for file_path in json_files:
                # Process each file
                messages = self.process_json_file(file_path)
                
                if not messages:
                    continue
                
                # Prepare and load in batches
                batch_data = self.prepare_batch_data(messages)
                
                for i in range(0, len(batch_data), batch_size):
                    batch = batch_data[i:i + batch_size]
                    self.load_batch(batch)
                
                total_messages += len(messages)
                logger.info(f"Completed processing {file_path}")
            
            logger.info(f"Total messages loaded: {total_messages}")
            
        except Exception as e:
            logger.error(f"Fatal error during database loading: {str(e)}")
            raise
        finally:
            self.disconnect()

# Main execution
if __name__ == "__main__":
    import sys
    
    try:
        # Initialize loader
        loader = DatabaseLoader()
        
        # Parse command line arguments
        batch_size = 100
        if len(sys.argv) > 1:
            try:
                batch_size = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Invalid batch_size argument. Using default: {batch_size}")
        
        # Run loader
        loader.run(batch_size)
        
    except KeyboardInterrupt:
        logger.info("Database loading interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)