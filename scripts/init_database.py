#!/usr/bin/env python3
"""
Initialize PostgreSQL database for Telegram Medical Data Warehouse
"""

import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/database_init.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseInitializer:
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
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def create_schemas(self):
        """Create database schemas"""
        schemas = ['raw', 'staging', 'marts', 'utils']
        
        try:
            cursor = self.connection.cursor()
            
            for schema in schemas:
                create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema)
                )
                cursor.execute(create_schema_sql)
                logger.info(f"Created schema: {schema}")
            
            cursor.close()
            logger.info("All schemas created successfully")
            
        except Exception as e:
            logger.error(f"Error creating schemas: {str(e)}")
            raise
    
    def create_raw_tables(self):
        """Create raw data tables"""
        try:
            cursor = self.connection.cursor()
            
            # Raw telegram messages table
            create_raw_messages_sql = """
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
            
            CREATE INDEX IF NOT EXISTS idx_raw_channel_name ON raw.telegram_messages(channel_name);
            CREATE INDEX IF NOT EXISTS idx_raw_message_date ON raw.telegram_messages(message_date);
            CREATE INDEX IF NOT EXISTS idx_raw_has_media ON raw.telegram_messages(has_media);
            CREATE INDEX IF NOT EXISTS idx_raw_scraped_at ON raw.telegram_messages(scraped_at);
            """
            
            cursor.execute(create_raw_messages_sql)
            logger.info("Created raw.telegram_messages table")
            
            # Raw image detections table (for YOLO results)
            create_raw_detections_sql = """
            CREATE TABLE IF NOT EXISTS raw.image_detections (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                channel_name VARCHAR(100) NOT NULL,
                image_path VARCHAR(500) NOT NULL,
                detected_objects JSONB,
                processing_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT unique_detection UNIQUE (message_id, channel_name, image_path)
            );
            
            CREATE INDEX IF NOT EXISTS idx_raw_detection_message ON raw.image_detections(message_id);
            CREATE INDEX IF NOT EXISTS idx_raw_detection_channel ON raw.image_detections(channel_name);
            """
            
            cursor.execute(create_raw_detections_sql)
            logger.info("Created raw.image_detections table")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error creating raw tables: {str(e)}")
            raise
    
    def create_date_dimension(self):
        """Create date dimension table"""
        try:
            cursor = self.connection.cursor()
            
            create_date_dim_sql = """
            CREATE TABLE IF NOT EXISTS utils.dim_dates (
                date_key INTEGER PRIMARY KEY,
                full_date DATE NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_name VARCHAR(20) NOT NULL,
                day_of_month INTEGER NOT NULL,
                day_of_year INTEGER NOT NULL,
                week_of_year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                month_name VARCHAR(20) NOT NULL,
                quarter INTEGER NOT NULL,
                year INTEGER NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                is_holiday BOOLEAN DEFAULT FALSE,
                holiday_name VARCHAR(100),
                
                CONSTRAINT unique_full_date UNIQUE (full_date)
            );
            
            CREATE INDEX IF NOT EXISTS idx_date_full_date ON utils.dim_dates(full_date);
            CREATE INDEX IF NOT EXISTS idx_date_year_month ON utils.dim_dates(year, month);
            CREATE INDEX IF NOT EXISTS idx_date_is_weekend ON utils.dim_dates(is_weekend);
            """
            
            cursor.execute(create_date_dim_sql)
            logger.info("Created utils.dim_dates table")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error creating date dimension: {str(e)}")
            raise
    
    def populate_date_dimension(self, start_date='2024-01-01', end_date='2026-12-31'):
        """Populate date dimension table"""
        try:
            cursor = self.connection.cursor()
            
            # Check if table has data
            cursor.execute("SELECT COUNT(*) FROM utils.dim_dates")
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Date dimension already populated with {count} records")
                cursor.close()
                return
            
            # Generate dates
            from datetime import datetime, timedelta
            
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            dates_to_insert = []
            current = start
            
            while current <= end:
                date_key = int(current.strftime('%Y%m%d'))
                full_date = current.date()
                day_of_week = current.weekday() + 1  # 1=Monday, 7=Sunday
                day_name = current.strftime('%A')
                day_of_month = current.day
                day_of_year = current.timetuple().tm_yday
                week_of_year = current.isocalendar()[1]
                month = current.month
                month_name = current.strftime('%B')
                quarter = (current.month - 1) // 3 + 1
                year = current.year
                is_weekend = day_of_week in [6, 7]  # 6=Saturday, 7=Sunday
                
                dates_to_insert.append((
                    date_key, full_date, day_of_week, day_name, day_of_month,
                    day_of_year, week_of_year, month, month_name, quarter,
                    year, is_weekend, False, None
                ))
                
                current += timedelta(days=1)
            
            # Insert dates in batches
            insert_sql = """
            INSERT INTO utils.dim_dates 
            (date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
             week_of_year, month, month_name, quarter, year, is_weekend, is_holiday, holiday_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING
            """
            
            batch_size = 1000
            for i in range(0, len(dates_to_insert), batch_size):
                batch = dates_to_insert[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} dates")
            
            self.connection.commit()
            cursor.close()
            logger.info(f"Populated date dimension with {len(dates_to_insert)} records")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error populating date dimension: {str(e)}")
            raise
    
    def create_utility_functions(self):
        """Create utility functions"""
        try:
            cursor = self.connection.cursor()
            
            # Function to extract date key from timestamp
            create_date_key_function = """
            CREATE OR REPLACE FUNCTION utils.get_date_key(timestamp_val TIMESTAMP WITH TIME ZONE)
            RETURNS INTEGER AS $$
            BEGIN
                RETURN EXTRACT(YEAR FROM timestamp_val) * 10000 +
                       EXTRACT(MONTH FROM timestamp_val) * 100 +
                       EXTRACT(DAY FROM timestamp_val);
            END;
            $$ LANGUAGE plpgsql;
            """
            
            cursor.execute(create_date_key_function)
            logger.info("Created utils.get_date_key function")
            
            # Function to clean text
            create_clean_text_function = """
            CREATE OR REPLACE FUNCTION utils.clean_text(input_text TEXT)
            RETURNS TEXT AS $$
            BEGIN
                -- Remove extra whitespace
                input_text := regexp_replace(input_text, '\\s+', ' ', 'g');
                -- Trim
                input_text := trim(input_text);
                -- Remove control characters
                input_text := regexp_replace(input_text, '[\\x00-\\x1F\\x7F]', '', 'g');
                RETURN input_text;
            END;
            $$ LANGUAGE plpgsql;
            """
            
            cursor.execute(create_clean_text_function)
            logger.info("Created utils.clean_text function")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error creating utility functions: {str(e)}")
            raise
    
    def run(self):
        """Run the database initialization"""
        try:
            logger.info("Starting database initialization...")
            
            self.connect()
            self.create_schemas()
            self.create_raw_tables()
            self.create_date_dimension()
            self.populate_date_dimension()
            self.create_utility_functions()
            
            logger.info("Database initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise
        finally:
            self.disconnect()

def main():
    """Main function"""
    try:
        initializer = DatabaseInitializer()
        initializer.run()
        print("✓ Database initialized successfully")
        return 0
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())