import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import asyncio
from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaPhoto
from telethon.errors import FloodWaitError
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self, api_id: int, api_hash: str, phone: str):
        """
        Initialize Telegram scraper
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API Hash
            phone: Phone number associated with Telegram account
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.client = None
        
        # Create directories
        self.base_data_dir = Path("data/raw")
        self.images_dir = self.base_data_dir / "images"
        self.messages_dir = self.base_data_dir / "telegram_messages"
        
        for directory in [self.images_dir, self.messages_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Channels to scrape (Ethiopian medical businesses)
        self.channels = [
            "chemed",  # CheMed Telegram Channel
            "lobelia4cosmetics",  # Lobelia Cosmetics
            "tikvahpharma",  # Tikvah Pharma
            # Add more channels from https://et.tgstat.com/medicine
            "ethiopharm",
            "addispharmacy",
            "ethiomedical"
        ]
        
        # Ethiopian timezone
        self.ethiopia_tz = pytz.timezone('Africa/Addis_Ababa')
    
    async def init_client(self):
        """Initialize Telegram client"""
        self.client = TelegramClient(
            session='telegram_session',
            api_id=self.api_id,
            api_hash=self.api_hash
        )
        await self.client.start(phone=self.phone)
        logger.info("Telegram client initialized successfully")
    
    async def scrape_channel(self, channel_username: str, days_back: int = 7):
        """
        Scrape messages from a Telegram channel
        
        Args:
            channel_username: Username of the channel (without @)
            days_back: Number of days to look back
        
        Returns:
            List of scraped messages
        """
        try:
            # Get channel entity
            entity = await self.client.get_entity(channel_username)
            logger.info(f"Starting to scrape channel: {channel_username}")
            
            # Calculate date range
            end_date = datetime.now(self.ethiopia_tz)
            start_date = end_date - timedelta(days=days_back)
            
            messages = []
            async for message in self.client.iter_messages(
                entity,
                offset_date=end_date,
                reverse=True
            ):
                # Convert message date to Ethiopia time
                msg_date = message.date.astimezone(self.ethiopia_tz)
                
                # Stop if we've gone past our date range
                if msg_date < start_date:
                    break
                
                # Process message
                processed_msg = await self.process_message(message, channel_username)
                if processed_msg:
                    messages.append(processed_msg)
                
                # Rate limiting
                await asyncio.sleep(0.1)
            
            logger.info(f"Scraped {len(messages)} messages from {channel_username}")
            return messages
            
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {str(e)}")
            return []
    
    async def process_message(self, message: Message, channel_name: str) -> Optional[Dict[str, Any]]:
        """
        Process a single Telegram message
        
        Args:
            message: Telegram message object
            channel_name: Name of the channel
        
        Returns:
            Dictionary with processed message data
        """
        try:
            msg_data = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.astimezone(self.ethiopia_tz).isoformat(),
                "message_text": message.text or "",
                "has_media": message.media is not None,
                "views": message.views or 0,
                "forwards": message.forwards or 0,
                "image_path": None,
                "scraped_at": datetime.now(self.ethiopia_tz).isoformat()
            }
            
            # Download image if present
            if message.media and isinstance(message.media, MessageMediaPhoto):
                image_path = await self.download_image(message, channel_name)
                if image_path:
                    msg_data["image_path"] = str(image_path)
            
            return msg_data
            
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {str(e)}")
            return None
    
    async def download_image(self, message: Message, channel_name: str) -> Optional[Path]:
        """
        Download image from message
        
        Args:
            message: Telegram message object
            channel_name: Name of the channel
        
        Returns:
            Path to downloaded image
        """
        try:
            # Create channel directory
            channel_dir = self.images_dir / channel_name
            channel_dir.mkdir(parents=True, exist_ok=True)
            
            # Define image path
            image_path = channel_dir / f"{message.id}.jpg"
            
            # Download the image
            await message.download_media(file=str(image_path))
            
            logger.info(f"Downloaded image: {image_path}")
            return image_path
            
        except Exception as e:
            logger.error(f"Error downloading image for message {message.id}: {str(e)}")
            return None
    
    def save_messages_json(self, messages: List[Dict[str, Any]], channel_name: str):
        """
        Save messages to JSON file with date partitioning
        
        Args:
            messages: List of message dictionaries
            channel_name: Name of the channel
        """
        if not messages:
            return
        
        # Group messages by date
        messages_by_date = {}
        for msg in messages:
            msg_date = datetime.fromisoformat(msg["message_date"]).date()
            date_str = msg_date.isoformat()
            
            if date_str not in messages_by_date:
                messages_by_date[date_str] = []
            messages_by_date[date_str].append(msg)
        
        # Save each day's messages to separate file
        for date_str, daily_messages in messages_by_date.items():
            # Create date directory
            date_dir = self.messages_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # Define file path
            file_path = date_dir / f"{channel_name}.json"
            
            # Save to JSON
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(daily_messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(daily_messages)} messages to {file_path}")
    
    async def scrape_all_channels(self, days_back: int = 7):
        """
        Scrape all configured channels
        
        Args:
            days_back: Number of days to look back
        """
        try:
            # Initialize client
            await self.init_client()
            
            # Scrape each channel
            for channel in self.channels:
                try:
                    logger.info(f"Scraping channel: {channel}")
                    messages = await self.scrape_channel(channel, days_back)
                    
                    if messages:
                        self.save_messages_json(messages, channel)
                    
                    # Rate limiting between channels
                    await asyncio.sleep(2)
                    
                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    
                except Exception as e:
                    logger.error(f"Failed to scrape {channel}: {str(e)}")
                    continue
            
            logger.info("Finished scraping all channels")
            
        finally:
            # Disconnect client
            if self.client:
                await self.client.disconnect()
    
    def run(self, days_back: int = 7):
        """
        Main entry point to run the scraper
        
        Args:
            days_back: Number of days to look back
        """
        asyncio.run(self.scrape_all_channels(days_back))

# Main execution
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get credentials from environment
    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")
    phone = os.getenv("PHONE_NUMBER")
    
    if not all([api_id, api_hash, phone]):
        logger.error("Missing Telegram API credentials in .env file")
        sys.exit(1)
    
    try:
        # Initialize and run scraper
        scraper = TelegramScraper(
            api_id=int(api_id),
            api_hash=api_hash,
            phone=phone
        )
        
        # Parse command line arguments
        days_back = 7
        if len(sys.argv) > 1:
            try:
                days_back = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Invalid days_back argument. Using default: {days_back}")
        
        scraper.run(days_back)
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)