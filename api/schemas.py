"""
Pydantic models/schemas for FastAPI request/response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from enum import Enum

class MessageResponse(BaseModel):
    """Response model for message search results"""
    message_id: int
    channel_name: str
    message_date: Optional[str]
    message_text: str
    views: int
    forwards: int
    has_media: bool
    relevance_score: Optional[int] = 1
    
    class Config:
        schema_extra = {
            "example": {
                "message_id": 12345,
                "channel_name": "CheMed Telegram",
                "message_date": "2024-01-15T10:30:00",
                "message_text": "Paracetamol available at discount price...",
                "views": 150,
                "forwards": 5,
                "has_media": True,
                "relevance_score": 3
            }
        }

class ProductResponse(BaseModel):
    """Response model for product mentions"""
    product_name: str
    mention_count: int
    percentage: float
    
    class Config:
        schema_extra = {
            "example": {
                "product_name": "paracetamol",
                "mention_count": 1250,
                "percentage": 25.5
            }
        }

class ChannelActivity(BaseModel):
    """Model for channel activity trends"""
    time_period: str
    post_count: int
    avg_views: float
    avg_forwards: float
    media_count: int
    active_days: int

class ChannelProduct(BaseModel):
    """Model for channel's top products"""
    product: str
    count: int

class ChannelResponse(BaseModel):
    """Response model for channel activity"""
    channel_name: str
    channel_type: str
    total_posts: int
    first_post: Optional[str]
    last_post: Optional[str]
    avg_views: float
    avg_forwards: float
    media_percentage: float
    engagement_rate: float
    activity_trends: List[Dict[str, Any]]
    top_products: List[Dict[str, Any]]
    
    class Config:
        schema_extra = {
            "example": {
                "channel_name": "CheMed Telegram",
                "channel_type": "Pharmaceutical",
                "total_posts": 4200,
                "first_post": "2024-01-10T08:00:00",
                "last_post": "2024-01-17T18:30:00",
                "avg_views": 245.5,
                "avg_forwards": 12.3,
                "media_percentage": 13.6,
                "engagement_rate": 8.7,
                "activity_trends": [
                    {
                        "time_period": "2024-01-17",
                        "post_count": 85,
                        "avg_views": 230,
                        "avg_forwards": 10,
                        "media_count": 12,
                        "active_days": 1
                    }
                ],
                "top_products": [
                    {"product": "paracetamol", "count": 125},
                    {"product": "ibuprofen", "count": 89}
                ]
            }
        }

class SearchResponse(BaseModel):
    """Response model for message search"""
    query: str
    total_results: int
    limit: int
    offset: int
    messages: List[MessageResponse]
    
    class Config:
        schema_extra = {
            "example": {
                "query": "paracetamol",
                "total_results": 1250,
                "limit": 20,
                "offset": 0,
                "messages": []
            }
        }

class ChannelVisualStats(BaseModel):
    """Model for channel visual content statistics"""
    channel_name: str
    total_posts: int
    media_posts: int
    media_percentage: float
    avg_views_media: float
    avg_views_text: float

class ImageCategoryStats(BaseModel):
    """Model for image category statistics"""
    category: str
    image_count: int
    avg_detections: float
    avg_confidence: float

class VisualContentResponse(BaseModel):
    """Response model for visual content statistics"""
    period_days: int
    total_posts: int
    media_posts: int
    media_percentage: float
    avg_views_media: float
    avg_views_text: float
    avg_forwards_media: float
    avg_forwards_text: float
    engagement_ratio: float
    channels: List[Dict[str, Any]]
    image_categories: List[Dict[str, Any]]
    
    class Config:
        schema_extra = {
            "example": {
                "period_days": 30,
                "total_posts": 15423,
                "media_posts": 2105,
                "media_percentage": 13.6,
                "avg_views_media": 450.5,
                "avg_views_text": 150.2,
                "avg_forwards_media": 25.3,
                "avg_forwards_text": 8.7,
                "engagement_ratio": 3.0,
                "channels": [],
                "image_categories": []
            }
        }

class DailyStats(BaseModel):
    """Model for daily statistics"""
    date: str
    post_count: int
    avg_views: float
    media_count: int

class TopChannel(BaseModel):
    """Model for top channel statistics"""
    channel_name: str
    total_posts: int
    avg_views: float
    engagement_rate: float

class AnalyticsResponse(BaseModel):
    """Response model for comprehensive analytics"""
    total_channels: int
    total_messages: int
    earliest_message: Optional[str]
    latest_message: Optional[str]
    avg_views: float
    avg_forwards: float
    total_media_posts: int
    active_days: int
    media_percentage: float
    daily_stats: List[Dict[str, Any]]
    top_channels: List[Dict[str, Any]]
    generated_at: str
    
    class Config:
        schema_extra = {
            "example": {
                "total_channels": 6,
                "total_messages": 15423,
                "earliest_message": "2024-01-10T08:00:00",
                "latest_message": "2024-01-17T18:30:00",
                "avg_views": 245.5,
                "avg_forwards": 12.3,
                "total_media_posts": 2105,
                "active_days": 7,
                "media_percentage": 13.6,
                "daily_stats": [],
                "top_channels": [],
                "generated_at": "2024-01-18T10:30:00"
            }
        }

class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str
    database: str
    timestamp: str
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "database": "connected",
                "timestamp": "2024-01-18T10:30:00"
            }
        }