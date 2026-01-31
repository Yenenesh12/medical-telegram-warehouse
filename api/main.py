"""
FastAPI Application for Telegram Medical Data Warehouse Analytics
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import uvicorn

from api.database import get_db, Database
from api.schemas import (
    MessageResponse, ChannelResponse, ProductResponse,
    VisualContentResponse, SearchResponse, AnalyticsResponse
)

# Create FastAPI app
app = FastAPI(
    title="Telegram Medical Data Warehouse API",
    description="Analytical API for Ethiopian Medical Telegram Channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_database():
    db = Database()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    print("Starting Telegram Medical Data Warehouse API...")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    print("Shutting down API...")

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API health check"""
    return {
        "message": "Telegram Medical Data Warehouse API",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", tags=["Health"])
async def health_check(db: Database = Depends(get_database)):
    """Health check endpoint with database connectivity"""
    try:
        # Test database connection
        db.test_connection()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/api/reports/top-products", response_model=List[ProductResponse], tags=["Analytics"])
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    db: Database = Depends(get_database)
):
    """
    Get top mentioned medical products across all channels
    
    Returns the most frequently mentioned medical products/drugs
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        query = """
        WITH product_mentions AS (
            SELECT 
                UNNEST(ARRAY[
                    CASE WHEN message_text ~* '(?i)\\bparacetamol\\b' THEN 'paracetamol' END,
                    CASE WHEN message_text ~* '(?i)\\bibuprofen\\b' THEN 'ibuprofen' END,
                    CASE WHEN message_text ~* '(?i)\\bamoxicillin\\b' THEN 'amoxicillin' END,
                    CASE WHEN message_text ~* '(?i)\\bceftriaxone\\b' THEN 'ceftriaxone' END,
                    CASE WHEN message_text ~* '(?i)\\bmetformin\\b' THEN 'metformin' END,
                    CASE WHEN message_text ~* '(?i)\\binsulin\\b' THEN 'insulin' END,
                    CASE WHEN message_text ~* '(?i)\\bventolin\\b' THEN 'ventolin' END,
                    CASE WHEN message_text ~* '(?i)\\bantibiotic\\b' THEN 'antibiotic' END,
                    CASE WHEN message_text ~* '(?i)\\bvaccine\\b' THEN 'vaccine' END,
                    CASE WHEN message_text ~* '(?i)\\bantiviral\\b' THEN 'antiviral' END
                ]) AS product_name
            FROM marts.fct_messages
            WHERE message_date BETWEEN %s AND %s
              AND message_text IS NOT NULL
        )
        SELECT 
            product_name,
            COUNT(*) AS mention_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
        FROM product_mentions
        WHERE product_name IS NOT NULL
        GROUP BY product_name
        ORDER BY mention_count DESC
        LIMIT %s;
        """
        
        results = db.execute_query(query, (start_date, end_date, limit))
        
        products = []
        for row in results:
            products.append(ProductResponse(
                product_name=row[0],
                mention_count=row[1],
                percentage=float(row[2]) if row[2] else 0.0
            ))
        
        return products
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching top products: {str(e)}")

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelResponse, tags=["Channels"])
async def get_channel_activity(
    channel_name: str,
    period: str = Query("week", regex="^(day|week|month|year)$"),
    db: Database = Depends(get_database)
):
    """
    Get posting activity and trends for a specific channel
    
    Returns daily/weekly/monthly posting statistics
    """
    try:
        # Get channel information
        channel_query = """
        SELECT 
            channel_key,
            channel_display_name,
            channel_type,
            total_posts,
            first_post_date,
            last_post_date,
            avg_views,
            avg_forwards,
            media_percentage,
            engagement_rate
        FROM marts.dim_channels
        WHERE channel_name = %s OR channel_display_name ILIKE %s;
        """
        
        channel_result = db.execute_query(channel_query, (channel_name, f"%{channel_name}%"))
        
        if not channel_result:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        channel_data = channel_result[0]
        
        # Get activity trends based on period
        if period == "day":
            interval = "1 DAY"
            group_by = "DATE(message_date), EXTRACT(HOUR FROM message_date)"
            time_format = "HH24:00"
        elif period == "week":
            interval = "7 DAYS"
            group_by = "DATE(message_date)"
            time_format = "YYYY-MM-DD"
        elif period == "month":
            interval = "30 DAYS"
            group_by = "DATE_TRUNC('week', message_date)"
            time_format = "YYYY-'W'WW"
        else:  # year
            interval = "365 DAYS"
            group_by = "DATE_TRUNC('month', message_date)"
            time_format = "YYYY-MM"
        
        activity_query = f"""
        SELECT 
            TO_CHAR(MIN(message_date), '{time_format}') AS time_period,
            COUNT(*) AS post_count,
            AVG(views) AS avg_views,
            AVG(forwards) AS avg_forwards,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS media_count,
            COUNT(DISTINCT DATE(message_date)) AS active_days
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE dc.channel_name = %s
          AND message_date >= NOW() - INTERVAL '{interval}'
        GROUP BY {group_by}
        ORDER BY time_period;
        """
        
        activity_results = db.execute_query(activity_query, (channel_data[0],))
        
        # Format activity data
        activity_data = []
        for row in activity_results:
            activity_data.append({
                "time_period": row[0],
                "post_count": row[1],
                "avg_views": float(row[2]) if row[2] else 0,
                "avg_forwards": float(row[3]) if row[3] else 0,
                "media_count": row[4],
                "active_days": row[5]
            })
        
        # Get top products for this channel
        products_query = """
        WITH channel_products AS (
            SELECT 
                UNNEST(ARRAY[
                    CASE WHEN message_text ~* '(?i)\\bparacetamol\\b' THEN 'paracetamol' END,
                    CASE WHEN message_text ~* '(?i)\\bibuprofen\\b' THEN 'ibuprofen' END,
                    CASE WHEN message_text ~* '(?i)\\bamoxicillin\\b' THEN 'amoxicillin' END
                ]) AS product_name
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE dc.channel_name = %s
              AND message_date >= NOW() - INTERVAL '30 DAYS'
        )
        SELECT 
            product_name,
            COUNT(*) AS mention_count
        FROM channel_products
        WHERE product_name IS NOT NULL
        GROUP BY product_name
        ORDER BY mention_count DESC
        LIMIT 5;
        """
        
        products_result = db.execute_query(products_query, (channel_data[0],))
        top_products = [{"product": row[0], "count": row[1]} for row in products_result]
        
        return ChannelResponse(
            channel_name=channel_data[1],
            channel_type=channel_data[2],
            total_posts=channel_data[3],
            first_post=channel_data[4].isoformat() if channel_data[4] else None,
            last_post=channel_data[5].isoformat() if channel_data[5] else None,
            avg_views=float(channel_data[6]) if channel_data[6] else 0,
            avg_forwards=float(channel_data[7]) if channel_data[7] else 0,
            media_percentage=float(channel_data[8]) if channel_data[8] else 0,
            engagement_rate=float(channel_data[9]) if channel_data[9] else 0,
            activity_trends=activity_data,
            top_products=top_products
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching channel activity: {str(e)}")

@app.get("/api/search/messages", response_model=SearchResponse, tags=["Search"])
async def search_messages(
    query: str = Query(..., min_length=2, description="Search keyword"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    start_date: Optional[date] = Query(None, description="Start date for filtering"),
    end_date: Optional[date] = Query(None, description="End date for filtering"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Database = Depends(get_database)
):
    """
    Search for messages containing specific keywords
    
    Returns matching messages with relevance information
    """
    try:
        # Build query conditions
        conditions = ["message_text ILIKE %s"]
        params = [f"%{query}%"]
        
        if channel:
            conditions.append("dc.channel_name = %s")
            params.append(channel)
        
        if start_date:
            conditions.append("DATE(fm.message_date) >= %s")
            params.append(start_date)
        
        if end_date:
            conditions.append("DATE(fm.message_date) <= %s")
            params.append(end_date)
        
        where_clause = " AND ".join(conditions)
        
        # Get total count
        count_query = f"""
        SELECT COUNT(*)
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE {where_clause};
        """
        
        total_count = db.execute_query(count_query, params)[0][0]
        
        # Get search results
        search_query = f"""
        SELECT 
            fm.message_id,
            dc.channel_display_name,
            fm.message_date,
            fm.message_text,
            fm.views,
            fm.forwards,
            fm.has_media,
            -- Calculate relevance score
            CASE 
                WHEN message_text ILIKE %s THEN 3
                WHEN message_text ILIKE %s THEN 2
                ELSE 1
            END AS relevance_score
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE {where_clause}
        ORDER BY relevance_score DESC, fm.views DESC
        LIMIT %s OFFSET %s;
        """
        
        # Add relevance parameters
        search_params = params + [f"%{query}%", f"%{query}%", limit, offset]
        results = db.execute_query(search_query, search_params)
        
        messages = []
        for row in results:
            messages.append(MessageResponse(
                message_id=row[0],
                channel_name=row[1],
                message_date=row[2].isoformat() if row[2] else None,
                message_text=row[3][:200] + "..." if len(row[3]) > 200 else row[3],
                views=row[4],
                forwards=row[5],
                has_media=row[6],
                relevance_score=row[7]
            ))
        
        return SearchResponse(
            query=query,
            total_results=total_count,
            limit=limit,
            offset=offset,
            messages=messages
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching messages: {str(e)}")

@app.get("/api/reports/visual-content", response_model=VisualContentResponse, tags=["Analytics"])
async def get_visual_content_stats(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Database = Depends(get_database)
):
    """
    Get statistics about image usage and visual content across channels
    
    Returns engagement metrics for visual vs text-only content
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Overall visual content statistics
        overall_query = """
        SELECT 
            COUNT(*) AS total_posts,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS media_posts,
            AVG(CASE WHEN has_media THEN views ELSE NULL END) AS avg_views_media,
            AVG(CASE WHEN NOT has_media THEN views ELSE NULL END) AS avg_views_text,
            AVG(CASE WHEN has_media THEN forwards ELSE NULL END) AS avg_forwards_media,
            AVG(CASE WHEN NOT has_media THEN forwards ELSE NULL END) AS avg_forwards_text
        FROM marts.fct_messages
        WHERE message_date BETWEEN %s AND %s;
        """
        
        overall_result = db.execute_query(overall_query, (start_date, end_date))[0]
        
        # Channel-wise visual content statistics
        channel_query = """
        SELECT 
            dc.channel_display_name,
            COUNT(*) AS total_posts,
            SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END) AS media_posts,
            ROUND(
                SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                2
            ) AS media_percentage,
            AVG(CASE WHEN fm.has_media THEN fm.views ELSE NULL END) AS avg_views_media,
            AVG(CASE WHEN NOT fm.has_media THEN fm.views ELSE NULL END) AS avg_views_text
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE fm.message_date BETWEEN %s AND %s
        GROUP BY dc.channel_display_name
        ORDER BY media_percentage DESC;
        """
        
        channel_results = db.execute_query(channel_query, (start_date, end_date))
        
        # Image detection statistics (if available)
        detection_query = """
        SELECT 
            image_category,
            COUNT(*) AS image_count,
            AVG(detection_count) AS avg_detections,
            AVG(avg_confidence) AS avg_confidence
        FROM marts.fct_image_detections
        WHERE processing_date BETWEEN %s AND %s
        GROUP BY image_category
        ORDER BY image_count DESC;
        """
        
        detection_results = db.execute_query(detection_query, (start_date, end_date))
        
        # Format response
        channels = []
        for row in channel_results:
            channels.append({
                "channel_name": row[0],
                "total_posts": row[1],
                "media_posts": row[2],
                "media_percentage": float(row[3]) if row[3] else 0,
                "avg_views_media": float(row[4]) if row[4] else 0,
                "avg_views_text": float(row[5]) if row[5] else 0
            })
        
        detections = []
        for row in detection_results:
            detections.append({
                "category": row[0],
                "image_count": row[1],
                "avg_detections": float(row[2]) if row[2] else 0,
                "avg_confidence": float(row[3]) if row[3] else 0
            })
        
        return VisualContentResponse(
            period_days=days,
            total_posts=overall_result[0],
            media_posts=overall_result[1],
            media_percentage=(
                float(overall_result[1]) * 100 / float(overall_result[0])
                if overall_result[0] > 0 else 0
            ),
            avg_views_media=float(overall_result[2]) if overall_result[2] else 0,
            avg_views_text=float(overall_result[3]) if overall_result[3] else 0,
            avg_forwards_media=float(overall_result[4]) if overall_result[4] else 0,
            avg_forwards_text=float(overall_result[5]) if overall_result[5] else 0,
            engagement_ratio=(
                float(overall_result[2]) / float(overall_result[3])
                if overall_result[3] and overall_result[3] > 0 else 0
            ),
            channels=channels,
            image_categories=detections
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching visual content stats: {str(e)}")

@app.get("/api/analytics/summary", response_model=AnalyticsResponse, tags=["Analytics"])
async def get_analytics_summary(
    db: Database = Depends(get_database)
):
    """
    Get comprehensive analytics summary
    
    Returns overall statistics and insights
    """
    try:
        # Overall statistics
        summary_query = """
        SELECT 
            COUNT(DISTINCT channel_key) AS total_channels,
            COUNT(*) AS total_messages,
            MIN(message_date) AS earliest_date,
            MAX(message_date) AS latest_date,
            AVG(views) AS avg_views,
            AVG(forwards) AS avg_forwards,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS total_media,
            COUNT(DISTINCT DATE(message_date)) AS active_days
        FROM marts.fct_messages;
        """
        
        summary_result = db.execute_query(summary_query)[0]
        
        # Daily statistics
        daily_query = """
        SELECT 
            DATE(message_date) AS post_date,
            COUNT(*) AS post_count,
            AVG(views) AS avg_views,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS media_count
        FROM marts.fct_messages
        GROUP BY DATE(message_date)
        ORDER BY post_date DESC
        LIMIT 7;
        """
        
        daily_results = db.execute_query(daily_query)
        
        # Top channels
        top_channels_query = """
        SELECT 
            channel_display_name,
            total_posts,
            avg_views,
            engagement_rate
        FROM marts.dim_channels
        ORDER BY total_posts DESC
        LIMIT 5;
        """
        
        top_channels = db.execute_query(top_channels_query)
        
        # Format response
        daily_stats = []
        for row in daily_results:
            daily_stats.append({
                "date": row[0].isoformat() if row[0] else None,
                "post_count": row[1],
                "avg_views": float(row[2]) if row[2] else 0,
                "media_count": row[3]
            })
        
        channels = []
        for row in top_channels:
            channels.append({
                "channel_name": row[0],
                "total_posts": row[1],
                "avg_views": float(row[2]) if row[2] else 0,
                "engagement_rate": float(row[3]) if row[3] else 0
            })
        
        return AnalyticsResponse(
            total_channels=summary_result[0],
            total_messages=summary_result[1],
            earliest_message=summary_result[2].isoformat() if summary_result[2] else None,
            latest_message=summary_result[3].isoformat() if summary_result[3] else None,
            avg_views=float(summary_result[4]) if summary_result[4] else 0,
            avg_forwards=float(summary_result[5]) if summary_result[5] else 0,
            total_media_posts=summary_result[6],
            active_days=summary_result[7],
            media_percentage=(
                float(summary_result[6]) * 100 / float(summary_result[1])
                if summary_result[1] > 0 else 0
            ),
            daily_stats=daily_stats,
            top_channels=channels,
            generated_at=datetime.now().isoformat()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching analytics summary: {str(e)}")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

def start_server(host: str = "0.0.0.0", port: int = 8000):
    """Start the FastAPI server"""
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_server()