{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    channel_key,
    channel_name,
    channel_display_name,
    channel_type,
    total_posts,
    first_post_date,
    last_post_date,
    avg_views,
    avg_forwards,
    total_media_posts,
    avg_message_length,
    media_percentage,
    engagement_rate,
    -- Calculate days active
    DATE_PART('day', last_post_date - first_post_date) + 1 AS days_active,
    -- Calculate posts per day
    ROUND(
        CASE 
            WHEN DATE_PART('day', last_post_date - first_post_date) + 1 > 0
            THEN total_posts::FLOAT / (DATE_PART('day', last_post_date - first_post_date) + 1)
            ELSE total_posts::FLOAT
        END, 2
    ) AS posts_per_day,
    -- Activity level classification
    CASE 
        WHEN total_posts >= 1000 THEN 'Very High'
        WHEN total_posts >= 500 THEN 'High'
        WHEN total_posts >= 100 THEN 'Medium'
        ELSE 'Low'
    END AS activity_level,
    CURRENT_TIMESTAMP AS loaded_at
FROM {{ ref('stg_channels') }}