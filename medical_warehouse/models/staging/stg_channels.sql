{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH channel_stats AS (
    SELECT
        channel_name,
        COUNT(*) AS total_posts,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        AVG(views) AS avg_views,
        AVG(forwards) AS avg_forwards,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS total_media_posts,
        AVG(message_length) AS avg_message_length
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY 1
),

channel_categories AS (
    SELECT
        channel_name,
        CASE 
            WHEN channel_name LIKE '%chemed%' OR channel_name LIKE '%pharm%' THEN 'Pharmaceutical'
            WHEN channel_name LIKE '%cosmetic%' OR channel_name LIKE '%lobelia%' THEN 'Cosmetics'
            WHEN channel_name LIKE '%medical%' OR channel_name LIKE '%health%' THEN 'Medical'
            ELSE 'Other'
        END AS channel_type,
        CASE 
            WHEN channel_name LIKE '%chemed%' THEN 'CheMed'
            WHEN channel_name LIKE '%lobelia%' THEN 'Lobelia Cosmetics'
            WHEN channel_name LIKE '%tikvah%' THEN 'Tikvah Pharma'
            WHEN channel_name LIKE '%ethiopharm%' THEN 'EthioPharm'
            WHEN channel_name LIKE '%addis%' THEN 'Addis Pharmacy'
            WHEN channel_name LIKE '%ethiomed%' THEN 'Ethio Medical'
            ELSE channel_name
        END AS channel_display_name
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY 1
)

SELECT
    cs.channel_name,
    cc.channel_display_name,
    cc.channel_type,
    cs.total_posts,
    cs.first_post_date,
    cs.last_post_date,
    cs.avg_views,
    cs.avg_forwards,
    cs.total_media_posts,
    cs.avg_message_length,
    -- Calculate media percentage
    ROUND(
        CASE 
            WHEN cs.total_posts > 0 
            THEN (cs.total_media_posts::FLOAT / cs.total_posts) * 100 
            ELSE 0 
        END, 2
    ) AS media_percentage,
    -- Calculate engagement rate (views + forwards per post)
    ROUND(
        CASE 
            WHEN cs.total_posts > 0 
            THEN ((cs.avg_views + cs.avg_forwards) / cs.total_posts) * 100 
            ELSE 0 
        END, 2
    ) AS engagement_rate,
    -- Add surrogate key
    {{ dbt_utils.generate_surrogate_key(['cs.channel_name']) }} AS channel_key
FROM channel_stats cs
LEFT JOIN channel_categories cc ON cs.channel_name = cc.channel_name