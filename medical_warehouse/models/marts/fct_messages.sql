{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH messages AS (
    SELECT
        stm.message_id,
        stm.message_key,
        dc.channel_key,
        dd.date_key,
        stm.message_date,
        stm.message_text,
        stm.message_length,
        stm.views,
        stm.forwards,
        stm.has_media,
        stm.image_path,
        stm.has_medical_keywords,
        stm.detected_product,
        stm.message_hour,
        -- Calculate engagement metrics
        stm.views + stm.forwards AS total_engagement,
        CASE 
            WHEN stm.views > 0 
            THEN ROUND((stm.forwards::FLOAT / stm.views) * 100, 2)
            ELSE 0 
        END AS forward_rate,
        -- Text analysis
        CASE 
            WHEN stm.message_text LIKE '%price%' OR stm.message_text LIKE '%cost%' 
            THEN TRUE 
            ELSE FALSE 
        END AS mentions_price,
        CASE 
            WHEN stm.message_text LIKE '%stock%' OR stm.message_text LIKE '%available%' 
            THEN TRUE 
            ELSE FALSE 
        END AS mentions_availability,
        -- Extract potential prices (simplified regex)
        REGEXP_MATCHES(stm.message_text, '(\d+(?:\.\d+)?)\s*(?:birr|etb|br)', 'i') AS extracted_price,
        stm.scraped_at,
        CURRENT_TIMESTAMP AS loaded_at
    FROM {{ ref('stg_telegram_messages') }} stm
    LEFT JOIN {{ ref('dim_channels') }} dc ON stm.channel_name = dc.channel_name
    LEFT JOIN utils.dim_dates dd ON stm.date_key = dd.date_key
    WHERE stm.message_date IS NOT NULL
)

SELECT
    message_id,
    message_key,
    channel_key,
    date_key,
    message_date,
    message_text,
    message_length,
    views,
    forwards,
    has_media,
    image_path,
    has_medical_keywords,
    detected_product,
    message_hour,
    total_engagement,
    forward_rate,
    mentions_price,
    mentions_availability,
    CASE 
        WHEN extracted_price IS NOT NULL AND array_length(extracted_price, 1) > 0
        THEN extracted_price[1]::DECIMAL(10,2)
        ELSE NULL
    END AS extracted_price_amount,
    scraped_at,
    loaded_at
FROM messages