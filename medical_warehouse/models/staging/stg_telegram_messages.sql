{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH raw_messages AS (
    SELECT
        message_id,
        channel_name,
        message_date,
        message_text,
        has_media,
        image_path,
        views,
        forwards,
        scraped_at,
        raw_data
    FROM {{ source('raw', 'telegram_messages') }}
    WHERE message_date IS NOT NULL
      AND channel_name IS NOT NULL
),

cleaned_messages AS (
    SELECT
        message_id,
        LOWER(TRIM(channel_name)) AS channel_name,
        message_date,
        CASE 
            WHEN message_text IS NULL OR message_text = '' THEN 'NO_TEXT'
            ELSE utils.clean_text(message_text)
        END AS message_text,
        COALESCE(has_media, FALSE) AS has_media,
        image_path,
        COALESCE(views, 0) AS views,
        COALESCE(forwards, 0) AS forwards,
        scraped_at,
        raw_data,
        -- Calculate message length
        LENGTH(
            CASE 
                WHEN message_text IS NULL OR message_text = '' THEN 'NO_TEXT'
                ELSE utils.clean_text(message_text)
            END
        ) AS message_length,
        -- Extract date parts for partitioning
        DATE(message_date) AS message_date_date,
        EXTRACT(HOUR FROM message_date) AS message_hour,
        -- Flag for medical keywords
        CASE 
            WHEN LOWER(message_text) LIKE '%paracetamol%' 
                 OR LOWER(message_text) LIKE '%antibiotic%'
                 OR LOWER(message_text) LIKE '%vaccine%'
                 OR LOWER(message_text) LIKE '%medicine%'
                 OR LOWER(message_text) LIKE '%drug%'
                 OR LOWER(message_text) LIKE '%pill%'
                 OR LOWER(message_text) LIKE '%tablet%'
                 OR LOWER(message_text) LIKE '%injection%'
                 OR LOWER(message_text) LIKE '%syrup%'
            THEN TRUE
            ELSE FALSE
        END AS has_medical_keywords,
        -- Extract potential product names (simplified)
        CASE 
            WHEN message_text ~* '(?i)(paracetamol|ibuprofen|amoxicillin|ceftriaxone|metformin|insulin|ventolin)'
            THEN REGEXP_MATCHES(message_text, '(?i)(paracetamol|ibuprofen|amoxicillin|ceftriaxone|metformin|insulin|ventolin)')[1]
            ELSE NULL
        END AS detected_product
    FROM raw_messages
)

SELECT
    message_id,
    channel_name,
    message_date,
    message_text,
    has_media,
    image_path,
    views,
    forwards,
    scraped_at,
    raw_data,
    message_length,
    message_date_date,
    message_hour,
    has_medical_keywords,
    detected_product,
    -- Add surrogate key
    {{ dbt_utils.generate_surrogate_key(['message_id', 'channel_name']) }} AS message_key,
    -- Add date key for joining
    utils.get_date_key(message_date) AS date_key
FROM cleaned_messages