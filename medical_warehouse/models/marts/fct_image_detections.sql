{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH raw_detections AS (
    SELECT
        message_id,
        channel_name,
        image_path,
        detected_objects,
        detection_count,
        image_category,
        processing_date
    FROM {{ source('raw', 'image_detections') }}
    WHERE message_id IS NOT NULL
      AND channel_name IS NOT NULL
),

parsed_detections AS (
    SELECT
        rd.message_id,
        rd.channel_name,
        rd.image_path,
        rd.detection_count,
        rd.image_category,
        rd.processing_date,
        -- Parse JSON array of detected objects
        json_array_elements(rd.detected_objects::json) AS detected_object,
        -- Extract key objects
        CASE 
            WHEN rd.detected_objects::text LIKE '%"person"%' THEN TRUE
            ELSE FALSE
        END AS has_person,
        CASE 
            WHEN rd.detected_objects::text LIKE '%"bottle"%' 
                 OR rd.detected_objects::text LIKE '%"cup"%'
                 OR rd.detected_objects::text LIKE '%"bowl"%'
            THEN TRUE
            ELSE FALSE
        END AS has_container,
        CASE 
            WHEN rd.detected_objects::text LIKE '%"scissors"%' 
                 OR rd.detected_objects::text LIKE '%"knife"%'
            THEN TRUE
            ELSE FALSE
        END AS has_medical_tool
    FROM raw_detections rd
),

object_details AS (
    SELECT
        message_id,
        channel_name,
        image_path,
        detection_count,
        image_category,
        processing_date,
        has_person,
        has_container,
        has_medical_tool,
        -- Extract object details
        json_array_length(detected_objects::json) AS object_count,
        -- Calculate average confidence
        (
            SELECT AVG((obj->>'confidence')::FLOAT)
            FROM json_array_elements(detected_objects::json) AS obj
        ) AS avg_confidence,
        -- List all detected objects
        (
            SELECT string_agg(DISTINCT obj->>'class_name', ', ')
            FROM json_array_elements(detected_objects::json) AS obj
        ) AS detected_objects_list,
        CURRENT_TIMESTAMP AS loaded_at
    FROM raw_detections
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, detected_objects
)

SELECT
    od.message_id,
    od.channel_name,
    od.image_path,
    od.detection_count,
    od.image_category,
    od.processing_date,
    od.has_person,
    od.has_container,
    od.has_medical_tool,
    od.object_count,
    od.avg_confidence,
    od.detected_objects_list,
    od.loaded_at,
    -- Join with fact messages to get additional context
    fm.message_key,
    fm.channel_key,
    fm.date_key,
    fm.views,
    fm.forwards,
    fm.total_engagement,
    -- Calculate engagement metrics for images
    CASE 
        WHEN fm.views > 0 
        THEN ROUND((fm.forwards::FLOAT / fm.views) * 100, 2)
        ELSE 0 
    END AS forward_rate,
    -- Compare image vs non-image engagement
    CASE 
        WHEN od.has_person AND od.has_container THEN 'promotional'
        WHEN od.has_container AND NOT od.has_person THEN 'product_display'
        WHEN od.has_person AND NOT od.has_container THEN 'lifestyle'
        WHEN od.has_medical_tool THEN 'medical_tools'
        ELSE 'other'
    END AS detailed_category
FROM object_details od
LEFT JOIN {{ ref('fct_messages') }} fm 
    ON od.message_id = fm.message_id 
    AND od.channel_name = (
        SELECT channel_name 
        FROM {{ ref('dim_channels') }} 
        WHERE channel_key = fm.channel_key
    )
WHERE od.detection_count > 0