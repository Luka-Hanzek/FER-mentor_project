-- query_06_envelope_bbox.sql
-- Calculate bounding boxes for each road
SELECT 
    id,
    tags['name'] as road_name,
    ST_Envelope(geometry) as bbox,
    ST_Length(geometry) as length
FROM osm_data
WHERE tags['name'] IS NOT NULL
LIMIT 100