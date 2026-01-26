-- query_11_buffer_analysis.sql
-- Create buffers around roads
SELECT 
    id,
    tags['name'] as road_name,
    ST_Buffer(geometry, 0.001) as buffer_geom,
    ST_Length(geometry) as original_length,
    ST_Area(ST_Buffer(geometry, 0.001)) as buffer_area
FROM osm_data
WHERE tags['highway'] IN ('primary', 'secondary', 'tertiary')
LIMIT 50