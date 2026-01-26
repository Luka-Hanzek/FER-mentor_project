-- query_02_road_length.sql
-- Calculate total and average road length
SELECT 
    COUNT(*) as road_count,
    SUM(ST_Length(geometry)) as total_length,
    AVG(ST_Length(geometry)) as avg_length,
    MAX(ST_Length(geometry)) as max_length
FROM osm_data