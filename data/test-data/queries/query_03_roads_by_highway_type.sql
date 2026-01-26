-- query_03_roads_by_highway_type.sql
-- Group roads by highway type from tags
SELECT 
    tags['highway'] as highway_type,
    COUNT(*) as road_count,
    SUM(ST_Length(geometry)) as total_length
FROM osm_data
WHERE tags['highway'] IS NOT NULL
GROUP BY tags['highway']
ORDER BY road_count DESC