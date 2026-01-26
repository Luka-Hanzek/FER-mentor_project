-- query_05_roads_by_surface.sql
-- Analyze road surface types
SELECT 
    tags['surface'] as surface_type,
    COUNT(*) as road_count,
    SUM(ST_Length(geometry)) as total_length,
    AVG(ST_Length(geometry)) as avg_length
FROM osm_data
WHERE tags['surface'] IS NOT NULL
GROUP BY tags['surface']
ORDER BY total_length DESC