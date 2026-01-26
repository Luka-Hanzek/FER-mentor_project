-- query_10_road_statistics.sql
-- Complex aggregation with multiple metrics
SELECT 
    tags['highway'] as highway_type,
    COUNT(*) as segment_count,
    SUM(ST_Length(geometry)) as total_length,
    AVG(ST_Length(geometry)) as avg_length,
    MIN(ST_Length(geometry)) as min_length,
    MAX(ST_Length(geometry)) as max_length,
    COUNT(CASE WHEN tags['lit'] = 'yes' THEN 1 END) as lit_count,
    COUNT(CASE WHEN tags['surface'] = 'asphalt' THEN 1 END) as asphalt_count
FROM osm_data
WHERE tags['highway'] IS NOT NULL
GROUP BY tags['highway']
ORDER BY total_length DESC