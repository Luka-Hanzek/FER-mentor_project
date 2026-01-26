-- query_14_road_complexity.sql
-- Analyze road complexity by number of points
SELECT 
    id,
    tags['name'] as road_name,
    tags['highway'] as highway_type,
    ST_NumPoints(geometry) as num_points,
    ST_Length(geometry) as length,
    ST_Length(geometry) / ST_NumPoints(geometry) as avg_segment_length
FROM osm_data
WHERE ST_NumPoints(geometry) > 10
ORDER BY num_points DESC
LIMIT 100