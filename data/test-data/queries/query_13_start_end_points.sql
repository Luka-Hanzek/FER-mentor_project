-- query_13_start_end_points.sql
-- Extract start and end points of road segments
SELECT 
    id,
    tags['name'] as road_name,
    ST_StartPoint(geometry) as start_point,
    ST_EndPoint(geometry) as end_point,
    ST_Length(geometry) as length
FROM osm_data
LIMIT 100