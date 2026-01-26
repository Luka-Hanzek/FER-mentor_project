-- query_12_spatial_filter.sql
-- Filter roads within a specific bounding box
SELECT 
    id,
    tags['name'] as road_name,
    tags['highway'] as highway_type,
    ST_Length(geometry) as length
FROM osm_data
WHERE ST_Intersects(
    geometry,
    ST_GeomFromWKT('POLYGON((15.94 45.81, 15.95 45.81, 15.95 45.83, 15.94 45.83, 15.94 45.81))')
)