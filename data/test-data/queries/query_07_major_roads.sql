-- query_07_major_roads.sql
-- Find major roads (primary, secondary, tertiary)
SELECT 
    id,
    tags['name'] as road_name,
    tags['highway'] as highway_type,
    tags['ref'] as road_ref,
    ST_Length(geometry) as length
FROM osm_data
WHERE tags['highway'] IN ('primary', 'secondary', 'tertiary')
ORDER BY length DESC