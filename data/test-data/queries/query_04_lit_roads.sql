-- query_04_lit_roads.sql
-- Find roads that are lit (have street lighting)
SELECT 
    id,
    tags['name'] as road_name,
    tags['highway'] as highway_type,
    tags['lit'] as lighting,
    ST_Length(geometry) as length
FROM osm_data
WHERE tags['lit'] = 'yes'