-- query_09_named_roads_with_lanes.sql
-- Find roads with lane information
SELECT 
    id,
    tags['name'] as road_name,
    tags['highway'] as highway_type,
    tags['lanes'] as num_lanes,
    ST_Length(geometry) as length
FROM osm_data
WHERE tags['name'] IS NOT NULL 
  AND tags['lanes'] IS NOT NULL
ORDER BY CAST(tags['lanes'] AS INT) DESC