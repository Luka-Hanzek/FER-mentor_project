-- query_15_self_join_nearby.sql
-- Find road segments that are near each other (within 0.01 degrees)
SELECT 
    a.id as road1_id,
    a.tags['name'] as road1_name,
    b.id as road2_id,
    b.tags['name'] as road2_name,
    ST_Distance(a.geometry, b.geometry) as distance
FROM osm_data a
JOIN osm_data b ON a.id < b.id
WHERE ST_Distance(a.geometry, b.geometry) < 0.01
  AND a.tags['name'] IS NOT NULL
  AND b.tags['name'] IS NOT NULL
LIMIT 100