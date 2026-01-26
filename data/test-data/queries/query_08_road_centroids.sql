-- query_08_road_centroids.sql
-- Calculate centroids of road segments
SELECT 
    id,
    tags['name'] as road_name,
    ST_Centroid(geometry) as centroid,
    ST_Length(geometry) as length
FROM osm_data
WHERE tags['name'] IS NOT NULL
LIMIT 100