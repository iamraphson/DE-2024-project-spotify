{{ config(materialized='table') }}

WITH albums AS (
    SELECT * FROM {{ ref('stg_spotify_albums')}}
),
tracks as (
    SELECT * FROM {{ ref('stg_spotify_tracks')}}
)


SELECT 
    albums.track_name AS track_name, 
    tracks.popularity AS popularity
FROM tracks 
INNER JOIN albums 
ON albums.track_id = tracks.id 
ORDER BY 2