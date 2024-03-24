{{
    config(
        materialized='view'
    )
}}

with albumsdata as (
    select *
    from {{ source('staging', 'spotify_albums') }}
)

select
    album_id as id,
    track_name,
    track_id,
    track_number,
    album_type as type,
    total_tracks,
    album_name as name,
    release_date,
    label,
    album_popularity as popularity,
    artist_id,
    artist_0,
    artist_1,
    artist_2,
    artist_3,
    artist_4,
    artist_5,
    artist_6,
    duration_sec,
from albumsdata
