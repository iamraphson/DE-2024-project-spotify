{{ 
    config(
        materialized='view'
    )
}}

with tracksdata as (
    select * 
    from {{ source('staging', 'spotify_tracks') }}
)

select 
    id,
    track_popularity,
    explicit
from tracksdata