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
    track_popularity as popularity,
    explicit
from tracksdata