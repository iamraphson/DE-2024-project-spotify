{{
    config(
        materialized='view'
    )
}}

with artistsdata as (
    select *
    from {{ source('staging', 'spotify_artists') }}
)

select
    id,
    name,
    artist_popularity as popularity,
    artist_genres as genre,
    followers,
    genre_0,
    genre_1,
    genre_2,
    genre_3,
    genre_4
from artistsdata
