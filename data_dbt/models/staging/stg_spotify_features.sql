{{ 
    config(
        materialized='view'
    )
}}

with featuresdata as (
    select * 
    from {{ source('staging', 'spotify_features') }}
)

select 
    id,
    danceability,
    energy,
    coalesce({{ dbt.safe_cast('key', api.Column.translate_type("integer")) }},-1) as key,
    {{ get_audio_key_description('key') }} as key_description,
    loudness,
    {{ dbt.safe_cast('mode', api.Column.translate_type("integer")) }} as mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    time_signature,
    duration_sec
from featuresdata