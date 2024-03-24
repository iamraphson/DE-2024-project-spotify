{{
    config(
        materialized='table'

    )
}}

WITH albums AS (
    SELECT * FROM {{ ref('stg_spotify_albums')}}
),
artists AS (
    SELECT * FROM {{ ref('stg_spotify_artists')}}
),
tracks AS (
    SELECT * FROM {{ ref('stg_spotify_tracks')}}
),
features AS (
    SELECT * FROM {{ ref('stg_spotify_features')}}
)

SELECT
    -- Album columns
    albums.id AS album_id,
    albums.track_name AS album_track_name,
    albums.track_number AS album_track_number,
    albums.type AS album_type,
    albums.total_tracks AS album_total_tracks,
    albums.name AS album_name,
    albums.releASe_date AS album_releASe_date,
    albums.label AS album_record_label,
    albums.popularity AS album_popularity,
    albums.artist_0 AS album_main_artist,
    albums.artist_1 AS album_featuring_artist_1,
    albums.artist_2 AS album_featuring_artist_2,
    albums.artist_3 AS album_featuring_artist_3,
    albums.artist_4 AS album_featuring_artist_4,
    albums.artist_5 AS album_featuring_artist_5,
    albums.artist_6 AS album_featuring_artist_6,
    albums.duration_sec AS album_duration_sec,

    -- Artist columns
    artists.id AS artist_id,
    artists.name AS artist_name,
    artists.popularity AS artist_popularity,
    artists.followers AS artist_followers_count,
    artists.genre_0 AS artist_main_genre,
    artists.genre_1 AS artist_sub_genre_1,
    artists.genre_2 AS artist_sub_genre_2,
    artists.genre_3 AS artist_sub_genre_3,
    artists.genre_4 AS artist_sub_genre_4,

    -- Track columns
    tracks.id AS track_id,
    tracks.popularity AS track_popularity,
    tracks.explicit AS track_explicit,

    -- track's Feature columns
    features.danceability AS track_danceability,
    features.energy AS track_energy,
    features.key_description AS track_key,
    features.loudness AS track_loudness,
    features.mode AS track_mode,
    features.speechiness AS track_speechiness,
    features.acousticness AS track_acousticness,
    features.instrumentalness AS track_instrumentalness,
    features.liveness AS track_liveness,
    features.valence AS track_valence,
    features.tempo AS track_tempo,
    features.time_signature AS track_time_signature,
    features.duration_sec AS track_duration_sec
FROM tracks
INNER JOIN features ON features.id = tracks.id
INNER JOIN albums ON tracks.id = albums.track_id
LEFT JOIN artists ON artists.id = albums.artist_id
