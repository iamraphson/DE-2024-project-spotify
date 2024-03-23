-- load tracks
DROP TABLE IF EXISTS `{{ SPOTIFY_WH_DATASET }}.spotify_tracks`;

CREATE TABLE IF NOT EXISTS
  `{{ SPOTIFY_WH_DATASET }}.spotify_tracks`
CLUSTER BY
  `explicit`,
  `track_popularity` AS
SELECT
  *
FROM
  `{{SPOTIFY_WH_EXT_DATASET}}.spotify_tracks`;

-- load albums
DROP TABLE IF EXISTS `{{ SPOTIFY_WH_DATASET }}.spotify_albums`;

CREATE TABLE IF NOT EXISTS
  `{{ SPOTIFY_WH_DATASET }}.spotify_albums`
CLUSTER BY
  `album_type`,
  `album_popularity` AS
SELECT
  *
FROM
  `{{SPOTIFY_WH_EXT_DATASET}}.spotify_albums`;


-- load artists
DROP TABLE IF EXISTS `{{ SPOTIFY_WH_DATASET }}.spotify_artists`;

CREATE TABLE IF NOT EXISTS
  `{{ SPOTIFY_WH_DATASET }}.spotify_artists`
CLUSTER BY
  `artist_popularity` AS
SELECT
  *
FROM
  `{{SPOTIFY_WH_EXT_DATASET}}.spotify_artists`;


-- load features
DROP TABLE IF EXISTS `{{ SPOTIFY_WH_DATASET }}.spotify_features`;

CREATE TABLE IF NOT EXISTS
  `{{ SPOTIFY_WH_DATASET }}.spotify_features` AS
SELECT
  *
FROM
  `{{SPOTIFY_WH_EXT_DATASET}}.spotify_features`;
