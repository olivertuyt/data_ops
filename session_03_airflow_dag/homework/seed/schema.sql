-- Session 3 homework — source schema + deterministic seed data.
-- Load into a dedicated "homework" database (see homework/README.md).
-- Safe to re-run: it drops and recreates everything.

DROP TABLE IF EXISTS daily_genre_stats;
DROP TABLE IF EXISTS plays;
DROP TABLE IF EXISTS tracks;

-- Source tables (normalised): a play references a track; the track carries the genre.
CREATE TABLE tracks (
    track_id INT PRIMARY KEY,
    title    TEXT NOT NULL,
    artist   TEXT NOT NULL,
    genre    TEXT NOT NULL
);

CREATE TABLE plays (
    play_id   BIGINT PRIMARY KEY,
    track_id  INT NOT NULL REFERENCES tracks(track_id),
    user_id   INT NOT NULL,
    played_at TIMESTAMP NOT NULL,
    ms_played INT NOT NULL
);

-- Target table you write into (starts empty). One row per (day, genre).
CREATE TABLE daily_genre_stats (
    dt               DATE NOT NULL,
    genre            TEXT NOT NULL,
    play_count       INT NOT NULL,
    unique_listeners INT NOT NULL,
    minutes_played   NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (dt, genre)
);

INSERT INTO tracks (track_id, title, artist, genre) VALUES
    (1, 'Neon Skyline',  'Aurora Vale',   'pop'),
    (2, 'Iron Pulse',    'The Gearheads', 'rock'),
    (3, 'Midnight Blue', 'Cool Trio',     'jazz'),
    (4, 'Sugar Rush',    'Bubble Pop',    'pop'),
    (5, 'Thunder Road',  'Volt',          'rock'),
    (6, 'Slow Ember',    'Nightcafe',     'jazz');

-- 200 deterministic plays spread across 2024-03-01 .. 2024-03-05 (40 per day).
INSERT INTO plays (play_id, track_id, user_id, played_at, ms_played)
SELECT
    gs,
    1 + (gs % 6),
    100 + (gs % 10),
    TIMESTAMP '2024-03-01 00:00:00'
        + ((gs % 5) * INTERVAL '1 day')
        + ((gs % 24) * INTERVAL '1 hour'),
    120000 + (gs % 90) * 1000
FROM generate_series(1, 200) AS gs;
