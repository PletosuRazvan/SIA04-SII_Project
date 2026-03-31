DROP TABLE IF EXISTS olympics.stg_results;
DROP TABLE IF EXISTS olympics.stg_athletes;

CREATE TABLE olympics.stg_athletes (
    athlete_id TEXT,
    name       TEXT,
    sex        TEXT,
    height     TEXT,
    weight     TEXT
);

CREATE TABLE olympics.stg_results (
    result_id  TEXT,
    athlete_id TEXT,
    game_id    TEXT,
    event_id   TEXT,
    noc        TEXT,
    team       TEXT,
    age        TEXT,
    medal      TEXT
);
