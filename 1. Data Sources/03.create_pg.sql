DROP TABLE IF EXISTS olympics.results;
DROP TABLE IF EXISTS olympics.athletes;

CREATE TABLE olympics.athletes (
    athlete_id INTEGER PRIMARY KEY,
    name       VARCHAR(300) NOT NULL,
    sex        CHAR(1),
    height     NUMERIC(5,1),
    weight     NUMERIC(5,1)
);

CREATE TABLE olympics.results (
    result_id  BIGINT PRIMARY KEY,
    athlete_id INTEGER NOT NULL,
    game_id    INTEGER NOT NULL,
    event_id   INTEGER NOT NULL,
    noc        VARCHAR(10) NOT NULL,
    team       VARCHAR(200),
    age        INTEGER,
    medal      VARCHAR(10) DEFAULT 'None',
    CONSTRAINT fk_results_athlete
        FOREIGN KEY (athlete_id)
        REFERENCES olympics.athletes(athlete_id)
);

CREATE INDEX idx_results_athlete ON olympics.results(athlete_id);
CREATE INDEX idx_results_game    ON olympics.results(game_id);
CREATE INDEX idx_results_event   ON olympics.results(event_id);
CREATE INDEX idx_results_noc     ON olympics.results(noc);
CREATE INDEX idx_results_medal   ON olympics.results(medal);
