SHOW CON_NAME;

CREATE OR REPLACE VIEW FDBO.GAMES_V AS
SELECT
    game_id,
    games_name,
    year,
    season,
    city
FROM OLY_REF.GAMES;

CREATE OR REPLACE VIEW FDBO.SPORTS_V AS
SELECT
    sport_id,
    sport_name
FROM OLY_REF.SPORTS;

CREATE OR REPLACE VIEW FDBO.EVENTS_V AS
SELECT
    event_id,
    sport_id,
    event_name
FROM OLY_REF.EVENTS;

CREATE OR REPLACE VIEW FDBO.COUNTRIES_V AS
SELECT
    noc,
    region,
    notes
FROM OLY_REF.COUNTRIES;

SELECT COUNT(*) AS nr, 'GAMES'     AS tabel FROM FDBO.GAMES_V     UNION ALL
SELECT COUNT(*),       'SPORTS'             FROM FDBO.SPORTS_V    UNION ALL
SELECT COUNT(*),       'EVENTS'             FROM FDBO.EVENTS_V    UNION ALL
SELECT COUNT(*),       'COUNTRIES'          FROM FDBO.COUNTRIES_V;

SELECT
    r.athlete_id,
    a.name,
    a.sex,
    g.games_name,
    g.year,
    e.event_name,
    s.sport_name,
    c.region AS country,
    r.medal
FROM FDBO.RESULTS_V r
JOIN FDBO.ATHLETES_V a   ON r.athlete_id = a.athlete_id
JOIN FDBO.GAMES_V g      ON r.game_id    = g.game_id
JOIN FDBO.EVENTS_V e     ON r.event_id   = e.event_id
JOIN FDBO.SPORTS_V s     ON e.sport_id   = s.sport_id
JOIN FDBO.COUNTRIES_V c  ON r.noc        = c.noc
WHERE r.medal != 'None'
  AND ROWNUM <= 50;
