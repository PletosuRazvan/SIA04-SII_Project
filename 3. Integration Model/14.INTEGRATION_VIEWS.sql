CREATE OR REPLACE VIEW INT_ATHLETE_PROFILE_V AS
SELECT
    a.athlete_id,
    a.name,
    a.sex,
    a.height,
    a.weight,
    NVL(m.total_medals, 0) AS total_medals,
    NVL(m.gold, 0)         AS gold,
    NVL(m.silver, 0)       AS silver,
    NVL(m.bronze, 0)       AS bronze,
    NVL(m.sports_count, 0) AS sports_count,
    NVL(m.games_count, 0)  AS games_count
FROM ATHLETES_V a
LEFT JOIN V_MONGO_ATHLETE_MEDALS m
    ON a.athlete_id = m.athlete_id;

SELECT COUNT(*) FROM INT_ATHLETE_PROFILE_V;

CREATE OR REPLACE VIEW INT_EVENT_CATALOG_V AS
SELECT
    e.event_id,
    e.event_name,
    s.sport_id,
    s.sport_name
FROM EVENTS_V e
JOIN SPORTS_V s ON e.sport_id = s.sport_id;

SELECT COUNT(*) FROM INT_EVENT_CATALOG_V;

CREATE OR REPLACE VIEW INT_GAME_PROFILE_V AS
SELECT
    g.game_id,
    g.games_name,
    g.year,
    g.season,
    g.city,
    NVL(gs.total_athletes, 0)  AS total_athletes,
    NVL(gs.total_entries, 0)   AS total_entries,
    NVL(gs.total_medals, 0)    AS total_medals,
    NVL(gs.gold_medals, 0)     AS gold_medals,
    NVL(gs.silver_medals, 0)   AS silver_medals,
    NVL(gs.bronze_medals, 0)   AS bronze_medals,
    NVL(gs.countries_count, 0) AS countries_count,
    NVL(gs.events_count, 0)    AS events_count
FROM GAMES_V g
LEFT JOIN V_MONGO_GAME_SUMMARY gs
    ON g.game_id = gs.game_id;

SELECT * FROM INT_GAME_PROFILE_V ORDER BY year;

CREATE OR REPLACE VIEW INT_RESULTS_FULL_V AS
SELECT
    r.result_id,
    r.athlete_id,
    a.name         AS athlete_name,
    a.sex,
    a.height,
    a.weight,
    r.age,
    r.game_id,
    g.games_name,
    g.year,
    g.season,
    g.city,
    r.event_id,
    e.event_name,
    s.sport_id,
    s.sport_name,
    r.noc,
    c.region       AS country,
    r.team,
    r.medal
FROM RESULTS_V r
LEFT JOIN ATHLETES_V a   ON r.athlete_id = a.athlete_id
LEFT JOIN GAMES_V g      ON r.game_id    = g.game_id
LEFT JOIN EVENTS_V e     ON r.event_id   = e.event_id
LEFT JOIN SPORTS_V s     ON e.sport_id   = s.sport_id
LEFT JOIN COUNTRIES_V c  ON r.noc        = c.noc;

SELECT COUNT(*) AS total_rows FROM INT_RESULTS_FULL_V;
SELECT * FROM INT_RESULTS_FULL_V WHERE ROWNUM <= 20;

SELECT athlete_name, sex, country, COUNT(*) AS gold_medals
FROM INT_RESULTS_FULL_V
WHERE medal = 'Gold'
GROUP BY athlete_name, sex, country
ORDER BY gold_medals DESC
FETCH FIRST 10 ROWS ONLY;

SELECT 'CSV_DIRECT'    AS sursa, COUNT(*) AS rows FROM CSV_ATHLETE_EVENTS_V
UNION ALL
SELECT 'VIEW_INTEGRAT' AS sursa, COUNT(*) AS rows FROM INT_RESULTS_FULL_V;

SELECT
    csv.athlete_name,
    csv.country,
    csv.sport_name,
    csv.medal,
    m.total_medals AS mongo_total_medals,
    gs.total_athletes AS mongo_game_athletes
FROM CSV_ATHLETE_EVENTS_V csv
LEFT JOIN V_MONGO_ATHLETE_MEDALS m
    ON csv.athlete_id = m.athlete_id
LEFT JOIN V_MONGO_GAME_SUMMARY gs
    ON csv.game_id = gs.game_id
WHERE csv.medal = 'Gold'
  AND ROWNUM <= 20;
