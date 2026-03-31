CREATE OR REPLACE VIEW OLAP_ROLLUP_DECADE_YEAR_GAME_V AS
SELECT
    CASE
        WHEN GROUPING(g.decade) = 1 THEN '{TOTAL_GENERAL}'
        ELSE TO_CHAR(g.decade) || 's'
    END AS decade,
    CASE
        WHEN GROUPING(g.decade) = 1 THEN ' '
        WHEN GROUPING(g.year) = 1 THEN 'subtotal ' || TO_CHAR(g.decade) || 's'
        ELSE TO_CHAR(g.year)
    END AS year,
    CASE
        WHEN GROUPING(g.decade) = 1 THEN ' '
        WHEN GROUPING(g.year) = 1 THEN ' '
        WHEN GROUPING(g.games_name) = 1 THEN 'subtotal ' || TO_CHAR(g.year)
        ELSE g.games_name
    END AS games_name,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_silver)   AS silver_medals,
    SUM(f.is_bronze)   AS bronze_medals,
    SUM(f.is_medalist) AS total_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g ON f.game_id = g.game_id
GROUP BY ROLLUP(g.decade, g.year, g.games_name)
ORDER BY g.decade, g.year, g.games_name;

SELECT * FROM OLAP_ROLLUP_DECADE_YEAR_GAME_V;

CREATE OR REPLACE VIEW OLAP_ROLLUP_SPORT_EVENT_V AS
SELECT
    CASE
        WHEN GROUPING(e.sport_name) = 1 THEN '{TOTAL_GENERAL}'
        ELSE e.sport_name
    END AS sport_name,
    CASE
        WHEN GROUPING(e.sport_name) = 1 THEN ' '
        WHEN GROUPING(e.event_name) = 1 THEN 'subtotal ' || e.sport_name
        ELSE e.event_name
    END AS event_name,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_silver)   AS silver_medals,
    SUM(f.is_bronze)   AS bronze_medals,
    SUM(f.is_medalist) AS total_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_EVENT_V e ON f.event_id = e.event_id
GROUP BY ROLLUP(e.sport_name, e.event_name)
ORDER BY e.sport_name, e.event_name;

SELECT * FROM OLAP_ROLLUP_SPORT_EVENT_V;

CREATE OR REPLACE VIEW OLAP_ROLLUP_COUNTRY_SEASON_V AS
SELECT
    CASE
        WHEN GROUPING(c.country_name) = 1 THEN '{TOTAL_GENERAL}'
        ELSE c.country_name
    END AS country_name,
    CASE
        WHEN GROUPING(c.country_name) = 1 THEN ' '
        WHEN GROUPING(g.season) = 1 THEN 'subtotal ' || c.country_name
        ELSE g.season
    END AS season,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_silver)   AS silver_medals,
    SUM(f.is_bronze)   AS bronze_medals,
    SUM(f.is_medalist) AS total_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
GROUP BY ROLLUP(c.country_name, g.season)
ORDER BY c.country_name, g.season;

SELECT * FROM OLAP_ROLLUP_COUNTRY_SEASON_V;

CREATE OR REPLACE VIEW OLAP_CUBE_COUNTRY_SEASON_V AS
SELECT
    CASE
        WHEN GROUPING(c.country_name) = 1 THEN '{ALL_COUNTRIES}'
        ELSE c.country_name
    END AS country_name,
    CASE
        WHEN GROUPING(g.season) = 1 THEN '{ALL_SEASONS}'
        ELSE g.season
    END AS season,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_medalist) AS total_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
GROUP BY CUBE(c.country_name, g.season)
ORDER BY c.country_name, g.season;

SELECT * FROM OLAP_CUBE_COUNTRY_SEASON_V;

CREATE OR REPLACE VIEW OLAP_CUBE_SPORT_MEDAL_GENDER_V AS
SELECT
    CASE
        WHEN GROUPING(e.sport_name) = 1 THEN '{ALL_SPORTS}'
        ELSE e.sport_name
    END AS sport_name,
    CASE
        WHEN GROUPING(f.medal) = 1 THEN '{ALL_MEDALS}'
        ELSE f.medal
    END AS medal,
    CASE
        WHEN GROUPING(a.gender_label) = 1 THEN '{ALL_GENDERS}'
        ELSE a.gender_label
    END AS gender,
    COUNT(*) AS entries
FROM FACT_RESULTS_V f
JOIN DIM_EVENT_V e   ON f.event_id   = e.event_id
JOIN DIM_ATHLETE_V a ON f.athlete_id = a.athlete_id
WHERE f.medal != 'None'
GROUP BY CUBE(e.sport_name, f.medal, a.gender_label)
ORDER BY e.sport_name, f.medal, a.gender_label;

SELECT * FROM OLAP_CUBE_SPORT_MEDAL_GENDER_V;

CREATE OR REPLACE VIEW OLAP_CUBE_ERA_AGEGROUP_V AS
SELECT
    CASE
        WHEN GROUPING(g.era) = 1 THEN '{ALL_ERAS}'
        ELSE g.era
    END AS era,
    CASE
        WHEN GROUPING(f.age_group) = 1 THEN '{ALL_AGE_GROUPS}'
        ELSE f.age_group
    END AS age_group,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_silver)   AS silver_medals,
    SUM(f.is_bronze)   AS bronze_medals,
    SUM(f.is_medalist) AS total_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g ON f.game_id = g.game_id
GROUP BY CUBE(g.era, f.age_group)
ORDER BY g.era, f.age_group;

SELECT * FROM OLAP_CUBE_ERA_AGEGROUP_V;

CREATE OR REPLACE VIEW OLAP_GSETS_YEAR_COUNTRY_SPORT_V AS
SELECT
    CASE
        WHEN GROUPING(g.year) = 1 THEN '{TOTAL_GENERAL}'
        ELSE TO_CHAR(g.year)
    END AS year,
    CASE
        WHEN GROUPING(g.year) = 1 THEN ' '
        WHEN GROUPING(c.country_name) = 1 THEN 'subtotal ' || TO_CHAR(g.year)
        ELSE c.country_name
    END AS country_name,
    CASE
        WHEN GROUPING(g.year) = 1 THEN ' '
        WHEN GROUPING(c.country_name) = 1 THEN ' '
        WHEN GROUPING(e.sport_name) = 1 THEN 'subtotal ' || c.country_name
        ELSE e.sport_name
    END AS sport_name,
    SUM(f.is_medalist) AS total_medals,
    SUM(f.is_gold)     AS gold_medals,
    COUNT(*)           AS total_participations
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g    ON f.game_id  = g.game_id
JOIN DIM_COUNTRY_V c ON f.noc      = c.noc
JOIN DIM_EVENT_V e   ON f.event_id = e.event_id
GROUP BY GROUPING SETS (
    (g.year),
    (g.year, c.country_name),
    (g.year, c.country_name, e.sport_name),
    ()
)
ORDER BY 1, 2, 3;

SELECT * FROM OLAP_GSETS_YEAR_COUNTRY_SPORT_V;

CREATE OR REPLACE VIEW OLAP_GSETS_SEASON_COUNTRY_MEDAL_V AS
SELECT
    CASE
        WHEN GROUPING(g.season) = 1 THEN '{ALL_SEASONS}'
        ELSE g.season
    END AS season,
    CASE
        WHEN GROUPING(c.country_name) = 1 THEN '{ALL_COUNTRIES}'
        ELSE c.country_name
    END AS country_name,
    CASE
        WHEN GROUPING(f.medal) = 1 THEN '{ALL_MEDALS}'
        ELSE f.medal
    END AS medal,
    COUNT(*) AS entries
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
WHERE f.medal != 'None'
GROUP BY GROUPING SETS (
    (g.season, f.medal),
    (c.country_name, f.medal),
    ()
)
ORDER BY 1, 2, 3;

SELECT * FROM OLAP_GSETS_SEASON_COUNTRY_MEDAL_V;

CREATE OR REPLACE VIEW OLAP_GSETS_DECADE_GENDER_AGE_V AS
SELECT
    CASE
        WHEN GROUPING(g.decade) = 1 THEN '{TOTAL_GENERAL}'
        ELSE TO_CHAR(g.decade) || 's'
    END AS decade,
    CASE
        WHEN GROUPING(g.decade) = 1 THEN ' '
        WHEN GROUPING(a.gender_label) = 1 THEN 'subtotal ' || TO_CHAR(g.decade) || 's'
        ELSE a.gender_label
    END AS gender,
    CASE
        WHEN GROUPING(g.decade) = 1 THEN ' '
        WHEN GROUPING(a.gender_label) = 1 THEN ' '
        WHEN GROUPING(f.age_group) = 1 THEN 'subtotal ' || a.gender_label
        ELSE f.age_group
    END AS age_group,
    SUM(f.is_medalist) AS total_medals,
    SUM(f.is_gold)     AS gold_medals,
    COUNT(*)           AS total_participations,
    ROUND(AVG(f.age), 1) AS avg_age
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g    ON f.game_id    = g.game_id
JOIN DIM_ATHLETE_V a ON f.athlete_id = a.athlete_id
GROUP BY GROUPING SETS (
    (g.decade),
    (g.decade, a.gender_label),
    (g.decade, a.gender_label, f.age_group),
    ()
)
ORDER BY 1, 2, 3;

SELECT * FROM OLAP_GSETS_DECADE_GENDER_AGE_V;
